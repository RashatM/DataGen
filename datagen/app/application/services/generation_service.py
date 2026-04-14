from collections.abc import Iterator
import random
import time

from app.application.errors import GenerationInvariantError
from app.application.internal.foreign_key_reference_tracker import ForeignKeyReferenceTracker
from app.application.ports.generator_factory_port import DataGeneratorFactoryPort
from app.application.ports.table_dependency_planner_port import TableDependencyPlannerPort
from app.application.ports.value_converter_port import ValueConverterPort
from app.domain.derivation import DerivationPolicy
from app.domain.entities import GeneratedTableData, GenerationRun, TableColumnSpec, TableSpec
from app.domain.enums import RelationType
from app.domain.output_validation import validate_column_output_values
from app.domain.value_types import (
    ColumnValues,
    GeneratedColumnsByName,
    NonNullColumnValues,
)
from app.domain.validation_errors import UnsatisfiableConstraintsError
from app.shared.logger import generation_logger

logger = generation_logger


class DataGenerationService:
    """Оркестрирует генерацию таблиц в корректном порядке зависимостей.

    Сервис не просто вызывает генераторы по колонкам. Он:
    - сначала получает порядок таблиц с учётом внешних ключей
    - поддерживает runtime-кэш родительских значений для FK-колонок
    - отдельно строит исходные значения, выходные значения и производные колонки
    - валидирует итог по output constraints уже после вставки NULL
    - возвращает таблицы в том порядке, в котором их можно безопасно публиковать
    """
    def __init__(
        self,
        table_dependency_planner: TableDependencyPlannerPort,
        data_generator_factory: DataGeneratorFactoryPort,
        value_converter: ValueConverterPort,
        rng: random.Random,
    ) -> None:
        self.table_dependency_planner = table_dependency_planner
        self.data_generator_factory = data_generator_factory
        self.value_converter = value_converter
        self.rng = rng
        self.derivation_policy = DerivationPolicy()

    @staticmethod
    def insert_nulls(
        values: NonNullColumnValues,
        total_rows: int,
        null_positions: set[int],
    ) -> ColumnValues:
        """Возвращает итоговый столбец, вставляя NULL в заранее выбранные позиции без повторной генерации значений."""
        if not null_positions:
            return values

        output_values: ColumnValues = []
        append_output_value = output_values.append
        value_index = 0

        for row_index in range(total_rows):
            if row_index in null_positions:
                append_output_value(None)
                continue

            append_output_value(values[value_index])
            value_index += 1

        return output_values

    def generate_non_null_source_values(
        self,
        total_non_null_rows: int,
        table_column: TableColumnSpec,
    ) -> NonNullColumnValues:
        return self.data_generator_factory.get(table_column.generator_data_type).generate_values(
            total_rows=total_non_null_rows,
            constraints=table_column.generator_constraints,
            output_constraints=table_column.output_constraints,
        )

    def build_foreign_key_non_null_values(
        self,
        total_non_null_rows: int,
        table_column: TableColumnSpec,
        referenced_values: NonNullColumnValues,
    ) -> NonNullColumnValues:
        """Строит значения FK-колонки, выбирая их из уже сгенерированного пула родительских ключей.

        Для ONE_TO_MANY допускаются повторы через choices.
        Для ONE_TO_ONE повторы запрещены, поэтому используется sample и заранее проверяется,
        что у родителя достаточно уникальных значений для всех non-null строк ребёнка.
        """
        foreign_key = table_column.foreign_key
        if foreign_key is None:
            raise GenerationInvariantError(f"Column {table_column.name} is not a foreign key")

        if not referenced_values and total_non_null_rows > 0:
            raise UnsatisfiableConstraintsError(
                f"Foreign key column {table_column.name} has no non-null referenced values to sample from"
            )

        if foreign_key.relation_type == RelationType.ONE_TO_MANY:
            return self.rng.choices(referenced_values, k=total_non_null_rows)

        if total_non_null_rows > len(referenced_values):
            raise UnsatisfiableConstraintsError(
                f"Foreign key column {table_column.name} requires {total_non_null_rows} unique referenced values, "
                f"but only {len(referenced_values)} are available"
            )
        return self.rng.sample(referenced_values, total_non_null_rows)

    def finalize_output_values(
        self,
        table_column: TableColumnSpec,
        output_non_null_values: NonNullColumnValues,
        total_rows: int,
        null_positions: set[int],
    ) -> ColumnValues:
        """Собирает финальный столбец и валидирует его уже в том виде, в котором он уйдёт в публикацию."""
        output_values = self.insert_nulls(
            values=output_non_null_values,
            total_rows=total_rows,
            null_positions=null_positions,
        )
        validate_column_output_values(table_column=table_column, values=output_values)
        return output_values

    @staticmethod
    def collect_derived_columns_by_source(
        table: TableSpec,
    ) -> dict[str, list[TableColumnSpec]]:
        """Группирует производные колонки по имени исходной, чтобы не пересчитывать source values повторно."""
        derived_columns_by_source: dict[str, list[TableColumnSpec]] = {}

        for table_column in table.columns:
            if not table_column.is_derived:
                continue

            derivation = table_column.derivation
            if derivation is None:
                raise GenerationInvariantError(f"Column {table_column.name} is marked as derived without derivation config")

            derived_columns_by_source.setdefault(derivation.source_column, []).append(table_column)

        return derived_columns_by_source

    def generate_table_data(
        self,
        table: TableSpec,
        fk_reference_tracker: ForeignKeyReferenceTracker,
    ) -> GeneratedTableData:
        """Генерирует все колонки одной таблицы, соблюдая зависимость generated -> derived и опираясь на FK-кэш.

        Порядок внутри таблицы намеренно такой:
        1. пропускаются производные колонки, потому что они не являются самостоятельным источником значений
        2. для каждой базовой или FK-колонки заранее выбираются позиции NULL
        3. обычные колонки проходят путь source generation -> output conversion -> finalize
        4. производные колонки вычисляются из тех же non-null source values, что и исходная колонка
        5. в конце проверяется, что не осталось ни одной неразрешённой колонки
        """
        derived_columns_by_source = self.collect_derived_columns_by_source(table)
        generated_columns: GeneratedColumnsByName = {}
        table_started_at = time.monotonic()

        for table_column in table.columns:
            if table_column.is_derived:
                continue

            total_nulls = int(table.total_rows * table_column.output_constraints.null_ratio)
            total_non_null_rows = table.total_rows - total_nulls
            null_positions = set(self.rng.sample(range(table.total_rows), total_nulls)) if total_nulls else set()

            if table_column.is_foreign_key:
                foreign_key_output_non_null_values = self.build_foreign_key_non_null_values(
                    total_non_null_rows=total_non_null_rows,
                    table_column=table_column,
                    referenced_values=fk_reference_tracker.get_cached_parent_values(table_column),
                )
                generated_columns[table_column.name] = self.finalize_output_values(
                    table_column=table_column,
                    output_non_null_values=foreign_key_output_non_null_values,
                    total_rows=table.total_rows,
                    null_positions=null_positions,
                )
                continue

            derived_columns = derived_columns_by_source.get(table_column.name, [])
            source_non_null_values = self.generate_non_null_source_values(
                total_non_null_rows=total_non_null_rows,
                table_column=table_column,
            )
            base_output_non_null_values = self.value_converter.convert(
                values=source_non_null_values,
                table_column=table_column,
            )
            generated_columns[table_column.name] = self.finalize_output_values(
                table_column=table_column,
                output_non_null_values=base_output_non_null_values,
                total_rows=table.total_rows,
                null_positions=null_positions,
            )

            if not derived_columns:
                continue

            for derived_column in derived_columns:
                derived_output_non_null_values = self.derivation_policy.derive_output_values(
                    table_column=derived_column,
                    source_non_null_values=source_non_null_values,
                )
                generated_columns[derived_column.name] = self.finalize_output_values(
                    table_column=derived_column,
                    output_non_null_values=derived_output_non_null_values,
                    total_rows=table.total_rows,
                    null_positions=null_positions,
                )

        missing_columns = [column.name for column in table.columns if column.name not in generated_columns]
        if missing_columns:
            missing_columns_text = ", ".join(missing_columns)
            raise GenerationInvariantError(
                f"Table {table.table_name} has unresolved columns after generation: {missing_columns_text}"
            )

        total_elapsed_ms = int((time.monotonic() - table_started_at) * 1000)
        logger.info(
            f"Table data generated: table={table.table_name}, rows={table.total_rows}, "
            f"columns={len(table.columns)}, elapsed_ms={total_elapsed_ms}"
        )

        return GeneratedTableData(
            table=table,
            generated_data={column.name: generated_columns[column.name] for column in table.columns},
        )

    def generate_tables(self, generation_run: GenerationRun) -> Iterator[GeneratedTableData]:
        """Генерирует таблицы лениво в dependency-safe порядке и обслуживает жизненный цикл FK-кэша между ними."""
        ordered_tables = self.table_dependency_planner.plan(generation_run.tables)
        fk_reference_tracker = ForeignKeyReferenceTracker.from_ordered_tables(ordered_tables)

        for table in ordered_tables:
            table_data = self.generate_table_data(
                table=table,
                fk_reference_tracker=fk_reference_tracker,
            )
            fk_reference_tracker.cache_parent_values(table, table_data.generated_data)
            yield table_data
            fk_reference_tracker.release_parent_cache_for_child(table)

    def generate(self, generation_run: GenerationRun) -> list[GeneratedTableData]:
        """Материализует ленивую генерацию целиком в список, если вызывающему нужен eager-результат."""
        return list(self.generate_tables(generation_run))
