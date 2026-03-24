import random
from typing import Any

from app.core.application.ports.generator_factory_port import DataGeneratorFactoryPort
from app.core.application.ports.table_dependency_planner_port import TableDependencyPlannerPort
from app.core.application.ports.value_converter_port import ValueConverterPort
from app.core.domain.entities import TableColumnSpec, GeneratedTableData, GenerationRun
from app.core.domain.enums import RelationType
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.logger import generation_logger
from app.shared.utils import shuffle_values_with_nulls

logger = generation_logger


class DataGenerationService:
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

    def generate_column_values(self, total_rows: int, table_column: TableColumnSpec) -> list[Any]:
        total_nulls = int(total_rows * (table_column.output_constraints.null_ratio / 100))
        total_non_nulls = total_rows - total_nulls

        values = self.data_generator_factory.get(table_column.generator_data_type).generate_values(
            total_rows=total_non_nulls,
            constraints=table_column.generator_constraints,
            output_constraints=table_column.output_constraints,
        )
        values = self.value_converter.convert(values=values, table_column=table_column)

        if table_column.output_constraints.null_ratio > 0:
            values = shuffle_values_with_nulls(target_count=total_nulls, values=values, rng=self.rng)

        return values

    def generate_foreign_key_values(
        self,
        total_rows: int,
        table_column: TableColumnSpec,
        referenced_values: list[Any],
    ) -> list[Any]:
        total_nulls = int(total_rows * (table_column.output_constraints.null_ratio / 100))
        total_non_nulls = total_rows - total_nulls
        reference_pool = [value for value in referenced_values if value is not None]

        if not reference_pool and total_non_nulls > 0:
            raise UnsatisfiableConstraintsError(
                f"Foreign key column {table_column.name} has no non-null referenced values to sample from"
            )

        if table_column.foreign_key.relation_type == RelationType.ONE_TO_MANY:
            values = self.rng.choices(reference_pool, k=total_non_nulls)
        else:
            if total_non_nulls > len(reference_pool):
                raise UnsatisfiableConstraintsError(
                    f"Foreign key column {table_column.name} requires {total_non_nulls} unique referenced values, "
                    f"but only {len(reference_pool)} are available"
                )
            values = self.rng.sample(reference_pool, total_non_nulls)

        if total_nulls > 0:
            values = shuffle_values_with_nulls(target_count=total_nulls, values=values, rng=self.rng)

        return values

    @staticmethod
    def validate_generated_values(table_column: TableColumnSpec, values: list[Any]) -> None:
        non_null_values = [value for value in values if value is not None]

        if table_column.is_primary_key and len(non_null_values) != len(values):
            raise InvalidConstraintsError(f"Primary key column {table_column.name} cannot contain null values")

        if table_column.output_constraints.is_unique and len(set(non_null_values)) != len(non_null_values):
            raise UnsatisfiableConstraintsError(
                f"Generated values for column {table_column.name} are not unique in final output"
            )

    def generate(self, generation_run: GenerationRun) -> list[GeneratedTableData]:
        ordered_tables = self.table_dependency_planner.plan(generation_run.tables)

        generated_table_data = {}
        table_data_results = []

        for table in ordered_tables:
            generated_columns = {}

            for table_column in table.columns:
                fk_info = table_column.foreign_key
                if fk_info:
                    fk_data = generated_table_data[fk_info.full_table_name][fk_info.column_name]
                    generated_columns[table_column.name] = self.generate_foreign_key_values(
                        total_rows=table.total_rows,
                        table_column=table_column,
                        referenced_values=fk_data,
                    )
                else:
                    generated_columns[table_column.name] = self.generate_column_values(
                        total_rows=table.total_rows,
                        table_column=table_column,
                    )

                self.validate_generated_values(
                    table_column=table_column,
                    values=generated_columns[table_column.name],
                )

            generated_table_data[table.full_table_name] = generated_columns
            table_data_results.append(
                GeneratedTableData(
                    table=table,
                    generated_data=generated_columns,
                )
            )
            logger.info(
                f"Table generated: table={table.full_table_name}, rows={table.total_rows}, columns={len(table.columns)}"
            )

        return table_data_results
