import random
from typing import Any, List

from app.core.application.ports.dependency_graph_builder_port import IDependencyGraphBuilder
from app.core.application.ports.generator_factory_port import IDataGeneratorFactory
from app.core.application.ports.value_converter_port import IValueConverter
from app.core.domain.entities import TableColumnSpec, GeneratedTableData, GenerationRun
from app.core.domain.enums import RelationType
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.logger import generation_logger
from app.shared.utils import shuffle_values_with_nulls

logger = generation_logger


class DataGenerationService:
    def __init__(
        self,
        dependency_order_builder: IDependencyGraphBuilder,
        data_generator_factory: IDataGeneratorFactory,
        value_converter: IValueConverter,
    ):
        self.dependency_order_builder = dependency_order_builder
        self.data_generator_factory = data_generator_factory
        self.value_converter = value_converter

    def generate_column_values(self, total_rows: int, table_column: TableColumnSpec) -> List[Any]:
        total_nulls = int(total_rows * (table_column.constraints.null_ratio / 100))
        total_non_nulls = total_rows - total_nulls

        values = self.data_generator_factory.get(table_column.gen_data_type).generate_values(
            total_rows=total_non_nulls,
            constraints=table_column.constraints,
        )
        values = self.value_converter.convert(values=values, table_column=table_column)

        if table_column.constraints.null_ratio > 0:
            values = shuffle_values_with_nulls(target_count=total_nulls, values=values)

        return values

    @staticmethod
    def generate_foreign_key_values(
        total_rows: int,
        table_column: TableColumnSpec,
        referenced_values: List[Any],
    ) -> List[Any]:
        total_nulls = int(total_rows * (table_column.constraints.null_ratio / 100))
        total_non_nulls = total_rows - total_nulls
        reference_pool = [value for value in referenced_values if value]

        if not reference_pool and total_non_nulls > 0:
            raise UnsatisfiableConstraintsError(
                f"Foreign key column {table_column.name} has no non-null referenced values to sample from"
            )

        if table_column.foreign_key.relation_type == RelationType.ONE_TO_MANY:
            values = random.choices(reference_pool, k=total_non_nulls)
        else:
            if total_non_nulls > len(reference_pool):
                raise UnsatisfiableConstraintsError(
                    f"Foreign key column {table_column.name} requires {total_non_nulls} unique referenced values, "
                    f"but only {len(reference_pool)} are available"
                )
            values = random.sample(reference_pool, total_non_nulls)

        if total_nulls > 0:
            values = shuffle_values_with_nulls(target_count=total_nulls, values=values)

        return values

    @staticmethod
    def validate_generated_values(table_column: TableColumnSpec, values: List[Any]) -> None:
        non_null_values = [value for value in values if value]

        if table_column.is_primary_key and len(non_null_values) != len(values):
            raise InvalidConstraintsError(f"Primary key column {table_column.name} cannot contain null values")

        if getattr(table_column.constraints, "is_unique", False) and len(set(non_null_values)) != len(non_null_values):
            raise UnsatisfiableConstraintsError(
                f"Generated values for column {table_column.name} are not unique in final output"
            )

    def generate(self, generation_run: GenerationRun) -> List[GeneratedTableData]:
        ordered_tables = self.dependency_order_builder.build_graph(generation_run.tables)

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
