import random
from typing import Any, List

from app.core.application.ports.dependency_graph_builder_port import IDependencyGraphBuilder
from app.core.application.ports.generator_factory_port import IDataGeneratorFactory
from app.core.application.ports.value_converter_port import IValueConverter
from app.core.domain.entities import TableColumnSpec, GeneratedTableData, GenerationRun
from app.core.domain.enums import RelationType
from app.shared.logger import logger
from app.shared.utils import shuffle_values_with_nulls


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

    def generate_table_data(self, generation_run: GenerationRun) -> List[GeneratedTableData]:
        ordered_tables = self.dependency_order_builder.build_graph(generation_run.tables)

        generated_table_data = {}
        table_data_results = []

        for table in ordered_tables:
            generated_columns = {}

            for table_column in table.columns:
                fk_info = table_column.foreign_key
                if fk_info:
                    fk_data = generated_table_data[fk_info.full_table_name][fk_info.column_name]

                    if fk_info.relation_type == RelationType.ONE_TO_MANY:
                        generated_columns[table_column.name] = random.choices(fk_data, k=table.total_rows)
                    elif fk_info.relation_type == RelationType.ONE_TO_ONE:
                        generated_columns[table_column.name] = random.sample(fk_data, table.total_rows)
                else:
                    generated_columns[table_column.name] = self.generate_column_values(
                        total_rows=table.total_rows,
                        table_column=table_column,
                    )

            generated_table_data[table.full_table_name] = generated_columns
            table_data_results.append(
                GeneratedTableData(
                    table=table,
                    generated_data=generated_columns,
                )
            )
            logger.info(
                f"Generated table {table.full_table_name} "
                f"rows={table.total_rows} columns={len(table.columns)}"
            )

        return table_data_results
