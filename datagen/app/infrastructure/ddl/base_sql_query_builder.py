from abc import ABC, abstractmethod
from typing import Dict, List

from app.core.application.dto.publication import EngineLoadPayload
from app.core.application.ports.table_load_payload_builder_port import ITableLoadPayloadBuilder
from app.core.domain.entities import TableColumnSpec, TableSpec
from app.core.domain.enums import DataType
from app.infrastructure.errors import UnsupportedOutputDataTypeError


class BaseSqlQueryBuilder(ITableLoadPayloadBuilder, ABC):
    type_mapping: Dict[DataType, str] = {}

    def __init__(self, database_name: str):
        self.database_name = database_name

    def map_column_type(self, table_column: TableColumnSpec) -> str:
        column_type = self.type_mapping.get(table_column.output_data_type)
        if not column_type:
            raise UnsupportedOutputDataTypeError(
                f"Unsupported output data type: {table_column.output_data_type}"
            )
        return column_type

    def build_columns_definition(self, columns: List[TableColumnSpec]) -> str:
        columns_sql: List[str] = []
        for column in columns:
            column_type = self.map_column_type(column)
            columns_sql.append(f"{column.name} {column_type}")
        return ",\n  ".join(columns_sql)

    def build_target_table_name(self, table: TableSpec) -> str:
        return f"{self.database_name}.{table.schema_name}__{table.table_name}"

    @abstractmethod
    def generate_table_ddl(self, table: TableSpec, target_table_name: str) -> str:
        pass

    def build_load_payload(self, table: TableSpec) -> EngineLoadPayload:
        target_table_name = self.build_target_table_name(table)
        ddl_query = self.generate_table_ddl(table, target_table_name)
        return EngineLoadPayload(
            ddl_query=ddl_query,
            target_table_name=target_table_name,
        )
