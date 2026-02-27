from abc import ABC
from typing import Dict, List

from app.core.application.ports.query_builder_port import IQueryBuilder
from app.core.domain.entities import MockDataColumn
from app.core.domain.enums import DataType
from app.infrastructure.errors import UnsupportedOutputDataTypeError


class BaseSqlQueryBuilder(IQueryBuilder, ABC):
    type_mapping: Dict[DataType, str] = {}

    def map_column_type(self, entity_column: MockDataColumn) -> str:
        column_type = self.type_mapping.get(entity_column.output_data_type)
        if not column_type:
            raise UnsupportedOutputDataTypeError(
                f"Unsupported output data type: {entity_column.output_data_type}"
            )
        return column_type

    def build_columns_definition(self, columns: List[MockDataColumn]) -> str:
        columns_sql: List[str] = []
        for column in columns:
            column_type = self.map_column_type(column)
            columns_sql.append(f"{column.name} {column_type}")
        return ",\n  ".join(columns_sql)
