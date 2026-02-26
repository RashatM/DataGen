from typing import Dict

from app.core.application.ports.query_builder_port import IQueryBuilderService
from app.core.domain.entities import MockDataColumn, MockDataEntity
from app.core.domain.enums import DataType


class PostgresQueryBuilderService(IQueryBuilderService):
    default_type_mapping: Dict[DataType, str] = {
        DataType.STRING: "varchar",
        DataType.BOOLEAN: "boolean",
        DataType.DATE: "date",
        DataType.TIMESTAMP: "timestamp",
        DataType.INT: "bigint",
        DataType.FLOAT: "double precision",
    }

    def map_column_type(self, entity_column: MockDataColumn) -> str:
        column_type = self.default_type_mapping.get(entity_column.output_data_type)
        if not column_type:
            raise ValueError(f"Unsupported output data type: {entity_column.output_data_type}")
        return column_type

    def create_ddl(self, entity: MockDataEntity) -> str:
        columns_sql = []
        for column in entity.columns:
            column_type = self.map_column_type(column)
            columns_sql.append(f"{column.name} {column_type} NULL")

        columns_definition = ",\n  ".join(columns_sql)
        return f"CREATE TABLE {entity.full_table_name} (\n  {columns_definition}\n);"
