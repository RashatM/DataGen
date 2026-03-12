from typing import Dict

from app.core.domain.entities import TableSpec
from app.core.domain.enums import DataType
from app.infrastructure.ddl.base_sql_query_builder import BaseSqlQueryBuilder


class HiveQueryBuilder(BaseSqlQueryBuilder):
    type_mapping: Dict[DataType, str] = {
        DataType.STRING: "string",
        DataType.BOOLEAN: "boolean",
        DataType.DATE: "date",
        DataType.TIMESTAMP: "timestamp",
        DataType.INT: "bigint",
        DataType.FLOAT: "double",
    }

    def generate_table_ddl(self, table: TableSpec) -> str:
        columns_definition = self.build_columns_definition(table.columns)
        target_table_name = self.build_target_table_name(table)
        return (
            f"CREATE TABLE IF NOT EXISTS {target_table_name} (\n"
            f"  {columns_definition}\n"
            f")\n"
            f"STORED AS PARQUET;"
        )
