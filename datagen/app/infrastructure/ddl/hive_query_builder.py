from app.core.domain.entities import TableSpec
from app.core.domain.enums import DataType
from app.infrastructure.ddl.base_sql_query_builder import BaseSqlQueryBuilder


class HiveQueryBuilder(BaseSqlQueryBuilder):
    type_mapping: dict[DataType, str] = {
        DataType.STRING: "string",
        DataType.BOOLEAN: "boolean",
        DataType.DATE: "date",
        DataType.TIMESTAMP: "timestamp",
        DataType.INT: "bigint",
        DataType.FLOAT: "double",
    }

    def generate_table_ddl(self, table: TableSpec, target_table_name: str) -> str:
        columns_definition = self.build_columns_definition(table.columns)
        return (
            f"CREATE TABLE IF NOT EXISTS {target_table_name} (\n"
            f"  {columns_definition}\n"
            f")\n"
            f"STORED AS PARQUET;"
        )
