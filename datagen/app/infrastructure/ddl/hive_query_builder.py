from typing import Dict

from app.core.domain.entities import MockDataEntity
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

    def create_ddl(self, entity: MockDataEntity) -> str:
        columns_definition = self.build_columns_definition(entity.columns)
        return (
            f"CREATE TABLE IF NOT EXISTS {entity.full_table_name} (\n"
            f"  {columns_definition}\n"
            f")\n"
            f"STORED AS PARQUET;"
        )
