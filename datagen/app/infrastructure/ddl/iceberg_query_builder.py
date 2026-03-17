from typing import Dict

from app.core.domain.entities import TableSpec
from app.core.domain.enums import DataType
from app.infrastructure.ddl.base_sql_query_builder import BaseSqlQueryBuilder


class IcebergQueryBuilder(BaseSqlQueryBuilder):
    type_mapping: Dict[DataType, str] = {
        DataType.STRING: "string",
        DataType.BOOLEAN: "boolean",
        DataType.DATE: "date",
        DataType.TIMESTAMP: "timestamp",
        DataType.INT: "bigint",
        DataType.FLOAT: "double",
    }

    @staticmethod
    def build_table_properties_definition(write_meta_num: int) -> str:
        table_properties = [
            "'format' = 'iceberg/parquet'",
            "'format-version' = '2'",
            "'write.distribution-mode' = 'hash'",
            "'write.delete.mode' = 'merge-on-read'",
            "'write.merge.mode' = 'merge-on-read'",
            "'write.update.mode' = 'merge-on-read'",
            "'write.metadata.delete-after-commit.enabled' = 'true'",
            "'write.metadata.previous-versions-max' = '100'",
            "'write.metadata.metrics.default' = 'full'",
            "'write.parquet.compression-codec' = 'zstd'",
            "'write.parquet.compression-level' = '3'",
            "'write.delete.granularity' = 'file'",
            "'commit.retry.num-retries' = '20'",
            "'commit.retry.min-wait-ms' = '100'",
            "'commit.retry.max-wait-ms' = '5000'",
            f"'write.metadata.metrics.max-inferred-column-defaults' = '{write_meta_num}'",
        ]
        return ",\n  ".join(table_properties)

    def generate_table_ddl(self, table: TableSpec, target_table_name: str) -> str:
        columns_definition = self.build_columns_definition(table.columns)
        write_meta_num = len(table.columns)
        table_properties_definition = self.build_table_properties_definition(write_meta_num)
        return (
            f"CREATE TABLE IF NOT EXISTS {target_table_name} (\n"
            f"  {columns_definition}\n"
            f")\n"
            f"USING ICEBERG\n"
            f"TBLPROPERTIES (\n"
            f"  {table_properties_definition}\n"
            f")"
        )
