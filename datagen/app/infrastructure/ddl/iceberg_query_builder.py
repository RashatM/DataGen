from typing import Dict

from app.core.domain.entities import MockDataEntity
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


    def __init__(self, write_meta_num: str = "{{ WRITE_META_NUM }}"):
        self.write_meta_num = write_meta_num


    def create_ddl(self, entity: MockDataEntity) -> str:
        columns_definition = self.build_columns_definition(entity.columns)
        write_meta_num = len(entity.columns)
        return (
            f"CREATE TABLE IF NOT EXISTS {entity.full_table_name} (\n"
            f"  {columns_definition}\n"
            f")\n"
            f"USING ICEBERG\n"
            f"""TBLPROPERTIES (
              'format' = 'iceberg/parquet',
              'format-version' = '2',
              'write.distribution-mode' = 'hash',
              'write.delete.mode' = 'merge-on-read',
              'write.merge.mode' = 'merge-on-read',
              'write.update.mode' = 'merge-on-read',
              'write.metadata.delete-after-commit.enabled' = 'true',
              'write.metadata.previous-versions-max' = '100',
              'write.metadata.metrics.default' = 'full',
              'write.parquet.compression-codec' = 'zstd',
              'write.parquet.compression-level' = '3',
              'write.delete.granularity' = 'file',
              'commit.retry.num-retries' = '20',
              'commit.retry.min-wait-ms' = '100',
              'commit.retry.max-wait-ms' = '5000',
              'write.metadata.metrics.max-inferred-column-defaults' = '{write_meta_num}'
            )
            """
        )
