import argparse
from contextlib import contextmanager
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
import pyspark.sql.functions as f

from base_loader import BaseSynthLoader
from job_common import logger, parse_job_contract


@contextmanager
def open_spark_session(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("INFO")
        logger.info("Spark session opened.")
        yield spark
    finally:
        spark.stop()
        logger.info("Spark session closed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataGen: S3 to Iceberg loader")
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--contract", required=True)
    return parser.parse_args()


class IcebergSynthLoader(BaseSynthLoader):

    ICEBERG_TABLE_PROPERTIES = [
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
    ]

    def build_create_table_sql(self, schema: StructType, table_name: str) -> str:
        cols = ", ".join(f"`{field.name}` {field.dataType.simpleString()}" for field in schema)
        props = self.ICEBERG_TABLE_PROPERTIES + [
            f"'write.metadata.metrics.max-inferred-column-defaults' = '{len(schema)}'",
        ]
        props_sql = ", ".join(props)
        return f"CREATE TABLE {table_name} ({cols}) USING ICEBERG TBLPROPERTIES ({props_sql})"

    def load_into_table(self, df: DataFrame, table_name: str) -> None:
        df.writeTo(table_name).overwrite(f.lit(True))


if __name__ == "__main__":
    args = parse_args()
    contract = parse_job_contract(args.contract)

    with open_spark_session(args.app_name) as spark_session:
        loader = IcebergSynthLoader(spark_session, run_id=contract.run_id)
        tables = contract.build_table_contracts("iceberg")
        loader.publish_tables(tables)
        loader.materialize_comparison_result(contract.comparison, "iceberg")
