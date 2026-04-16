import argparse
from contextlib import contextmanager
from pyspark.sql import DataFrame, SparkSession
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


class IcebergSynthLoader(BaseSynthLoader):
    """Реализация загрузчика для Iceberg через DataFrameWriterV2 и встроенные overwrite semantics движка."""

    def append_to_table(self, df: DataFrame, table_name: str) -> None:
        df.writeTo(table_name).append()

    def overwrite_table(self, df: DataFrame, table_name: str) -> None:
        df.writeTo(table_name).overwrite(f.lit(True))

    def overwrite_partitions(self, df: DataFrame, table_name: str) -> None:
        df.writeTo(table_name).overwritePartitions()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataGen: S3 to Iceberg loader")
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--contract", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    contract = parse_job_contract(args.contract)

    with open_spark_session(args.app_name) as spark_session:
        loader = IcebergSynthLoader(spark_session, run_id=contract.run_id)
        tables = contract.build_load_contracts("iceberg")
        loader.publish_tables(tables)
        loader.materialize_comparison_result(contract.comparison, "iceberg")
