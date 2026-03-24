import argparse
from contextlib import contextmanager
from pyspark.sql import SparkSession
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
        .config("spark.sql.sources.commitProtocolClass", "org.apache.iceberg.spark.SparkCommitProtocol")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.files.maxPartitionBytes", 536870912)
        .config("spark.sql.files.openCostInBytes", 16777216)
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

    def write_to_tmp(self, data_uri: str, tmp_name: str) -> None:
        self.spark.read.parquet(data_uri).writeTo(tmp_name).overwrite(f.lit(True))


if __name__ == "__main__":
    args = parse_args()
    contract = parse_job_contract(args.contract)

    with open_spark_session(args.app_name) as spark_session:
        loader = IcebergSynthLoader(spark_session, run_id=contract.run_id)
        loader.execute(
            tables=contract.build_table_contracts("iceberg"),
            comparison_contract=contract.comparison,
            engine="iceberg",
        )
