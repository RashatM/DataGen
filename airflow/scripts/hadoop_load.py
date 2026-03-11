import argparse
from contextlib import contextmanager
from pyspark.sql import SparkSession

from common import BaseLoader, parse_table_contracts, logger


@contextmanager
def open_spark_session(app_name: str):
    spark_session = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonestrict")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.hive.convertMetastoreParquet", False)
        .config("spark.sql.hive.convertMetastoreOrc", False)
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.parquet.filterPushdown", True)
        .config("spark.sql.orc.filterPushdown", False)
        .config("spark.sql.orc.mergeSchema", True)
        .config("spark.sql.orc.compression.codec", 'snappy')
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", True)
        .config("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.adaptive.coalescePartitions.enabled", True)
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", 1)
        .config("spark.sql.session.timeZone", "Europe/Moscow")
        .enableHiveSupport()
        .getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("INFO")
        logger.info("Spark session opened")
        yield spark_session
    finally:
        spark_session.stop()
        logger.info("Spark session closed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataGen: S3 to Hadoop/Hive loader")
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--contract", required=True)
    return parser.parse_args()


class HadoopLoader(BaseLoader):

    def write_to_tmp(self, data_uri: str, tmp_name: str) -> None:
        (
            self.spark.read
            .parquet(data_uri)
            .hint("REBALANCE")
            .write
            .mode("overwrite")
            .format("hive")
            .insertInto(tmp_name, overwrite=True)
        )


if __name__ == "__main__":
    args = parse_args()
    contract = parse_contract(args.contract)
    run_id = get_run_id(contract)
    tables = parse_table_contracts(args.contract, ddl_target="hive")

    with open_spark_session(args.app_name) as spark:
        loader = HadoopLoader(spark, run_id=args.run_id)
        loader.load_all(tables)
