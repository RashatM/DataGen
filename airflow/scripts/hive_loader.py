import argparse
from contextlib import contextmanager
from pyspark.sql import DataFrame, SparkSession

from base_loader import BaseSynthLoader
from job_common import logger, parse_job_contract, run_with_diagnostic


@contextmanager
def open_spark_session(app_name: str):
    spark = (
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
        .config("spark.sql.orc.compression.codec", "snappy")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", True)
        .config("spark.sql.optimizer.dynamicPartitionPruning.fallbackFilterRatio", "0.5")
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.adaptive.coalescePartitions.enabled", True)
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", 1)
        .config("spark.sql.session.timeZone", "UTC")
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


class HiveSynthLoader(BaseSynthLoader):
    """Реализация загрузчика для Hive на Hadoop через insertInto и переключение partition overwrite mode."""

    @contextmanager
    def partition_overwrite_mode(self, mode: str):
        """Временно переключает режим overwrite партиций для Hive-записи через insertInto."""
        previous_mode = self.spark.conf.get("spark.sql.sources.partitionOverwriteMode", "static")
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", mode)
        try:
            yield
        finally:
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", previous_mode)

    def append_to_table(self, df: DataFrame, table_name: str) -> None:
        df.write.insertInto(table_name, overwrite=False)

    def overwrite_table(self, df: DataFrame, table_name: str) -> None:
        with self.partition_overwrite_mode("static"):
            df.write.insertInto(table_name, overwrite=True)

    def overwrite_partitions(self, df: DataFrame, table_name: str) -> None:
        with self.partition_overwrite_mode("dynamic"):
            df.write.insertInto(table_name, overwrite=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataGen: S3 to Hive loader")
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--task_id", required=True)
    parser.add_argument("--diagnostic_uri", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    contract = parse_job_contract(args.contract)

    with open_spark_session(args.app_name) as spark_session:
        def run_job() -> None:
            loader = HiveSynthLoader(spark_session, run_id=contract.run_id)
            tables = contract.build_load_contracts("hive")
            loader.publish_tables(tables)
            loader.materialize_comparison_result(contract.comparison, "hive")

        run_with_diagnostic(
            spark=spark_session,
            diagnostic_uri=args.diagnostic_uri,
            run_id=contract.run_id,
            task_id=args.task_id,
            action=run_job,
        )
