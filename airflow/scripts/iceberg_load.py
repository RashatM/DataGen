import argparse
from contextlib import contextmanager
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from base_loader import BaseSynthLoader
from job_common import ComparisonContract, logger, parse_job_contract


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
    DEFAULT_PATH_COMMIT_PROTOCOL = "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"

    def write_to_tmp(self, data_uri: str, tmp_name: str) -> None:
        self.spark.read.parquet(data_uri).writeTo(tmp_name).overwrite(f.lit(True))

    def materialize_comparison_result(self, comparison_contract: ComparisonContract, engine: str) -> None:
        engine_contract = comparison_contract.get_engine_contract(engine)
        logger.info(
            f"Comparison query execution started. engine={engine}, run_id={self.run_id}, "
            f"query_uri={engine_contract.query_uri}, result_uri={engine_contract.result_uri}"
        )
        comparison_query = self.read_query_from_uri(engine_contract.query_uri)
        comparison_df = self.spark.sql(comparison_query)
        normalized_df = self.comparison_data_normalizer.normalize(comparison_df)
        self.spark.conf.set("spark.sql.sources.commitProtocolClass", self.DEFAULT_PATH_COMMIT_PROTOCOL)
        try:
            normalized_df.write.mode("overwrite").parquet(engine_contract.result_uri)
        except Exception as error:
            raise RuntimeError(
                f"Failed to write comparison parquet. engine={engine}, "
                f"result_uri={engine_contract.result_uri}"
            ) from error
        logger.info(
            f"Comparison query execution completed. engine={engine}, run_id={self.run_id}, "
            f"result_uri={engine_contract.result_uri}"
        )


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
