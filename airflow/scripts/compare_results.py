import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from base_loader import ComparisonContract, airflow_logger, parse_comparison_contract, write_json_to_uri


@contextmanager
def open_spark_session(app_name: str):
    spark_session = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("INFO")
        airflow_logger.info("Spark session opened.")
        yield spark_session
    finally:
        spark_session.stop()
        airflow_logger.info("Spark session closed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataGen: compare Hive and Iceberg query results")
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--contract", required=True)
    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class EngineCounts:
    hive: int
    iceberg: int


@dataclass(frozen=True, slots=True)
class ComparisonMetrics:
    row_count: EngineCounts
    unmatched_row_count: EngineCounts

    def status(self) -> str:
        if self.unmatched_row_count.hive == 0 and self.unmatched_row_count.iceberg == 0:
            return "MATCH"
        return "MISMATCH"


class ComparisonReportBuilder:

    @staticmethod
    def to_checked_at() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def build(
        self,
        run_id: str,
        hive_result_uri: str,
        iceberg_result_uri: str,
        metrics: ComparisonMetrics,
    ) -> Dict[str, object]:
        return {
            "run_id": run_id,
            "checked_at": self.to_checked_at(),
            "status": metrics.status(),
            "summary": {
                "row_count": {
                    "hive": metrics.row_count.hive,
                    "iceberg": metrics.row_count.iceberg,
                },
                "unmatched_row_count": {
                    "hive": metrics.unmatched_row_count.hive,
                    "iceberg": metrics.unmatched_row_count.iceberg,
                },
            },
            "artifacts": {
                "hive_result_uri": hive_result_uri,
                "iceberg_result_uri": iceberg_result_uri,
            },
        }


class ComparisonJob:

    def __init__(
        self,
        spark: SparkSession,
        report_builder: ComparisonReportBuilder,
    ) -> None:
        self.spark = spark
        self.report_builder = report_builder

    @staticmethod
    def validate_schemas(hive_df: DataFrame, iceberg_df: DataFrame) -> None:
        if set(hive_df.columns) != set(iceberg_df.columns):
            raise ValueError(
                "Hive and Iceberg results must have identical column names: "
                f"hive={sorted(hive_df.columns)}, iceberg={sorted(iceberg_df.columns)}"
            )
        hive_types = {field.name: field.dataType.simpleString() for field in hive_df.schema.fields}
        iceberg_types = {field.name: field.dataType.simpleString() for field in iceberg_df.schema.fields}
        mismatched = {
            col: (hive_types[col], iceberg_types[col])
            for col in hive_types
            if hive_types[col] != iceberg_types[col]
        }
        if mismatched:
            raise ValueError(
                f"Schema mismatch after normalization: {mismatched}"
            )

    @staticmethod
    def calculate_metrics(hive_df: DataFrame, iceberg_df: DataFrame) -> ComparisonMetrics:
        hive_df.persist()
        iceberg_df.persist()
        try:
            return ComparisonMetrics(
                row_count=EngineCounts(
                    hive=hive_df.count(),
                    iceberg=iceberg_df.count(),
                ),
                unmatched_row_count=EngineCounts(
                    hive=hive_df.exceptAll(iceberg_df).count(),
                    iceberg=iceberg_df.exceptAll(hive_df).count(),
                ),
            )
        finally:
            hive_df.unpersist()
            iceberg_df.unpersist()

    def execute(self, run_id: str, comparison_contract: ComparisonContract) -> None:
        hive_contract = comparison_contract.for_engine("hive")
        iceberg_contract = comparison_contract.for_engine("iceberg")

        airflow_logger.info(
            f"Comparison started. run_id={run_id}, hive_result_uri={hive_contract.result_uri}, "
            f"iceberg_result_uri={iceberg_contract.result_uri}"
        )

        hive_df = self.spark.read.parquet(hive_contract.result_uri)
        iceberg_df = self.spark.read.parquet(iceberg_contract.result_uri)
        self.validate_schemas(hive_df, iceberg_df)
        metrics = self.calculate_metrics(hive_df, iceberg_df)

        report = self.report_builder.build(
            run_id=run_id,
            hive_result_uri=hive_contract.result_uri,
            iceberg_result_uri=iceberg_contract.result_uri,
            metrics=metrics,
        )
        write_json_to_uri(self.spark, comparison_contract.report_uri, report)

        airflow_logger.info(
            f"Comparison completed. run_id={run_id}, status={metrics.status()}, "
            f"report_uri={comparison_contract.report_uri}"
        )


if __name__ == "__main__":
    args = parse_args()
    with open_spark_session(args.app_name) as spark_session:
        ComparisonJob(
            spark=spark_session,
            report_builder=ComparisonReportBuilder(),
        ).execute(
            run_id=args.run_id,
            comparison_contract=parse_comparison_contract(args.contract),
        )
