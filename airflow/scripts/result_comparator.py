import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame, SparkSession

from job_common import ComparisonContract, logger, parse_job_contract, write_json_to_uri



@contextmanager
def open_spark_session(app_name: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.session.timeZone", "Europe/Moscow")
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
    parser = argparse.ArgumentParser(description="DataGen: compare Hive and Iceberg query results")
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--contract", required=True)
    return parser.parse_args()


@dataclass(frozen=True, slots=True)
class EngineCounts:
    hive: int
    iceberg: int


@dataclass(frozen=True, slots=True)
class EngineRatios:
    hive: float
    iceberg: float


@dataclass(frozen=True, slots=True)
class ComparisonMetrics:
    row_count: EngineCounts
    row_count_delta: int
    exclusive_row_count: EngineCounts
    exclusive_row_ratio: EngineRatios

    def status(self) -> str:
        if self.exclusive_row_count.hive == 0 and self.exclusive_row_count.iceberg == 0:
            return "MATCH"
        return "MISMATCH"


class ComparisonReportBuilder:

    @staticmethod
    def to_checked_at() -> str:
        return datetime.now(ZoneInfo("Europe/Moscow")).replace(microsecond=0).isoformat()

    @staticmethod
    def calculate_ratio(exclusive_count: int, total_count: int) -> float:
        if total_count == 0:
            return 0.0
        return round(exclusive_count / total_count, 6)

    def build(
        self,
        run_id: str,
        hive_result_uri: str,
        iceberg_result_uri: str,
        metrics: ComparisonMetrics,
    ) -> dict[str, object]:
        """Build comparison_result.json payload.

        Example:
        {
            "run_id": "...",
            "checked_at": "...",
            "status": "MATCH | MISMATCH",
            "summary": {
                "row_count": {"hive": 10000, "iceberg": 10000},
                "row_count_delta": 0,
                "exclusive_row_count": {"hive": 0, "iceberg": 0},
                "exclusive_row_ratio": {"hive": 0.0, "iceberg": 0.0}
            },
            "artifacts": {
                "hive_result_uri": "...",
                "iceberg_result_uri": "..."
            }
        }

        row_count_delta is abs(row_count.hive - row_count.iceberg).
        exclusive_row_count is the bilateral exceptAll residual per engine.
        exclusive_row_ratio is exclusive_row_count / row_count for each engine.
        """
        return {
            "run_id": run_id,
            "checked_at": self.to_checked_at(),
            "status": metrics.status(),
            "summary": {
                "row_count": {
                    "hive": metrics.row_count.hive,
                    "iceberg": metrics.row_count.iceberg,
                },
                "row_count_delta": metrics.row_count_delta,
                "exclusive_row_count": {
                    "hive": metrics.exclusive_row_count.hive,
                    "iceberg": metrics.exclusive_row_count.iceberg,
                },
                "exclusive_row_ratio": {
                    "hive": metrics.exclusive_row_ratio.hive,
                    "iceberg": metrics.exclusive_row_ratio.iceberg,
                },
            },
            "artifacts": {
                "hive_result_uri": hive_result_uri,
                "iceberg_result_uri": iceberg_result_uri,
            },
        }


class ResultComparator:

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
    def align_columns(hive_df: DataFrame, iceberg_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        ordered_columns = sorted(hive_df.columns)
        return (
            hive_df.select(*ordered_columns),
            iceberg_df.select(*ordered_columns),
        )

    @staticmethod
    def calculate_metrics(hive_df: DataFrame, iceberg_df: DataFrame) -> ComparisonMetrics:
        hive_df, iceberg_df = ResultComparator.align_columns(hive_df, iceberg_df)
        hive_df.persist()
        iceberg_df.persist()
        try:
            hive_row_count = hive_df.count()
            iceberg_row_count = iceberg_df.count()
            hive_exclusive_row_count = hive_df.exceptAll(iceberg_df).count()
            iceberg_exclusive_row_count = iceberg_df.exceptAll(hive_df).count()
            return ComparisonMetrics(
                row_count=EngineCounts(
                    hive=hive_row_count,
                    iceberg=iceberg_row_count,
                ),
                row_count_delta=abs(hive_row_count - iceberg_row_count),
                exclusive_row_count=EngineCounts(
                    hive=hive_exclusive_row_count,
                    iceberg=iceberg_exclusive_row_count,
                ),
                exclusive_row_ratio=EngineRatios(
                    hive=ComparisonReportBuilder.calculate_ratio(hive_exclusive_row_count, hive_row_count),
                    iceberg=ComparisonReportBuilder.calculate_ratio(iceberg_exclusive_row_count, iceberg_row_count),
                ),
            )
        finally:
            hive_df.unpersist()
            iceberg_df.unpersist()

    @staticmethod
    def build_outcome_message(run_id: str, metrics: ComparisonMetrics) -> str:
        status_line = "Comparison passed." if metrics.status() == "MATCH" else "Comparison mismatch detected."
        return (
            f"{status_line}\n"
            f"run_id: {run_id}\n"
            f"row_count:\n"
            f"  hive: {metrics.row_count.hive}\n"
            f"  iceberg: {metrics.row_count.iceberg}\n"
            f"row_count_delta: {metrics.row_count_delta}\n"
            f"exclusive_row_count:\n"
            f"  hive: {metrics.exclusive_row_count.hive}\n"
            f"  iceberg: {metrics.exclusive_row_count.iceberg}\n"
            f"exclusive_row_ratio:\n"
            f"  hive: {metrics.exclusive_row_ratio.hive}\n"
            f"  iceberg: {metrics.exclusive_row_ratio.iceberg}"
        )

    def execute(self, run_id: str, comparison_contract: ComparisonContract) -> None:
        hive_contract = comparison_contract.get_engine_contract("hive")
        iceberg_contract = comparison_contract.get_engine_contract("iceberg")

        logger.info(
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

        logger.info(
            f"{self.build_outcome_message(run_id, metrics)}\n"
            f"report_uri: {comparison_contract.report_uri}"
        )


if __name__ == "__main__":
    args = parse_args()
    contract = parse_job_contract(args.contract)
    with open_spark_session(args.app_name) as spark_session:
        ResultComparator(
            spark=spark_session,
            report_builder=ComparisonReportBuilder(),
        ).execute(
            run_id=contract.run_id,
            comparison_contract=contract.comparison,
        )
