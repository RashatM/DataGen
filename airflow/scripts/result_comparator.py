import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f

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
    """Парная целочисленная метрика сравнения для Hive и Iceberg."""
    hive: int
    iceberg: int


@dataclass(frozen=True, slots=True)
class EngineRatios:
    """Парная ratio-метрика сравнения для Hive и Iceberg."""
    hive: float
    iceberg: float


@dataclass(frozen=True, slots=True)
class ComparisonMetrics:
    """Набор метрик, на основе которых строится итоговый comparison report."""
    row_count: EngineCounts
    row_count_delta: int
    exclusive_row_count: EngineCounts
    exclusive_row_ratio: EngineRatios

    def status(self) -> str:
        if self.exclusive_row_count.hive == 0 and self.exclusive_row_count.iceberg == 0:
            return "MATCH"
        return "MISMATCH"


@dataclass(frozen=True, slots=True)
class PreparedComparisonData:
    """Промежуточный результат подготовки двух parquet-результатов к честной сверке."""
    hive_df: DataFrame
    iceberg_df: DataFrame
    hive_ignored_by_name: tuple[str, ...]
    iceberg_ignored_by_name: tuple[str, ...]
    ignored_by_temporal_type: tuple[str, ...]


class ComparisonReportBuilder:
    """Строит финальный JSON-отчёт comparison_result.json из уже посчитанных метрик."""
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
    """Сверяет materialized query results двух движков и формирует comparison_result.json.

    Компаратор работает не напрямую с SQL, а с уже записанными parquet-результатами запросов.
    Его этапы такие:
    - применить engine-specific exclude_columns по именам
    - автоматически исключить колонки, если хотя бы с одной стороны тип temporal
    - проверить совпадение сравнимых колонок и их схем после нормализации
    - посчитать двусторонние exceptAll-метрики
    - записать итоговый report обратно в object storage
    """
    TEMPORAL_TYPE_NAMES = frozenset({"timestamp", "timestamp_ntz", "date"})

    def __init__(
        self,
        spark: SparkSession,
        report_builder: ComparisonReportBuilder,
    ) -> None:
        self.spark = spark
        self.report_builder = report_builder

    @staticmethod
    def filter_ignored_columns(df: DataFrame, ignored_columns: tuple[str, ...]) -> tuple[DataFrame, tuple[str, ...]]:
        """Удаляет из сравнения колонки, явно перечисленные в exclude_columns для конкретного движка."""
        ignored_names = {column_name.lower() for column_name in ignored_columns}
        kept_columns: list[str] = []
        ignored_by_name: list[str] = []

        for column_name in df.columns:
            if column_name.lower() in ignored_names:
                ignored_by_name.append(column_name)
                continue
            kept_columns.append(column_name)

        return df.select(*(f.col(column_name) for column_name in kept_columns)), tuple(ignored_by_name)

    @staticmethod
    def validate_column_sets(hive_df: DataFrame, iceberg_df: DataFrame) -> None:
        if set(hive_df.columns) != set(iceberg_df.columns):
            raise ValueError(
                "Hive and Iceberg results must have identical comparable column names: "
                f"hive={sorted(hive_df.columns)}, iceberg={sorted(iceberg_df.columns)}"
            )

    @classmethod
    def exclude_temporal_columns(cls, hive_df: DataFrame, iceberg_df: DataFrame) -> tuple[DataFrame, DataFrame, tuple[str, ...]]:
        """Исключает из сверки колонки, если хотя бы на одной стороне Spark считает их temporal-типом."""
        cls.validate_column_sets(hive_df, iceberg_df)

        hive_types = {field.name: field.dataType.typeName() for field in hive_df.schema.fields}
        iceberg_types = {field.name: field.dataType.typeName() for field in iceberg_df.schema.fields}

        ignored_by_temporal_type = tuple(
            sorted(
                column_name
                for column_name in hive_types
                if hive_types[column_name] in cls.TEMPORAL_TYPE_NAMES
                or iceberg_types[column_name] in cls.TEMPORAL_TYPE_NAMES
            )
        )
        if not ignored_by_temporal_type:
            return hive_df, iceberg_df, ignored_by_temporal_type

        kept_columns = [column_name for column_name in hive_df.columns if column_name not in ignored_by_temporal_type]
        return (
            hive_df.select(*(f.col(column_name) for column_name in kept_columns)),
            iceberg_df.select(*(f.col(column_name) for column_name in kept_columns)),
            ignored_by_temporal_type,
        )

    @staticmethod
    def validate_schemas(hive_df: DataFrame, iceberg_df: DataFrame) -> None:
        """Проверяет, что после всех исключений и нормализации обе стороны сравниваются по одинаковой схеме."""
        ResultComparator.validate_column_sets(hive_df, iceberg_df)
        hive_types = {field.name: field.dataType.simpleString() for field in hive_df.schema.fields}
        iceberg_types = {field.name: field.dataType.simpleString() for field in iceberg_df.schema.fields}
        mismatched = {
            col: (hive_types[col], iceberg_types[col])
            for col in hive_types
            if hive_types[col] != iceberg_types[col]
        }
        if mismatched:
            raise ValueError(f"Schema mismatch after normalization and temporal exclusion: {mismatched}")

    @staticmethod
    def align_columns(hive_df: DataFrame, iceberg_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        ordered_columns = sorted(hive_df.columns)
        return (
            hive_df.select(*ordered_columns),
            iceberg_df.select(*ordered_columns),
        )

    @classmethod
    def prepare_for_comparison(
        cls,
        comparison_contract: ComparisonContract,
        hive_df: DataFrame,
        iceberg_df: DataFrame,
    ) -> PreparedComparisonData:
        """Готовит обе стороны к сверке: применяет excludes, temporal-policy и schema validation."""
        hive_contract = comparison_contract.get_engine_contract("hive")
        iceberg_contract = comparison_contract.get_engine_contract("iceberg")

        filtered_hive_df, hive_ignored_by_name = cls.filter_ignored_columns(
            df=hive_df,
            ignored_columns=hive_contract.exclude_columns,
        )
        filtered_iceberg_df, iceberg_ignored_by_name = cls.filter_ignored_columns(
            df=iceberg_df,
            ignored_columns=iceberg_contract.exclude_columns,
        )
        comparable_hive_df, comparable_iceberg_df, ignored_by_temporal_type = cls.exclude_temporal_columns(
            hive_df=filtered_hive_df,
            iceberg_df=filtered_iceberg_df,
        )
        cls.validate_schemas(
            hive_df=comparable_hive_df,
            iceberg_df=comparable_iceberg_df,
        )

        return PreparedComparisonData(
            hive_df=comparable_hive_df,
            iceberg_df=comparable_iceberg_df,
            hive_ignored_by_name=hive_ignored_by_name,
            iceberg_ignored_by_name=iceberg_ignored_by_name,
            ignored_by_temporal_type=ignored_by_temporal_type,
        )

    @staticmethod
    def calculate_metrics(hive_df: DataFrame, iceberg_df: DataFrame) -> ComparisonMetrics:
        """Считает row counts и двусторонние exclusive counts через persist + bilateral exceptAll."""
        hive_df, iceberg_df = ResultComparator.align_columns(
            hive_df=hive_df,
            iceberg_df=iceberg_df,
        )
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
    def build_outcome_message(
        run_id: str,
        metrics: ComparisonMetrics,
        prepared_data: PreparedComparisonData,
    ) -> str:
        status_line = "Comparison passed." if metrics.status() == "MATCH" else "Comparison mismatch detected."
        return (
            f"{status_line}\n"
            f"run_id: {run_id}\n"
            f"ignored_by_name:\n"
            f"  hive: {list(prepared_data.hive_ignored_by_name)}\n"
            f"  iceberg: {list(prepared_data.iceberg_ignored_by_name)}\n"
            f"ignored_by_temporal_type: {list(prepared_data.ignored_by_temporal_type)}\n"
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

    def compare_results(self, run_id: str, comparison_contract: ComparisonContract) -> None:
        """Выполняет полный compare flow и записывает comparison_result.json по report_uri из контракта."""
        hive_contract = comparison_contract.get_engine_contract("hive")
        iceberg_contract = comparison_contract.get_engine_contract("iceberg")

        logger.info(
            f"Comparison started. run_id={run_id}, hive_result_uri={hive_contract.result_uri}, "
            f"iceberg_result_uri={iceberg_contract.result_uri}"
        )

        hive_df = self.spark.read.parquet(hive_contract.result_uri)
        iceberg_df = self.spark.read.parquet(iceberg_contract.result_uri)
        prepared_data = self.prepare_for_comparison(
            comparison_contract=comparison_contract,
            hive_df=hive_df,
            iceberg_df=iceberg_df,
        )
        metrics = self.calculate_metrics(
            hive_df=prepared_data.hive_df,
            iceberg_df=prepared_data.iceberg_df,
        )

        report = self.report_builder.build(
            run_id=run_id,
            hive_result_uri=hive_contract.result_uri,
            iceberg_result_uri=iceberg_contract.result_uri,
            metrics=metrics,
        )
        write_json_to_uri(
            spark=self.spark,
            uri=comparison_contract.report_uri,
            payload=report,
        )

        logger.info(
            f"{self.build_outcome_message(run_id=run_id, metrics=metrics, prepared_data=prepared_data)}\n"
            f"report_uri: {comparison_contract.report_uri}"
        )


if __name__ == "__main__":
    args = parse_args()
    contract = parse_job_contract(args.contract)
    with open_spark_session(args.app_name) as spark_session:
        ResultComparator(
            spark=spark_session,
            report_builder=ComparisonReportBuilder(),
        ).compare_results(
            run_id=contract.run_id,
            comparison_contract=contract.comparison,
        )
