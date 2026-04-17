import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StructType,
)

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
        logger.info("Spark session opened.")
        yield spark
    finally:
        spark.stop()
        logger.info("Spark session closed.")


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
class ComparisonColumnPlan:
    """Результат анализа схем: какие колонки сравниваем и какие исключили."""
    comparable_columns: list[str]
    hive_excluded_columns: list[str]
    iceberg_excluded_columns: list[str]
    temporal_excluded_columns: list[str]


@dataclass(frozen=True, slots=True)
class EngineColumnSelection:
    """Результат применения engine-specific excludes к колонкам одного result set."""
    kept_columns: list[str]
    excluded_columns: list[str]


def build_column_name_mismatch_message(hive_columns: list[str], iceberg_columns: list[str]) -> str:
    missing_in_hive = sorted(set(iceberg_columns) - set(hive_columns))
    missing_in_iceberg = sorted(set(hive_columns) - set(iceberg_columns))
    return (
        "Hive and Iceberg results must have identical comparable column names: "
        f"missing_in_hive={missing_in_hive}, missing_in_iceberg={missing_in_iceberg}"
    )


class ComparisonReportBuilder:
    """Строит финальный JSON-отчёт comparison_result.json из уже посчитанных метрик."""
    @staticmethod
    def to_checked_at() -> str:
        return datetime.now(ZoneInfo("Europe/Moscow")).replace(microsecond=0).isoformat()

    def build(
        self,
        run_id: str,
        hive_result_uri: str,
        iceberg_result_uri: str,
        metrics: ComparisonMetrics,
        column_plan: ComparisonColumnPlan,
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
            "excluded_columns": {
                "hive": column_plan.hive_excluded_columns,
                "iceberg": column_plan.iceberg_excluded_columns,
                "temporal": column_plan.temporal_excluded_columns,
            },
        }


class ComparisonColumnPlanner:
    """Строит план сравнимых колонок без создания новых Spark DataFrame-планов."""
    TEMPORAL_TYPE_NAMES = frozenset({"timestamp", "timestamp_ntz", "date"})
    TECHNICAL_COMPARE_EXCLUDES_BY_ENGINE = {
        "hive": frozenset({"date_part", "month_part", "year_part", "load_part"}),
        "iceberg": frozenset({"sys_insert_stamp", "sys_update_stamp", "src_modified_stamp", "job_insert_id"}),
    }

    def build_engine_column_selection(
        self,
        engine: str,
        column_names: list[str],
        contract_excludes: tuple[str, ...],
    ) -> EngineColumnSelection:
        """Применяет technical и contract-level excludes к колонкам одного engine result."""
        technical_excludes = self.TECHNICAL_COMPARE_EXCLUDES_BY_ENGINE.get(engine)
        if technical_excludes is None:
            raise ValueError(f"Unsupported engine={engine}")
        excluded_columns = set(technical_excludes) | set(contract_excludes)
        if not excluded_columns:
            return EngineColumnSelection(
                kept_columns=column_names,
                excluded_columns=[],
            )

        excluded_names = {column_name.lower() for column_name in excluded_columns}
        kept_columns: list[str] = []
        actual_excluded_columns: list[str] = []

        for column_name in column_names:
            if column_name.lower() in excluded_names:
                actual_excluded_columns.append(column_name)
                continue
            kept_columns.append(column_name)

        return EngineColumnSelection(
            kept_columns=kept_columns,
            excluded_columns=actual_excluded_columns,
        )

    @staticmethod
    def validate_unique_columns(engine: str, columns: list[str]) -> None:
        seen_columns: set[str] = set()
        duplicate_columns: set[str] = set()
        for column_name in columns:
            if column_name in seen_columns:
                duplicate_columns.add(column_name)
                continue
            seen_columns.add(column_name)

        duplicates = sorted(duplicate_columns)
        if duplicates:
            raise ValueError(f"{engine} result contains duplicate column names: {duplicates}")

    def build_common_columns(
        self,
        hive_columns: list[str],
        iceberg_columns: list[str],
    ) -> list[str]:
        self.validate_unique_columns("Hive", hive_columns)
        self.validate_unique_columns("Iceberg", iceberg_columns)
        if set(hive_columns) != set(iceberg_columns):
            raise ValueError(build_column_name_mismatch_message(hive_columns, iceberg_columns))
        return sorted(hive_columns)

    def find_temporal_columns(
        self,
        column_names: list[str],
        hive_df: DataFrame,
        iceberg_df: DataFrame,
    ) -> list[str]:
        """Находит колонки, которые нельзя сравнивать из-за temporal-типа хотя бы с одной стороны."""
        hive_types = {field.name: field.dataType.typeName() for field in hive_df.schema.fields}
        iceberg_types = {field.name: field.dataType.typeName() for field in iceberg_df.schema.fields}
        temporal_column_names: list[str] = []

        for column_name in column_names:
            hive_type = hive_types[column_name]
            iceberg_type = iceberg_types[column_name]
            if hive_type in self.TEMPORAL_TYPE_NAMES or iceberg_type in self.TEMPORAL_TYPE_NAMES:
                temporal_column_names.append(column_name)

        return sorted(temporal_column_names)

    def build(
        self,
        comparison_contract: ComparisonContract,
        hive_df: DataFrame,
        iceberg_df: DataFrame,
    ) -> ComparisonColumnPlan:
        """Строит список сравнимых колонок по схемам, не создавая новых DataFrame-планов."""
        hive_contract = comparison_contract.get_engine_contract("hive")
        iceberg_contract = comparison_contract.get_engine_contract("iceberg")

        hive_selection = self.build_engine_column_selection(
            engine="hive",
            column_names=hive_df.columns,
            contract_excludes=hive_contract.excluded_columns,
        )
        iceberg_selection = self.build_engine_column_selection(
            engine="iceberg",
            column_names=iceberg_df.columns,
            contract_excludes=iceberg_contract.excluded_columns,
        )
        common_columns = self.build_common_columns(
            hive_columns=hive_selection.kept_columns,
            iceberg_columns=iceberg_selection.kept_columns,
        )

        temporal_excluded_columns = self.find_temporal_columns(
            column_names=common_columns,
            hive_df=hive_df,
            iceberg_df=iceberg_df,
        )
        temporal_excluded_names = set(temporal_excluded_columns)
        comparable_columns = sorted(
            column_name for column_name in common_columns if column_name not in temporal_excluded_names
        )
        return ComparisonColumnPlan(
            comparable_columns=comparable_columns,
            hive_excluded_columns=hive_selection.excluded_columns,
            iceberg_excluded_columns=iceberg_selection.excluded_columns,
            temporal_excluded_columns=temporal_excluded_columns,
        )


class ComparisonDataFrameNormalizer:
    """Выбирает сравнимые колонки и нормализует primitive-типы в одном Spark select."""
    NORMALIZED_INTEGER_TYPE = "bigint"
    NORMALIZED_DECIMAL_TYPE = "decimal(38,18)"

    def build_normalized_expression(self, schema_field):
        column = f.col(schema_field.name)
        data_type = schema_field.dataType

        if isinstance(data_type, (ArrayType, MapType, StructType)):
            raise ValueError(
                f"Complex type is not supported for comparison: "
                f"column={schema_field.name}, type={data_type.simpleString()}"
            )

        if isinstance(data_type, (ByteType, ShortType, IntegerType, LongType)):
            return column.cast(self.NORMALIZED_INTEGER_TYPE).alias(schema_field.name)

        if isinstance(data_type, (FloatType, DoubleType, DecimalType)):
            return column.cast(self.NORMALIZED_DECIMAL_TYPE).alias(schema_field.name)

        if isinstance(data_type, BooleanType):
            return f.lower(column.cast("string")).alias(schema_field.name)

        return column

    def select_columns(self, df: DataFrame, column_names: list[str]) -> DataFrame:
        """Одним select применяет финальный список колонок и normalization expressions."""
        fields_by_column = {field.name: field for field in df.schema.fields}
        expressions = []
        for column_name in column_names:
            schema_field = fields_by_column.get(column_name)
            if schema_field is None:
                raise ValueError(f"Comparable column is absent in result schema: {column_name}")
            expressions.append(self.build_normalized_expression(schema_field))

        return df.select(*expressions)

    @staticmethod
    def validate_schemas(hive_df: DataFrame, iceberg_df: DataFrame) -> None:
        """Проверяет, что после всех исключений и нормализации обе стороны сравниваются по одинаковой схеме."""
        if set(hive_df.columns) != set(iceberg_df.columns):
            raise ValueError(build_column_name_mismatch_message(hive_df.columns, iceberg_df.columns))
        hive_types = {field.name: field.dataType.simpleString() for field in hive_df.schema.fields}
        iceberg_types = {field.name: field.dataType.simpleString() for field in iceberg_df.schema.fields}
        mismatched = {
            col: (hive_types[col], iceberg_types[col]) for col in hive_types if hive_types[col] != iceberg_types[col]
        }
        if mismatched:
            raise ValueError(f"Schema mismatch after normalization and temporal exclusion: {mismatched}")


class ComparisonMetricsCalculator:
    """Считает comparison metrics поверх уже подготовленных DataFrame."""
    @staticmethod
    def calculate_ratio(exclusive_count: int, total_count: int) -> float:
        if total_count == 0:
            return 0.0
        return round(exclusive_count / total_count, 6)

    def calculate(self, hive_df: DataFrame, iceberg_df: DataFrame) -> ComparisonMetrics:
        """Считает row counts и двусторонние exclusive counts через persist + bilateral exceptAll."""
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
                    hive=self.calculate_ratio(hive_exclusive_row_count, hive_row_count),
                    iceberg=self.calculate_ratio(iceberg_exclusive_row_count, iceberg_row_count),
                ),
            )
        finally:
            hive_df.unpersist()
            iceberg_df.unpersist()


class ResultComparator:
    """Оркестрирует сверку: чтение parquet, подготовку колонок, расчёт метрик и запись JSON report."""

    def __init__(
        self,
        spark: SparkSession,
        report_builder: ComparisonReportBuilder,
    ) -> None:
        self.spark = spark
        self.report_builder = report_builder
        self.column_planner = ComparisonColumnPlanner()
        self.normalizer = ComparisonDataFrameNormalizer()
        self.metrics_calculator = ComparisonMetricsCalculator()

    def compare_results(self, run_id: str, comparison_contract: ComparisonContract) -> None:
        """Выполняет полный compare flow и записывает comparison_result.json по report_uri из контракта."""
        hive_contract = comparison_contract.get_engine_contract("hive")
        iceberg_contract = comparison_contract.get_engine_contract("iceberg")

        logger.info(
            f"Comparison started: run_id={run_id}, hive_result_uri={hive_contract.result_uri}, "
            f"iceberg_result_uri={iceberg_contract.result_uri}"
        )

        hive_df = self.spark.read.parquet(hive_contract.result_uri)
        iceberg_df = self.spark.read.parquet(iceberg_contract.result_uri)
        column_plan = self.column_planner.build(
            comparison_contract=comparison_contract,
            hive_df=hive_df,
            iceberg_df=iceberg_df,
        )
        comparable_hive_df = self.normalizer.select_columns(
            df=hive_df,
            column_names=column_plan.comparable_columns,
        )
        comparable_iceberg_df = self.normalizer.select_columns(
            df=iceberg_df,
            column_names=column_plan.comparable_columns,
        )
        self.normalizer.validate_schemas(
            hive_df=comparable_hive_df,
            iceberg_df=comparable_iceberg_df,
        )
        metrics = self.metrics_calculator.calculate(
            hive_df=comparable_hive_df,
            iceberg_df=comparable_iceberg_df,
        )

        report = self.report_builder.build(
            run_id=run_id,
            hive_result_uri=hive_contract.result_uri,
            iceberg_result_uri=iceberg_contract.result_uri,
            metrics=metrics,
            column_plan=column_plan,
        )
        write_json_to_uri(
            spark=self.spark,
            uri=comparison_contract.report_uri,
            payload=report,
        )

        logger.info(
            f"Comparison completed: run_id={run_id}, status={metrics.status()}, "
            f"report_uri={comparison_contract.report_uri}, "
            f"hive_row_count={metrics.row_count.hive}, iceberg_row_count={metrics.row_count.iceberg}, "
            f"row_count_delta={metrics.row_count_delta}, "
            f"hive_exclusive_row_count={metrics.exclusive_row_count.hive}, "
            f"iceberg_exclusive_row_count={metrics.exclusive_row_count.iceberg}, "
            f"hive_exclusive_row_ratio={metrics.exclusive_row_ratio.hive}, "
            f"iceberg_exclusive_row_ratio={metrics.exclusive_row_ratio.iceberg}, "
            f"hive_excluded_columns={column_plan.hive_excluded_columns}, "
            f"iceberg_excluded_columns={column_plan.iceberg_excluded_columns}, "
            f"temporal_excluded_columns={column_plan.temporal_excluded_columns}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DataGen: compare Hive and Iceberg query results")
    parser.add_argument("--app_name", required=True)
    parser.add_argument("--contract", required=True)
    return parser.parse_args()


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
