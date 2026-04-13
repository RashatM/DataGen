import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

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
class PreparedComparison:
    """Промежуточный результат подготовки двух parquet-результатов к сверке."""
    hive_df: DataFrame
    iceberg_df: DataFrame
    hive_excluded_by_name: tuple[str, ...]
    iceberg_excluded_by_name: tuple[str, ...]
    excluded_by_temporal_type: tuple[str, ...]


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

    Компаратор работает не напрямую с SQL, а с raw parquet-результатами запросов.
    Его этапы такие:
    - применить technical и contract-level excludes по именам
    - исключить колонки, если хотя бы с одной стороны Spark считает тип temporal
    - нормализовать primitive-типы оставшихся колонок
    - проверить совпадение сравнимых колонок и их схем после нормализации
    - посчитать двусторонние exceptAll-метрики
    - записать итоговый report обратно в object storage
    """
    TEMPORAL_TYPE_NAMES = frozenset({"timestamp", "timestamp_ntz", "date"})
    NORMALIZED_INTEGER_TYPE = "bigint"
    NORMALIZED_DECIMAL_TYPE = "decimal(38,18)"
    TECHNICAL_COMPARE_EXCLUDES_BY_ENGINE = {
        "hive": frozenset({"date_part", "month_part", "year_part", "load_part"}),
        "iceberg": frozenset({"sys_insert_stamp", "sys_update_stamp", "src_modified_stamp", "job_insert_id"}),
    }

    def __init__(
        self,
        spark: SparkSession,
        report_builder: ComparisonReportBuilder,
    ) -> None:
        self.spark = spark
        self.report_builder = report_builder

    def build_excluded_columns(self, engine: str, contract_excludes: tuple[str, ...]) -> set[str]:
        """Объединяет technical excludes движка и exclude_columns из runtime-контракта."""
        technical_excludes = self.TECHNICAL_COMPARE_EXCLUDES_BY_ENGINE.get(engine)
        if technical_excludes is None:
            raise ValueError(f"Unsupported engine={engine}")
        return set(technical_excludes) | set(contract_excludes)

    @staticmethod
    def drop_excluded_columns_by_name(
        df: DataFrame,
        excluded_columns: set[str],
    ) -> tuple[DataFrame, tuple[str, ...]]:
        """Удаляет из сверки technical и contract-level exclude columns по имени."""
        if not excluded_columns:
            return df, ()

        excluded_names = {column_name.lower() for column_name in excluded_columns}
        kept_columns: list[str] = []
        excluded_by_name: list[str] = []

        for column_name in df.columns:
            if column_name.lower() in excluded_names:
                excluded_by_name.append(column_name)
                continue
            kept_columns.append(column_name)

        if not excluded_by_name:
            return df, ()

        return df.select(*(f.col(column_name) for column_name in kept_columns)), tuple(excluded_by_name)

    @staticmethod
    def validate_column_sets(hive_df: DataFrame, iceberg_df: DataFrame) -> None:
        if set(hive_df.columns) != set(iceberg_df.columns):
            raise ValueError(
                "Hive and Iceberg results must have identical comparable column names: "
                f"hive={sorted(hive_df.columns)}, iceberg={sorted(iceberg_df.columns)}"
            )

    def drop_temporal_columns(
        self,
        hive_df: DataFrame,
        iceberg_df: DataFrame,
    ) -> tuple[DataFrame, DataFrame, tuple[str, ...]]:
        """Исключает из сверки колонки, если хотя бы на одной стороне Spark считает их temporal-типом."""
        self.validate_column_sets(hive_df, iceberg_df)

        hive_types = {field.name: field.dataType.typeName() for field in hive_df.schema.fields}
        iceberg_types = {field.name: field.dataType.typeName() for field in iceberg_df.schema.fields}
        temporal_column_names: list[str] = []

        for column_name in hive_types:
            hive_type = hive_types[column_name]
            iceberg_type = iceberg_types[column_name]
            if hive_type in self.TEMPORAL_TYPE_NAMES or iceberg_type in self.TEMPORAL_TYPE_NAMES:
                temporal_column_names.append(column_name)

        excluded_by_temporal_type = tuple(sorted(temporal_column_names))
        if not excluded_by_temporal_type:
            return hive_df, iceberg_df, excluded_by_temporal_type

        excluded_temporal_names = set(excluded_by_temporal_type)
        kept_columns = [
            column_name
            for column_name in hive_df.columns
            if column_name not in excluded_temporal_names
        ]
        return (
            hive_df.select(*(f.col(column_name) for column_name in kept_columns)),
            iceberg_df.select(*(f.col(column_name) for column_name in kept_columns)),
            excluded_by_temporal_type,
        )

    def normalize_comparable_types(self, df: DataFrame) -> DataFrame:
        """Нормализует primitive-типы сравнимых колонок перед schema validation и exceptAll."""
        expressions = []
        for schema_field in df.schema.fields:
            column = f.col(schema_field.name)
            data_type = schema_field.dataType

            if isinstance(data_type, (ArrayType, MapType, StructType)):
                raise ValueError(
                    f"Complex type is not supported for comparison: "
                    f"column={schema_field.name}, type={data_type.simpleString()}"
                )

            if isinstance(data_type, (ByteType, ShortType, IntegerType, LongType)):
                expressions.append(column.cast(self.NORMALIZED_INTEGER_TYPE).alias(schema_field.name))
                continue

            if isinstance(data_type, (FloatType, DoubleType, DecimalType)):
                expressions.append(column.cast(self.NORMALIZED_DECIMAL_TYPE).alias(schema_field.name))
                continue

            if isinstance(data_type, BooleanType):
                expressions.append(f.lower(column.cast("string")).alias(schema_field.name))
                continue

            expressions.append(column)

        return df.select(*expressions)

    def validate_schemas(self, hive_df: DataFrame, iceberg_df: DataFrame) -> None:
        """Проверяет, что после всех исключений и нормализации обе стороны сравниваются по одинаковой схеме."""
        self.validate_column_sets(hive_df, iceberg_df)
        hive_types = {field.name: field.dataType.simpleString() for field in hive_df.schema.fields}
        iceberg_types = {field.name: field.dataType.simpleString() for field in iceberg_df.schema.fields}
        mismatched = {
            col: (hive_types[col], iceberg_types[col]) for col in hive_types if hive_types[col] != iceberg_types[col]
        }
        if mismatched:
            raise ValueError(f"Schema mismatch after normalization and temporal exclusion: {mismatched}")

    def prepare_for_comparison(
        self,
        comparison_contract: ComparisonContract,
        hive_df: DataFrame,
        iceberg_df: DataFrame,
    ) -> PreparedComparison:
        """Готовит обе стороны к сверке: применяет excludes, temporal-policy и schema validation."""
        hive_contract = comparison_contract.get_engine_contract("hive")
        iceberg_contract = comparison_contract.get_engine_contract("iceberg")

        hive_excluded_columns = self.build_excluded_columns(
            engine="hive",
            contract_excludes=hive_contract.excluded_columns,
        )
        iceberg_excluded_columns = self.build_excluded_columns(
            engine="iceberg",
            contract_excludes=iceberg_contract.excluded_columns,
        )
        filtered_hive_df, hive_excluded_by_name = self.drop_excluded_columns_by_name(
            df=hive_df,
            excluded_columns=hive_excluded_columns,
        )
        filtered_iceberg_df, iceberg_excluded_by_name = self.drop_excluded_columns_by_name(
            df=iceberg_df,
            excluded_columns=iceberg_excluded_columns,
        )
        non_temporal_hive_df, non_temporal_iceberg_df, excluded_by_temporal_type = self.drop_temporal_columns(
            hive_df=filtered_hive_df,
            iceberg_df=filtered_iceberg_df,
        )
        normalized_hive_df = self.normalize_comparable_types(non_temporal_hive_df)
        normalized_iceberg_df = self.normalize_comparable_types(non_temporal_iceberg_df)
        self.validate_schemas(
            hive_df=normalized_hive_df,
            iceberg_df=normalized_iceberg_df,
        )

        return PreparedComparison(
            hive_df=normalized_hive_df,
            iceberg_df=normalized_iceberg_df,
            hive_excluded_by_name=hive_excluded_by_name,
            iceberg_excluded_by_name=iceberg_excluded_by_name,
            excluded_by_temporal_type=excluded_by_temporal_type,
        )

    def calculate_metrics(self, hive_df: DataFrame, iceberg_df: DataFrame) -> ComparisonMetrics:
        """Считает row counts и двусторонние exclusive counts через persist + bilateral exceptAll."""
        ordered_columns = sorted(hive_df.columns)
        hive_df = hive_df.select(*ordered_columns)
        iceberg_df = iceberg_df.select(*ordered_columns)
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
                    hive=self.report_builder.calculate_ratio(hive_exclusive_row_count, hive_row_count),
                    iceberg=self.report_builder.calculate_ratio(iceberg_exclusive_row_count, iceberg_row_count),
                ),
            )
        finally:
            hive_df.unpersist()
            iceberg_df.unpersist()

    @staticmethod
    def build_outcome_message(
        run_id: str,
        metrics: ComparisonMetrics,
        prepared_data: PreparedComparison,
    ) -> str:
        status_line = "Comparison passed." if metrics.status() == "MATCH" else "Comparison mismatch detected."
        return (
            f"{status_line}\n"
            f"run_id: {run_id}\n"
            f"excluded_by_name:\n"
            f"  hive: {list(prepared_data.hive_excluded_by_name)}\n"
            f"  iceberg: {list(prepared_data.iceberg_excluded_by_name)}\n"
            f"excluded_by_temporal_type: {list(prepared_data.excluded_by_temporal_type)}\n"
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
            f"Comparison started: run_id={run_id}, hive_result_uri={hive_contract.result_uri}, "
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
