import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
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
    StringType,
    StructField,
    StructType,
)

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


@dataclass(frozen=True, slots=True)
class ColumnNormalization:
    hive_expression: Column
    iceberg_expression: Column


@dataclass(frozen=True, slots=True)
class TemporalStringValidation:
    column_name: str
    invalid_condition: Column
    expected_format: str


class DataFrameNormalizer:
    INTEGRAL_TYPES = (ByteType, ShortType, IntegerType, LongType)
    FRACTIONAL_TYPES = (FloatType, DoubleType, DecimalType)
    NORMALIZED_DECIMAL_TYPE = "decimal(38,18)"
    TIMESTAMP_OUTPUT_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    DATE_OUTPUT_FORMAT = "yyyy-MM-dd"
    TIMESTAMP_CANONICAL_REGEX = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$"
    DATE_CANONICAL_REGEX = r"^\d{4}-\d{2}-\d{2}$"

    @staticmethod
    def get_fields_by_name(schema: StructType) -> Dict[str, StructField]:
        return {field.name: field for field in schema.fields}

    @classmethod
    def is_integral(cls, data_type) -> bool:
        return isinstance(data_type, cls.INTEGRAL_TYPES)

    @classmethod
    def is_fractional(cls, data_type) -> bool:
        return isinstance(data_type, cls.FRACTIONAL_TYPES)

    @classmethod
    def is_numeric(cls, data_type) -> bool:
        return cls.is_integral(data_type) or cls.is_fractional(data_type)

    @staticmethod
    def is_string(data_type) -> bool:
        return isinstance(data_type, StringType)

    @staticmethod
    def is_boolean(data_type) -> bool:
        return isinstance(data_type, BooleanType)

    @staticmethod
    def is_date_type(data_type) -> bool:
        return data_type.typeName() == "date"

    @staticmethod
    def is_timestamp_type(data_type) -> bool:
        return data_type.typeName() in {"timestamp", "timestamp_ntz"}

    @staticmethod
    def is_complex_type(data_type) -> bool:
        return isinstance(data_type, (ArrayType, MapType, StructType))

    @classmethod
    def normalize_timestamp_expression(cls, column: Column) -> Column:
        return f.date_format(column.cast("timestamp"), cls.TIMESTAMP_OUTPUT_FORMAT)

    @classmethod
    def normalize_date_expression(cls, column: Column) -> Column:
        return f.date_format(column.cast("date"), cls.DATE_OUTPUT_FORMAT)

    @staticmethod
    def normalize_boolean_expression(column: Column) -> Column:
        return f.lower(column.cast("string"))

    @staticmethod
    def build_canonical_string_validation(raw_column: Column, pattern: str) -> Column:
        return raw_column.isNotNull() & (~raw_column.cast("string").rlike(pattern))

    @classmethod
    def normalize_column_pair(cls, column_name: str, hive_type, iceberg_type) -> ColumnNormalization:
        hive_column = f.col(column_name)
        iceberg_column = f.col(column_name)

        if cls.is_complex_type(hive_type) or cls.is_complex_type(iceberg_type):
            raise ValueError(
                f"Complex type is not supported for comparison column={column_name}: "
                f"hive={hive_type.simpleString()}, iceberg={iceberg_type.simpleString()}"
            )

        if cls.is_timestamp_type(hive_type) and cls.is_timestamp_type(iceberg_type):
            return ColumnNormalization(
                hive_expression=cls.normalize_timestamp_expression(hive_column),
                iceberg_expression=cls.normalize_timestamp_expression(iceberg_column),
            )

        if cls.is_integral(hive_type) and cls.is_integral(iceberg_type):
            return ColumnNormalization(
                hive_expression=hive_column.cast("bigint"),
                iceberg_expression=iceberg_column.cast("bigint"),
            )

        if cls.is_numeric(hive_type) and cls.is_numeric(iceberg_type):
            return ColumnNormalization(
                hive_expression=hive_column.cast(cls.NORMALIZED_DECIMAL_TYPE),
                iceberg_expression=iceberg_column.cast(cls.NORMALIZED_DECIMAL_TYPE),
            )

        if type(hive_type) is type(iceberg_type):
            if cls.is_date_type(hive_type):
                return ColumnNormalization(
                    hive_expression=cls.normalize_date_expression(hive_column),
                    iceberg_expression=cls.normalize_date_expression(iceberg_column),
                )
            if cls.is_boolean(hive_type):
                return ColumnNormalization(
                    hive_expression=cls.normalize_boolean_expression(hive_column),
                    iceberg_expression=cls.normalize_boolean_expression(iceberg_column),
                )
            return ColumnNormalization(
                hive_expression=hive_column.cast(hive_type.simpleString()),
                iceberg_expression=iceberg_column.cast(iceberg_type.simpleString()),
            )

        if cls.is_timestamp_type(hive_type) and cls.is_string(iceberg_type):
            return ColumnNormalization(
                hive_expression=cls.normalize_timestamp_expression(hive_column),
                iceberg_expression=iceberg_column.cast("string"),
            )

        if cls.is_string(hive_type) and cls.is_timestamp_type(iceberg_type):
            return ColumnNormalization(
                hive_expression=hive_column.cast("string"),
                iceberg_expression=cls.normalize_timestamp_expression(iceberg_column),
            )

        if cls.is_date_type(hive_type) and cls.is_string(iceberg_type):
            return ColumnNormalization(
                hive_expression=cls.normalize_date_expression(hive_column),
                iceberg_expression=iceberg_column.cast("string"),
            )

        if cls.is_string(hive_type) and cls.is_date_type(iceberg_type):
            return ColumnNormalization(
                hive_expression=hive_column.cast("string"),
                iceberg_expression=cls.normalize_date_expression(iceberg_column),
            )

        raise ValueError(
            f"Unsupported type pair for comparison column={column_name}: "
            f"hive={hive_type.simpleString()}, iceberg={iceberg_type.simpleString()}"
        )

    @classmethod
    def build_temporal_string_validation(
        cls,
        column_name: str,
        engine: str,
        temporal_kind: str,
    ) -> TemporalStringValidation:
        if temporal_kind == "date":
            return TemporalStringValidation(
                column_name=column_name,
                invalid_condition=cls.build_canonical_string_validation(f.col(column_name), cls.DATE_CANONICAL_REGEX),
                expected_format=cls.DATE_OUTPUT_FORMAT,
            )
        if temporal_kind == "timestamp":
            return TemporalStringValidation(
                column_name=column_name,
                invalid_condition=cls.build_canonical_string_validation(
                    f.col(column_name),
                    cls.TIMESTAMP_CANONICAL_REGEX,
                ),
                expected_format=cls.TIMESTAMP_OUTPUT_FORMAT,
            )
        raise ValueError(
            f"Unsupported string fallback for comparison column={column_name}: "
            f"engine={engine}, temporal_kind={temporal_kind}. Only canonical date/timestamp strings are allowed."
        )

    @staticmethod
    def validate_temporal_strings(
        dataframe: DataFrame,
        engine: str,
        validations: Tuple[TemporalStringValidation, ...],
    ) -> None:
        if not validations:
            return
        projection = [
            f.max(f.when(validation.invalid_condition, 1).otherwise(0)).alias(validation.column_name)
            for validation in validations
        ]
        result = dataframe.select(*projection).first()
        invalid_columns = [
            validation.column_name
            for validation in validations
            if result and result[validation.column_name] == 1
        ]
        if invalid_columns:
            expected_formats = ", ".join(
                f"{validation.column_name}={validation.expected_format}"
                for validation in validations
                if validation.column_name in invalid_columns
            )
            raise ValueError(
                f"Found non-canonical temporal strings in comparison result. "
                f"engine={engine}, expected={expected_formats}"
            )

    def normalize(self, hive_df: DataFrame, iceberg_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        hive_fields = self.get_fields_by_name(hive_df.schema)
        iceberg_fields = self.get_fields_by_name(iceberg_df.schema)

        if set(hive_fields) != set(iceberg_fields):
            raise ValueError(
                "Hive and Iceberg query results must have identical column names: "
                f"hive={sorted(hive_fields)}, iceberg={sorted(iceberg_fields)}"
            )

        ordered_columns = list(hive_df.columns)
        hive_expressions = []
        iceberg_expressions = []
        hive_validations = []
        iceberg_validations = []

        for column_name in ordered_columns:
            hive_type = hive_fields[column_name].dataType
            iceberg_type = iceberg_fields[column_name].dataType
            normalization = self.normalize_column_pair(
                column_name=column_name,
                hive_type=hive_type,
                iceberg_type=iceberg_type,
            )

            if self.is_string(hive_type) and self.is_date_type(iceberg_type):
                hive_validations.append(
                    self.build_temporal_string_validation(column_name=column_name, engine="hive", temporal_kind="date")
                )
            if self.is_string(hive_type) and self.is_timestamp_type(iceberg_type):
                hive_validations.append(
                    self.build_temporal_string_validation(
                        column_name=column_name,
                        engine="hive",
                        temporal_kind="timestamp",
                    )
                )
            if self.is_string(iceberg_type) and self.is_date_type(hive_type):
                iceberg_validations.append(
                    self.build_temporal_string_validation(
                        column_name=column_name,
                        engine="iceberg",
                        temporal_kind="date",
                    )
                )
            if self.is_string(iceberg_type) and self.is_timestamp_type(hive_type):
                iceberg_validations.append(
                    self.build_temporal_string_validation(
                        column_name=column_name,
                        engine="iceberg",
                        temporal_kind="timestamp",
                    )
                )

            hive_expressions.append(normalization.hive_expression.alias(column_name))
            iceberg_expressions.append(normalization.iceberg_expression.alias(column_name))

        self.validate_temporal_strings(
            dataframe=hive_df,
            engine="hive",
            validations=tuple(validation for validation in hive_validations if validation is not None),
        )
        self.validate_temporal_strings(
            dataframe=iceberg_df,
            engine="iceberg",
            validations=tuple(validation for validation in iceberg_validations if validation is not None),
        )

        return hive_df.select(*hive_expressions), iceberg_df.select(*iceberg_expressions)


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
        normalizer: DataFrameNormalizer,
        report_builder: ComparisonReportBuilder,
    ) -> None:
        self.spark = spark
        self.normalizer = normalizer
        self.report_builder = report_builder

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
        normalized_hive_df, normalized_iceberg_df = self.normalizer.normalize(hive_df, iceberg_df)
        metrics = self.calculate_metrics(normalized_hive_df, normalized_iceberg_df)

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
            normalizer=DataFrameNormalizer(),
            report_builder=ComparisonReportBuilder(),
        ).execute(
            run_id=args.run_id,
            comparison_contract=parse_comparison_contract(args.contract),
        )
