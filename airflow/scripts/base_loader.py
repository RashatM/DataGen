from abc import ABC, abstractmethod
from contextlib import contextmanager
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
from typing import ClassVar

from job_common import ComparisonContract, LoaderTableContract, logger, read_text_from_uri


class ComparisonDataNormalizer:
    """Приводит сравнимые primitive-типы к устойчивому виду перед parquet-материализацией comparison result."""
    NORMALIZED_INTEGER_TYPE = "bigint"
    NORMALIZED_DECIMAL_TYPE = "decimal(38,18)"

    @staticmethod
    def split_columns_for_materialization(
        df: DataFrame,
        ignored_columns: set[str],
    ) -> tuple[list[str], list[str]]:
        """Отделяет сравнимые колонки от явно исключённых до записи parquet-результата запроса."""
        selected_columns: list[str] = []
        ignored_by_name: list[str] = []
        ignored_names = {name.lower() for name in ignored_columns}

        for schema_field in df.schema.fields:
            column_name = schema_field.name
            if column_name.lower() in ignored_names:
                ignored_by_name.append(column_name)
                continue
            selected_columns.append(column_name)

        return selected_columns, ignored_by_name

    def normalize(self, df: DataFrame) -> DataFrame:
        """Нормализует primitive-типы так, чтобы дальнейшая сверка не зависела от мелких различий представления."""
        expressions = []
        for schema_field in df.schema.fields:
            col = f.col(schema_field.name)
            dt = schema_field.dataType

            if isinstance(dt, (ArrayType, MapType, StructType)):
                raise ValueError(
                    f"Complex type is not supported for comparison: "
                    f"column={schema_field.name}, type={dt.simpleString()}"
                )

            if isinstance(dt, (ByteType, ShortType, IntegerType, LongType)):
                expressions.append(col.cast(self.NORMALIZED_INTEGER_TYPE).alias(schema_field.name))
            elif isinstance(dt, (FloatType, DoubleType, DecimalType)):
                expressions.append(col.cast(self.NORMALIZED_DECIMAL_TYPE).alias(schema_field.name))
            elif isinstance(dt, BooleanType):
                expressions.append(f.lower(col.cast("string")).alias(schema_field.name))
            else:
                expressions.append(col)

        return df.select(*expressions)


class ComparisonResultMaterializer:
    """Выполняет comparison query и сохраняет parquet-результат для последующей сверки."""

    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        normalizer: ComparisonDataNormalizer,
        technical_excludes: frozenset[str],
    ) -> None:
        self.spark = spark
        self.run_id = run_id
        self.normalizer = normalizer
        self.technical_excludes = technical_excludes

    def read_query_from_uri(self, query_uri: str) -> str:
        return read_text_from_uri(self.spark, query_uri)

    def build_ignored_columns(self, extra_columns: tuple[str, ...]) -> set[str]:
        """Объединяет технические engine-specific исключения с exclude_columns из runtime-контракта."""
        return set(self.technical_excludes) | set(extra_columns)

    def materialize(
        self,
        comparison_contract: ComparisonContract,
        engine: str,
    ) -> None:
        engine_contract = comparison_contract.get_engine_contract(engine)
        ignored_columns = self.build_ignored_columns(extra_columns=engine_contract.exclude_columns)
        logger.info(
            f"Comparison query execution started. engine={engine}, run_id={self.run_id}, "
            f"query_uri={engine_contract.query_uri}, result_uri={engine_contract.result_uri}"
        )
        comparison_query = self.read_query_from_uri(engine_contract.query_uri)
        comparison_df = self.spark.sql(comparison_query)
        selected_columns, ignored_by_name = self.normalizer.split_columns_for_materialization(
            df=comparison_df,
            ignored_columns=ignored_columns,
        )
        logger.info(
            f"Comparison result columns filtered. engine={engine}, run_id={self.run_id}, "
            f"kept={selected_columns}, ignored_by_name={ignored_by_name}"
        )
        filtered_df = comparison_df.select(*(f.col(column_name) for column_name in selected_columns))
        normalized_df = self.normalizer.normalize(filtered_df)
        self.write_result(
            df=normalized_df,
            result_uri=engine_contract.result_uri,
            engine=engine,
        )

        logger.info(
            f"Comparison query execution completed. engine={engine}, run_id={self.run_id}, "
            f"result_uri={engine_contract.result_uri}"
        )

    @staticmethod
    def write_result(df: DataFrame, result_uri: str, engine: str) -> None:
        try:
            df.write.mode("overwrite").parquet(result_uri)
        except Exception as error:
            raise RuntimeError(
                f"Failed to write comparison parquet. engine={engine}, "
                f"result_uri={result_uri}"
            ) from error


class BaseSynthLoader(ABC):
    """Общая orchestration-логика загрузчика parquet-артефактов в существующие target tables.

    Базовый класс не владеет DDL таблиц и не создаёт временные таблицы. Его зона ответственности:
    - прочитать parquet только с нужной projection
    - получить реальную схему целевой таблицы
    - выровнять DataFrame под target schema с fail-fast cast semantics
    - выбрать нужную write strategy по write_mode
    - материализовать comparison result для своего движка
    """

    technical_compare_excludes: ClassVar[frozenset[str]] = frozenset()

    def __init__(self, spark: SparkSession, run_id: str) -> None:
        self.run_id = run_id
        self.spark = spark
        self.comparison_data_normalizer = ComparisonDataNormalizer()
        self.comparison_result_materializer = ComparisonResultMaterializer(
            spark=self.spark,
            run_id=self.run_id,
            normalizer=self.comparison_data_normalizer,
            technical_excludes=self.technical_compare_excludes,
        )

    @abstractmethod
    def append_to_table(self, df: DataFrame, table_name: str) -> None:
        pass

    @abstractmethod
    def overwrite_table(self, df: DataFrame, table_name: str) -> None:
        pass

    @abstractmethod
    def overwrite_partitions(self, df: DataFrame, table_name: str) -> None:
        pass

    def is_partitioned_table(self, table_name: str) -> bool:
        """Проверяет partitioning по DESCRIBE без чтения самих partition columns."""
        partition_count = (
            self.spark.sql(f"DESCRIBE {table_name}")
            .filter(
                (f.col("col_name") == "# Partitioning") |
                (f.col("col_name") == "# Partition Information")
            )
            .limit(1)
            .count()
        )
        return partition_count > 0

    @contextmanager
    def strict_cast_mode(self):
        """Временно включает ANSI-режим Spark, чтобы невалидные cast-ы падали ошибкой, а не превращались в NULL."""
        previous_ansi = self.spark.conf.get("spark.sql.ansi.enabled", "false")
        previous_assignment_policy = self.spark.conf.get("spark.sql.storeAssignmentPolicy", "ANSI")
        self.spark.conf.set("spark.sql.ansi.enabled", "true")
        self.spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")
        try:
            yield
        finally:
            self.spark.conf.set("spark.sql.ansi.enabled", previous_ansi)
            self.spark.conf.set("spark.sql.storeAssignmentPolicy", previous_assignment_policy)

    def table_exists(self, table_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(table_name))

    def load_target_schema(self, table_name: str) -> StructType:
        """Читает реальную схему уже существующей целевой таблицы движка."""
        if not self.table_exists(table_name):
            raise RuntimeError(f"Target table does not exist: {table_name}")
        return self.spark.table(table_name).schema

    def ensure_partitioned_table(self, table: LoaderTableContract) -> None:
        """Проверяет только факт partitioning, не протаскивая partition columns в общий load flow."""
        if not self.is_partitioned_table(table.target_table_name):
            raise RuntimeError(
                f"write_mode={table.write_mode} requires a partitioned target table: {table.target_table_name}"
            )

    @staticmethod
    def project_load_dataframe(df: DataFrame, table: LoaderTableContract) -> DataFrame:
        available_columns = set(df.columns)
        missing_columns = [column_name for column_name in table.columns if column_name not in available_columns]
        if missing_columns:
            missing_columns_text = ", ".join(missing_columns)
            raise RuntimeError(
                f"Parquet is missing projected columns for table={table.target_table_name}: "
                f"{missing_columns_text}"
            )

        return df.select(*table.columns)

    @staticmethod
    def align_to_target_schema(
        df: DataFrame,
        table: LoaderTableContract,
        target_schema: StructType,
    ) -> DataFrame:
        """Приводит projection parquet к порядку и типам целевой схемы, заполняя только nullable-пропуски через NULL."""
        target_columns = {field.name for field in target_schema.fields}
        source_columns = set(df.columns)

        unexpected_columns = sorted(source_columns - target_columns)
        if unexpected_columns:
            unexpected_columns_text = ", ".join(unexpected_columns)
            raise RuntimeError(
                f"Projected parquet contains columns absent in target table={table.target_table_name}: "
                f"{unexpected_columns_text}"
            )

        expressions = []
        for field in target_schema.fields:
            if field.name in source_columns:
                expressions.append(f.col(field.name).cast(field.dataType).alias(field.name))
                continue

            if field.nullable:
                expressions.append(f.lit(None).cast(field.dataType).alias(field.name))
                continue

            raise RuntimeError(
                f"Parquet is missing non-nullable target column for table={table.target_table_name}: {field.name}"
            )

        return df.select(*expressions)

    def write_table(
        self,
        df: DataFrame,
        table: LoaderTableContract,
    ) -> None:
        """Маршрутизирует уже выровненный DataFrame в engine-specific стратегию записи по write_mode."""
        if table.write_mode == "APPEND":
            self.append_to_table(df, table.target_table_name)
            return

        if table.write_mode == "OVERWRITE_TABLE":
            self.overwrite_table(df, table.target_table_name)
            return

        if table.write_mode == "OVERWRITE_PARTITIONS":
            self.ensure_partitioned_table(table)
            self.overwrite_partitions(df, table.target_table_name)
            return

        raise RuntimeError(f"Unsupported write_mode={table.write_mode} for table={table.target_table_name}")

    def load_table(self, table: LoaderTableContract) -> None:
        logger.info(
            f"Table load started. table={table.target_table_name}, run_id={self.run_id}, "
            f"write_mode={table.write_mode}"
        )
        source_df = self.spark.read.parquet(table.data_uri)
        projected_df = self.project_load_dataframe(df=source_df, table=table)
        target_schema = self.load_target_schema(table_name=table.target_table_name)

        with self.strict_cast_mode():
            aligned_df = self.align_to_target_schema(
                df=projected_df,
                table=table,
                target_schema=target_schema,
            )
            self.write_table(
                df=aligned_df,
                table=table,
            )
        logger.info(f"Table load completed. table={table.target_table_name}, run_id={self.run_id}")

    def publish_tables(self, tables: list[LoaderTableContract]) -> None:
        logger.info(f"Table load batch started. run_id={self.run_id}, tables_count={len(tables)}")
        for table in tables:
            self.load_table(table)
        logger.info(f"Table load batch completed. run_id={self.run_id}, tables_count={len(tables)}")

    def materialize_comparison_result(self, comparison_contract: ComparisonContract, engine: str) -> None:
        self.comparison_result_materializer.materialize(
            comparison_contract=comparison_contract,
            engine=engine,
        )
