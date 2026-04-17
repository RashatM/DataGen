from abc import ABC, abstractmethod
from contextlib import contextmanager
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from job_common import ComparisonContract, LoaderTableContract, logger, read_text_from_uri


class ComparisonResultMaterializer:
    """Выполняет comparison query и сохраняет parquet-результат без compare-фильтров."""

    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
    ) -> None:
        self.spark = spark
        self.run_id = run_id

    def read_query_from_uri(self, query_uri: str) -> str:
        return read_text_from_uri(self.spark, query_uri)

    def materialize(
        self,
        comparison_contract: ComparisonContract,
        engine: str,
    ) -> None:
        engine_contract = comparison_contract.get_engine_contract(engine)
        logger.info(
            f"Comparison query execution started: engine={engine}, run_id={self.run_id}, "
            f"query_uri={engine_contract.query_uri}, result_uri={engine_contract.result_uri}"
        )

        comparison_query = self.read_query_from_uri(engine_contract.query_uri)
        comparison_df = self.spark.sql(comparison_query)
        self.write_result(
            df=comparison_df,
            result_uri=engine_contract.result_uri,
            engine=engine,
        )

        logger.info(
            f"Comparison query execution completed: engine={engine}, run_id={self.run_id}, "
            f"result_uri={engine_contract.result_uri}"
        )

    @staticmethod
    def write_result(df: DataFrame, result_uri: str, engine: str) -> None:
        try:
            df.write.mode("overwrite").parquet(result_uri)
        except Exception as error:
            raise RuntimeError(
                f"Failed to write comparison parquet: engine={engine}, "
                f"result_uri={result_uri}"
            ) from error


class BaseSynthLoader(ABC):
    """Общая orchestration-логика загрузчика parquet-артефактов в существующие target tables.

    Базовый класс не владеет DDL таблиц и не создаёт временные таблицы. Его зона ответственности:
    - прочитать parquet только с колонками из контракта загрузки
    - получить реальную схему целевой таблицы
    - выровнять DataFrame под target schema с fail-fast cast semantics
    - выбрать нужную write strategy по write_mode
    - материализовать raw comparison query result для своего движка
    """

    TECHNICAL_FILLER_VALUES_BY_TYPE = {
        ByteType: 1,
        ShortType: 1,
        IntegerType: 1,
        LongType: 1,
        FloatType: 0.1,
        DoubleType: 0.1,
        DecimalType: "0.1",
        StringType: "N/A",
        BooleanType: False,
        DateType: "2040-01-01",
        TimestampType: "2040-01-01 23:59:59",
        TimestampNTZType: "2040-01-01 23:59:59",
    }

    def __init__(self, spark: SparkSession, run_id: str) -> None:
        self.run_id = run_id
        self.spark = spark
        self.comparison_result_materializer = ComparisonResultMaterializer(
            spark=self.spark,
            run_id=self.run_id,
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

    def ensure_partitioned_table(self, table_contract: LoaderTableContract) -> None:
        """Проверяет только факт partitioning, не протаскивая partition columns в общий load flow."""
        if not self.is_partitioned_table(table_contract.target_table_name):
            raise RuntimeError(
                f"write_mode={table_contract.write_mode} requires a partitioned target table: "
                f"{table_contract.target_table_name}"
            )

    @staticmethod
    def build_case_insensitive_column_map(columns: list[str]) -> dict[str, str]:
        columns_by_key: dict[str, str] = {}
        for column_name in columns:
            column_key = column_name.lower()
            existing_column = columns_by_key.get(column_key)
            if existing_column is None:
                columns_by_key[column_key] = column_name
                continue

            raise RuntimeError(
                f"Ambiguous columns differing only by case: {existing_column}, {column_name}"
            )
        return columns_by_key

    def select_load_columns(self, source_df: DataFrame, table_contract: LoaderTableContract) -> DataFrame:
        """Проверяет, что parquet содержит колонки контракта загрузки, и выбирает их в заданном порядке."""
        load_columns = table_contract.columns
        available_columns_by_key = self.build_case_insensitive_column_map(source_df.columns)
        missing_columns = [
            column_name for column_name in load_columns if column_name.lower() not in available_columns_by_key
        ]
        if missing_columns:
            missing_columns_text = ", ".join(missing_columns)
            raise RuntimeError(
                f"Parquet is missing load contract columns for table={table_contract.target_table_name}: "
                f"{missing_columns_text}"
            )

        expressions = [
            f.col(available_columns_by_key[column_name.lower()]).alias(column_name) for column_name in load_columns
        ]
        return source_df.select(*expressions)

    def technical_filler_expression(self, field: StructField, table_name: str):
        """Строит техническое значение для отсутствующей non-nullable target-колонки."""
        data_type = field.dataType
        data_type_class = type(data_type)

        if data_type_class in self.TECHNICAL_FILLER_VALUES_BY_TYPE:
            filler_value = self.TECHNICAL_FILLER_VALUES_BY_TYPE[data_type_class]
            return f.lit(filler_value).cast(data_type).alias(field.name)

        raise RuntimeError(
            f"Parquet is missing non-nullable target column for table={table_name}: "
            f"{field.name}. Technical filler is not supported for type={self.format_type(data_type)}"
        )

    @staticmethod
    def format_type(data_type: DataType) -> str:
        return data_type.simpleString()

    def align_to_target_schema(
        self,
        load_df: DataFrame,
        table_contract: LoaderTableContract,
        target_schema: StructType,
    ) -> DataFrame:
        """Приводит DataFrame к target schema.

        Отсутствующие nullable-колонки заполняются NULL.
        Отсутствующие non-nullable-колонки заполняются техническими filler-значениями по target-типу.
        """
        target_columns_by_key = self.build_case_insensitive_column_map(
            [field.name for field in target_schema.fields],
        )
        source_columns_by_key = self.build_case_insensitive_column_map(load_df.columns)

        unexpected_columns = sorted(
            source_columns_by_key[column_key]
            for column_key in source_columns_by_key
            if column_key not in target_columns_by_key
        )
        if unexpected_columns:
            unexpected_columns_text = ", ".join(unexpected_columns)
            raise RuntimeError(
                f"Load contract contains columns absent in target table={table_contract.target_table_name}: "
                f"{unexpected_columns_text}"
            )

        expressions = []
        technical_filler_columns = []
        normalized_columns = []
        for field in target_schema.fields:
            field_key = field.name.lower()
            source_column_name = source_columns_by_key.get(field_key)
            if source_column_name is not None:
                expressions.append(f.col(source_column_name).cast(field.dataType).alias(field.name))
                if source_column_name != field.name:
                    normalized_columns.append(f"{source_column_name}->{field.name}")
                continue

            if field.nullable:
                expressions.append(f.lit(None).cast(field.dataType).alias(field.name))
                continue

            expressions.append(
                self.technical_filler_expression(
                    field=field,
                    table_name=table_contract.target_table_name,
                )
            )
            technical_filler_columns.append(
                f"{field.name}:{self.format_type(field.dataType)}"
            )

        if technical_filler_columns:
            technical_filler_columns_text = ", ".join(technical_filler_columns)
            logger.info(
                f"Filled missing non-nullable target columns with technical values: "
                f"table={table_contract.target_table_name}, columns={technical_filler_columns_text}"
            )

        return load_df.select(*expressions)

    def write_table(
        self,
        df: DataFrame,
        table_contract: LoaderTableContract,
    ) -> None:
        """Маршрутизирует уже выровненный DataFrame в engine-specific стратегию записи по write_mode."""
        if table_contract.write_mode == "APPEND":
            self.append_to_table(df, table_contract.target_table_name)
            return

        if table_contract.write_mode == "OVERWRITE_TABLE":
            self.overwrite_table(df, table_contract.target_table_name)
            return

        if table_contract.write_mode == "OVERWRITE_PARTITIONS":
            self.ensure_partitioned_table(table_contract)
            self.overwrite_partitions(df, table_contract.target_table_name)
            return

        raise RuntimeError(
            f"Unsupported write_mode={table_contract.write_mode} for table={table_contract.target_table_name}"
        )

    def load_table(self, table_contract: LoaderTableContract) -> None:
        logger.info(
            f"Table load started: table={table_contract.target_table_name}, run_id={self.run_id}, "
            f"write_mode={table_contract.write_mode}"
        )
        if not self.table_exists(table_contract.target_table_name):
            raise RuntimeError(f"Target table does not exist: {table_contract.target_table_name}")

        source_df = self.spark.read.parquet(table_contract.data_uri)
        load_columns_df = self.select_load_columns(
            source_df=source_df,
            table_contract=table_contract,
        )
        target_schema = self.spark.table(table_contract.target_table_name).schema

        with self.strict_cast_mode():
            aligned_df = self.align_to_target_schema(
                load_df=load_columns_df,
                table_contract=table_contract,
                target_schema=target_schema,
            )
            self.write_table(
                df=aligned_df,
                table_contract=table_contract,
            )
        logger.info(f"Table load completed: table={table_contract.target_table_name}, run_id={self.run_id}")

    def publish_tables(self, tables: list[LoaderTableContract]) -> None:
        logger.info(f"Table load batch started: run_id={self.run_id}, tables_count={len(tables)}")
        for table_contract in tables:
            self.load_table(table_contract)
        logger.info(f"Table load batch completed: run_id={self.run_id}, tables_count={len(tables)}")

    def materialize_comparison_result(self, comparison_contract: ComparisonContract, engine: str) -> None:
        self.comparison_result_materializer.materialize(
            comparison_contract=comparison_contract,
            engine=engine,
        )
