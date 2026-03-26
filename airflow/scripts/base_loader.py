from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
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

from job_common import ComparisonContract, TableContract, logger, read_text_from_uri


class ComparisonDataNormalizer:
    TIMESTAMP_OUTPUT_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    DATE_OUTPUT_FORMAT = "yyyy-MM-dd"
    NORMALIZED_INTEGER_TYPE = "bigint"
    NORMALIZED_DECIMAL_TYPE = "decimal(38,18)"

    def normalize(self, df: DataFrame) -> DataFrame:
        expressions = []
        for schema_field in df.schema.fields:
            col = f.col(schema_field.name)
            dt = schema_field.dataType

            if isinstance(dt, (ArrayType, MapType, StructType)):
                raise ValueError(
                    f"Complex type is not supported for comparison: "
                    f"column={schema_field.name}, type={dt.simpleString()}"
                )

            if dt.typeName() in {"timestamp", "timestamp_ntz"}:
                expressions.append(
                    f.date_format(
                        col.cast("timestamp"), self.TIMESTAMP_OUTPUT_FORMAT
                    ).alias(schema_field.name)
                )
            elif dt.typeName() == "date":
                expressions.append(
                    f.date_format(
                        col.cast("date"), self.DATE_OUTPUT_FORMAT
                    ).alias(schema_field.name)
                )
            elif isinstance(dt, (ByteType, ShortType, IntegerType, LongType)):
                expressions.append(col.cast(self.NORMALIZED_INTEGER_TYPE).alias(schema_field.name))
            elif isinstance(dt, (FloatType, DoubleType, DecimalType)):
                expressions.append(col.cast(self.NORMALIZED_DECIMAL_TYPE).alias(schema_field.name))
            elif isinstance(dt, BooleanType):
                expressions.append(f.lower(col.cast("string")).alias(schema_field.name))
            else:
                expressions.append(col)

        return df.select(*expressions)


class ComparisonResultMaterializer:
    """Выполняет comparison query и сохраняет нормализованный parquet-результат."""

    def __init__(
        self,
        spark: SparkSession,
        run_id: str,
        normalizer: ComparisonDataNormalizer,
    ) -> None:
        self.spark = spark
        self.run_id = run_id
        self.normalizer = normalizer

    def read_query_from_uri(self, query_uri: str) -> str:
        return read_text_from_uri(self.spark, query_uri)

    def materialize(self, comparison_contract: ComparisonContract, engine: str) -> None:
        engine_contract = comparison_contract.get_engine_contract(engine)
        logger.info(
            f"Comparison query execution started. engine={engine}, run_id={self.run_id}, "
            f"query_uri={engine_contract.query_uri}, result_uri={engine_contract.result_uri}"
        )
        comparison_query = self.read_query_from_uri(engine_contract.query_uri)
        comparison_df = self.spark.sql(comparison_query)
        normalized_df = self.normalizer.normalize(comparison_df)
        self.write_result(normalized_df, engine_contract.result_uri, engine)
        logger.info(
            f"Comparison query execution completed. engine={engine}, run_id={self.run_id}, "
            f"result_uri={engine_contract.result_uri}"
        )

    def write_result(self, df: DataFrame, result_uri: str, engine: str) -> None:
        try:
            df.write.mode("overwrite").parquet(result_uri)
        except Exception as error:
            raise RuntimeError(
                f"Failed to write comparison parquet. engine={engine}, "
                f"result_uri={result_uri}"
            ) from error


@dataclass(slots=True)
class TableState:
    """Снимок физических имен одной таблицы во время commit/recovery."""

    full_exists: bool
    tmp_exists: bool
    old_exists: bool


class BaseSynthLoader(ABC):
    """Загрузка таблиц в целевой engine с двухфазным prepare/commit."""

    def __init__(self, spark: SparkSession, run_id: str) -> None:
        self.run_id = run_id
        self.spark = spark
        self.comparison_data_normalizer = ComparisonDataNormalizer()
        self.comparison_result_materializer = ComparisonResultMaterializer(
            spark=self.spark,
            run_id=self.run_id,
            normalizer=self.comparison_data_normalizer,
        )

    @abstractmethod
    def write_to_tmp(self, data_uri: str, tmp_name: str) -> None:
        pass

    def read_ddl_from_uri(self, ddl_uri: str) -> str:
        return read_text_from_uri(self.spark, ddl_uri)

    def build_tmp_ddl(self, table: TableContract) -> str:
        ddl = self.read_ddl_from_uri(table.ddl_uri)
        return ddl.replace(table.full_table_name, table.tmp_name)

    def drop_table(self, table_name: str) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")

    def rename_table(self, from_name: str, to_name: str) -> None:
        self.spark.sql(f"ALTER TABLE {from_name} RENAME TO {to_name}")

    def table_exists(self, table_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(table_name))

    def get_table_state(self, table: TableContract) -> TableState:
        """Проверяет, какие из `full/tmp/old` имен реально существуют сейчас."""
        return TableState(
            full_exists=self.table_exists(table.full_table_name),
            tmp_exists=self.table_exists(table.tmp_name),
            old_exists=self.table_exists(table.old_name),
        )

    def swap_tables(self, table: TableContract) -> None:
        """Заменяет существующую целевую таблицу на tmp-таблицу через промежуточное `old` имя."""
        self.drop_table(table.old_name)
        self.rename_table(table.full_table_name, table.old_name)
        self.rename_table(table.tmp_name, table.full_table_name)

    def commit_table(self, table: TableContract, existed_before: bool) -> None:
        """Публикует подготовленную tmp-таблицу в целевое имя.

        `existed_before` передаётся извне, чтобы решение о сценарии commit не зависело
        от промежуточных rename во время rollback/повторных проверок.
        """
        if existed_before:
            self.swap_tables(table)
        else:
            self.rename_table(table.tmp_name, table.full_table_name)

    def prepare_table(self, table: TableContract) -> None:
        logger.info(f"Preparing table. table={table.full_table_name}, run_id={self.run_id}")
        tmp_ddl = self.build_tmp_ddl(table)
        self.drop_table(table.tmp_name)
        self.spark.sql(tmp_ddl)
        self.write_to_tmp(table.data_uri, table.tmp_name)
        logger.info(f"Table prepared. table={table.full_table_name}, run_id={self.run_id}")

    def cleanup_tmp_tables(self, tables: list[TableContract]) -> None:
        """Best-effort cleanup для tmp-таблиц, которые больше не будут committed."""
        for table in tables:
            try:
                self.drop_table(table.tmp_name)
            except Exception as error:
                logger.error(f"Failed to drop temporary table. table={table.tmp_name}, error={error}")

    def cleanup_backup_tables(self, tables: list[TableContract]) -> None:
        """Удаляет `old`-backup после успешного завершения batch-коммита."""
        for table in tables:
            if not self.table_exists(table.old_name):
                continue
            try:
                self.drop_table(table.old_name)
            except Exception:
                logger.exception(
                    f"Failed to drop backup table after successful load. table={table.old_name}, run_id={self.run_id}"
                )

    def recover_failed_existing_commit(self, table: TableContract) -> None:
        """Чинит таблицу, если swap существующей таблицы оборвался посередине.

        Безопасное правило здесь простое:
        - если `full` уже существует, не трогаем его
        - если `full` пропал, но `old` есть, возвращаем `old -> full`
        - `tmp` удаляем только когда итоговый `full` уже на месте
        """
        state = self.get_table_state(table)

        if not state.full_exists and state.old_exists:
            try:
                self.rename_table(table.old_name, table.full_table_name)
            except Exception:
                logger.exception(
                    f"Failed to restore original table from backup after failed commit. "
                    f"table={table.full_table_name}, run_id={self.run_id}"
                )
        elif not state.full_exists and not state.old_exists:
            logger.error(
                f"Failed commit recovery cannot restore table: both full and old are missing. "
                f"table={table.full_table_name}, run_id={self.run_id}"
            )

        if self.table_exists(table.tmp_name) and self.table_exists(table.full_table_name):
            try:
                self.drop_table(table.tmp_name)
            except Exception:
                logger.exception(
                    f"Failed to drop temporary table after failed commit recovery. "
                    f"table={table.tmp_name}, run_id={self.run_id}"
                )

    def rollback_failed_new_table(self, table: TableContract) -> None:
        """Удаляет новую таблицу, если её commit завершился ошибкой или частично."""
        if self.table_exists(table.full_table_name):
            try:
                self.drop_table(table.full_table_name)
            except Exception:
                logger.exception(
                    f"Failed to drop new table after failed commit. "
                    f"table={table.full_table_name}, run_id={self.run_id}"
                )

        if self.table_exists(table.tmp_name):
            try:
                self.drop_table(table.tmp_name)
            except Exception:
                logger.exception(
                    f"Failed to drop temporary table after failed new-table commit. "
                    f"table={table.tmp_name}, run_id={self.run_id}"
                )

    def recover_failed_table(self, table: TableContract, existed_before: bool) -> None:
        """Выполняет recovery только для таблицы, на которой commit упал."""
        if existed_before:
            self.recover_failed_existing_commit(table)
            return
        self.rollback_failed_new_table(table)

    def materialize_comparison_result(self, comparison_contract: ComparisonContract, engine: str) -> None:
        self.comparison_result_materializer.materialize(comparison_contract, engine)

    def publish_tables(self, tables: list[TableContract]) -> None:
        """Готовит и публикует batch таблиц двухфазно: сначала все tmp, потом все commit."""
        logger.info(f"Table publish started. run_id={self.run_id}, tables_count={len(tables)}")

        existed_before = {
            table.full_table_name: self.table_exists(table.full_table_name)
            for table in tables
        }

        try:
            for table in tables:
                self.prepare_table(table)
        except Exception:
            self.cleanup_tmp_tables(tables)
            raise

        for index, table in enumerate(tables):
            table_existed_before = existed_before[table.full_table_name]
            try:
                self.commit_table(table, table_existed_before)
            except Exception:
                logger.exception(f"Table commit failed. table={table.full_table_name}, run_id={self.run_id}")
                self.recover_failed_table(table, table_existed_before)
                remaining_tables = tables[index + 1:]
                if remaining_tables:
                    self.cleanup_tmp_tables(remaining_tables)
                raise
            logger.info(f"Table committed. table={table.full_table_name}, run_id={self.run_id}")

        self.cleanup_backup_tables([t for t in tables if existed_before[t.full_table_name]])
        logger.info(f"Table publish completed. run_id={self.run_id}, tables_count={len(tables)}")
