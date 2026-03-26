from abc import ABC, abstractmethod
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


class BaseSynthLoader(ABC):
    def __init__(self, spark: SparkSession, run_id: str) -> None:
        self.run_id = run_id
        self.spark = spark
        self.comparison_data_normalizer = ComparisonDataNormalizer()

    @abstractmethod
    def write_to_tmp(self, data_uri: str, tmp_name: str) -> None:
        pass

    def read_ddl_from_uri(self, ddl_uri: str) -> str:
        return read_text_from_uri(self.spark, ddl_uri)

    def read_query_from_uri(self, query_uri: str) -> str:
        return read_text_from_uri(self.spark, query_uri)

    def build_tmp_ddl(self, table: TableContract) -> str:
        ddl = self.read_ddl_from_uri(table.ddl_uri)
        return ddl.replace(table.full_table_name, table.tmp_name)

    def drop_table(self, table_name: str) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")

    def rename_table(self, from_name: str, to_name: str) -> None:
        self.spark.sql(f"ALTER TABLE {from_name} RENAME TO {to_name}")

    def swap_tables(self, table: TableContract) -> None:
        self.drop_table(table.old_name)
        self.rename_table(table.full_table_name, table.old_name)
        self.rename_table(table.tmp_name, table.full_table_name)
        self.drop_table(table.old_name)

    def commit_table(self, table: TableContract) -> None:
        if self.spark.catalog.tableExists(table.full_table_name):
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
        for table in tables:
            try:
                self.drop_table(table.tmp_name)
            except Exception as error:
                logger.error(f"Failed to drop temporary table. table={table.tmp_name}, error={error}")

    def materialize_comparison_result(self, comparison_contract: ComparisonContract, engine: str) -> None:
        engine_contract = comparison_contract.get_engine_contract(engine)
        logger.info(
            f"Comparison query execution started. engine={engine}, run_id={self.run_id}, "
            f"query_uri={engine_contract.query_uri}, result_uri={engine_contract.result_uri}"
        )
        comparison_query = self.read_query_from_uri(engine_contract.query_uri)
        comparison_df = self.spark.sql(comparison_query)
        normalized_df = self.comparison_data_normalizer.normalize(comparison_df)
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

    def load_all(self, tables: list[TableContract]) -> None:
        logger.info(f"Loader execution started. run_id={self.run_id}, tables_count={len(tables)}")
        try:
            for table in tables:
                self.prepare_table(table)
        except Exception:
            logger.exception(f"Table preparation failed. run_id={self.run_id}")
            self.cleanup_tmp_tables(tables)
            raise

        for table in tables:
            self.commit_table(table)
            logger.info(f"Table committed. table={table.full_table_name}, run_id={self.run_id}")
        logger.info(f"Loader execution completed. run_id={self.run_id}, tables_count={len(tables)}")

    def execute(self, tables: list[TableContract], comparison_contract: ComparisonContract, engine: str) -> None:
        self.load_all(tables)
        self.materialize_comparison_result(comparison_contract, engine)
