import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List
from pyspark.sql import SparkSession
import pyspark.sql.functions as spark_functions
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
    StringType,
    StructType,
)


def create_logger() -> logging.Logger:
    custom_logger = logging.getLogger("logger")
    if custom_logger.handlers:
        return custom_logger
    custom_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="[DL_PLATFORM] %(asctime)s %(levelname)s: %(message)s",
        datefmt="%d-%m-%y %H:%M:%S"
    )
    handler.setFormatter(formatter)

    custom_logger.addHandler(handler)
    custom_logger.propagate = False
    return custom_logger


airflow_logger = create_logger()


@dataclass
class TableContract:
    logical_table_name: str
    full_table_name: str
    data_uri: str
    ddl_uri: str

    tmp_name: str = field(init=False)
    old_name: str = field(init=False)

    def __post_init__(self):
        self.tmp_name = f"{self.full_table_name}_tmp"
        self.old_name = f"{self.full_table_name}_old"


@dataclass
class EngineComparisonContract:
    query_uri: str
    result_uri: str


@dataclass
class ComparisonContract:
    query_uris: Dict[str, str]
    result_uris: Dict[str, str]
    report_uri: str

    def for_engine(self, engine: str) -> EngineComparisonContract:
        query_uri = self.query_uris.get(engine)
        if not isinstance(query_uri, str) or not query_uri.strip():
            raise ValueError(f"Comparison contract is missing query_uri for engine={engine}")

        result_uri = self.result_uris.get(engine)
        if not isinstance(result_uri, str) or not result_uri.strip():
            raise ValueError(f"Comparison contract is missing result_uri for engine={engine}")

        return EngineComparisonContract(
            query_uri=query_uri,
            result_uri=result_uri,
        )


def load_contract(contract_json: str) -> Dict[str, Any]:
    contract = json.loads(contract_json)
    if not isinstance(contract, dict):
        raise ValueError("Contract must be a JSON object")
    return contract


def validate_comparison_contract(contract: Dict[str, Any]) -> Dict[str, Any]:
    comparison = contract.get("comparison")
    if not isinstance(comparison, dict):
        raise ValueError("Contract is missing 'comparison' object")

    query_uris = comparison.get("query_uris")
    if not isinstance(query_uris, dict):
        raise ValueError("Contract is missing 'comparison.query_uris'")
    for engine in ("hive", "iceberg"):
        query_uri = query_uris.get(engine)
        if not isinstance(query_uri, str) or not query_uri.strip():
            raise ValueError(f"Contract is missing non-empty 'comparison.query_uris.{engine}'")

    report_uri = comparison.get("report_uri")
    if not isinstance(report_uri, str) or not report_uri.strip():
        raise ValueError("Contract is missing non-empty 'comparison.report_uri'")

    result_uris = comparison.get("result_uris")
    if not isinstance(result_uris, dict):
        raise ValueError("Contract is missing 'comparison.result_uris'")

    for engine in ("hive", "iceberg"):
        result_uri = result_uris.get(engine)
        if not isinstance(result_uri, str) or not result_uri.strip():
            raise ValueError(f"Contract is missing non-empty 'comparison.result_uris.{engine}'")

    return comparison


def parse_table_contracts(contract_json: str, engine: str) -> List[TableContract]:
    """
    Парсит JSON-контракт от DataGen и извлекает из него список TableContract для указанного движка.

    Формат контракта:
    {
      "run_id": "...",
      "tables": [
        {
          "schema_name": "sales",
          "table_name": "orders",
          "artifacts": {
            "data_uri": "s3a://bucket/runs/{run_id}/sales/orders/data/data.parquet",
            "engines": {
              "hive": {
                "ddl_uri": "s3a://bucket/runs/{run_id}/sales/orders/ddl/hive.sql",
                "target_table_name": "datagen_hive.sales__orders"
              },
              "iceberg": {
                "ddl_uri": "s3a://bucket/runs/{run_id}/sales/orders/ddl/iceberg.sql",
                "target_table_name": "datagen_iceberg.sales__orders"
              }
            }
          }
        }
      ]
    }

    Аргументы:
        contract_json: JSON-строка с контрактом
        engine: ключ движка в engines ("hive" или "iceberg")
    """
    contract = load_contract(contract_json)
    return [
        TableContract(
            logical_table_name=f"{table['schema_name']}.{table['table_name']}",
            full_table_name=table["artifacts"]["engines"][engine]["target_table_name"],
            data_uri=table["artifacts"]["data_uri"],
            ddl_uri=table["artifacts"]["engines"][engine]["ddl_uri"],
        )
        for table in contract["tables"]
    ]


def parse_comparison_contract(contract_json: str) -> ComparisonContract:
    contract = load_contract(contract_json)
    comparison = validate_comparison_contract(contract)
    return ComparisonContract(
        query_uris=dict(comparison["query_uris"]),
        result_uris=dict(comparison["result_uris"]),
        report_uri=comparison["report_uri"],
    )


def write_json_to_uri(spark: SparkSession, uri: str, payload: Dict[str, Any]) -> None:
    content = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    file_system = path.getFileSystem(hadoop_conf)

    output_stream = file_system.create(path, True)
    try:
        writer = jvm.java.io.OutputStreamWriter(output_stream, "UTF-8")
        writer.write(content)
        writer.flush()
    finally:
        output_stream.close()


class BaseSynthLoader(ABC):
    TIMESTAMP_OUTPUT_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    DATE_OUTPUT_FORMAT = "yyyy-MM-dd"
    NORMALIZED_DECIMAL_TYPE = "decimal(38,18)"

    def __init__(self, spark: SparkSession, run_id: str) -> None:
        self.run_id = run_id
        self.spark = spark

    @abstractmethod
    def write_to_tmp(self, data_uri: str, tmp_name: str) -> None:
        pass

    def read_ddl_from_s3(self, ddl_uri: str) -> str:
        return self.spark.sparkContext.wholeTextFiles(ddl_uri).values().first()

    def read_query_from_s3(self, query_uri: str) -> str:
        return self.spark.sparkContext.wholeTextFiles(query_uri).values().first()

    @staticmethod
    def normalize_for_comparison(df: DataFrame) -> DataFrame:
        expressions = []
        for field in df.schema.fields:
            col = spark_functions.col(field.name)
            dt = field.dataType

            if isinstance(dt, (ArrayType, MapType, StructType)):
                raise ValueError(
                    f"Complex type is not supported for comparison: "
                    f"column={field.name}, type={dt.simpleString()}"
                )

            if dt.typeName() in {"timestamp", "timestamp_ntz"}:
                expressions.append(
                    spark_functions.date_format(
                        col.cast("timestamp"), BaseSynthLoader.TIMESTAMP_OUTPUT_FORMAT
                    ).alias(field.name)
                )
            elif dt.typeName() == "date":
                expressions.append(
                    spark_functions.date_format(
                        col.cast("date"), BaseSynthLoader.DATE_OUTPUT_FORMAT
                    ).alias(field.name)
                )
            elif isinstance(dt, (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                expressions.append(col.cast(BaseSynthLoader.NORMALIZED_DECIMAL_TYPE).alias(field.name))
            elif isinstance(dt, BooleanType):
                expressions.append(spark_functions.lower(col.cast("string")).alias(field.name))
            else:
                expressions.append(col)

        return df.select(*expressions)

    def build_tmp_ddl(self, table: TableContract) -> str:
        ddl = self.read_ddl_from_s3(table.ddl_uri)
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
        airflow_logger.info(f"Preparing table. table={table.full_table_name}, run_id={self.run_id}")
        tmp_ddl = self.build_tmp_ddl(table)
        self.drop_table(table.tmp_name)
        self.spark.sql(tmp_ddl)
        self.write_to_tmp(table.data_uri, table.tmp_name)
        airflow_logger.info(f"Table prepared. table={table.full_table_name}, run_id={self.run_id}")

    def cleanup_tmp_tables(self, tables: List[TableContract]) -> None:
        for table in tables:
            try:
                self.drop_table(table.tmp_name)
            except Exception as error:
                airflow_logger.error(f"Failed to drop temporary table. table={table.tmp_name}, error={error}")

    def materialize_comparison_result(self, comparison_contract: ComparisonContract, engine: str) -> None:
        engine_contract = comparison_contract.for_engine(engine)
        airflow_logger.info(
            f"Comparison query execution started. engine={engine}, run_id={self.run_id}, "
            f"query_uri={engine_contract.query_uri}, result_uri={engine_contract.result_uri}"
        )
        comparison_query = self.read_query_from_s3(engine_contract.query_uri)
        comparison_df = self.spark.sql(comparison_query)
        normalized_df = self.normalize_for_comparison(comparison_df)
        normalized_df.write.mode("overwrite").parquet(engine_contract.result_uri)
        airflow_logger.info(
            f"Comparison query execution completed. engine={engine}, run_id={self.run_id}, "
            f"result_uri={engine_contract.result_uri}"
        )

    def load_all(self, tables: List[TableContract]) -> None:
        airflow_logger.info(f"Loader execution started. run_id={self.run_id}, tables_count={len(tables)}")
        try:
            for table in tables:
                self.prepare_table(table)
        except Exception:
            airflow_logger.exception(f"Table preparation failed. run_id={self.run_id}")
            self.cleanup_tmp_tables(tables)
            raise

        for table in tables:
            self.commit_table(table)
            airflow_logger.info(f"Table committed. table={table.full_table_name}, run_id={self.run_id}")
        airflow_logger.info(f"Loader execution completed. run_id={self.run_id}, tables_count={len(tables)}")

    def execute(self, tables: List[TableContract], comparison_contract: ComparisonContract, engine: str) -> None:
        self.load_all(tables)
        self.materialize_comparison_result(comparison_contract, engine)
