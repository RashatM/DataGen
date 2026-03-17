import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List
from pyspark.sql import SparkSession


def create_logger() -> logging.Logger:
    LOGGER_FORMAT = "[DL_PLATFORM] %(asctime)s %(levelname)s: %(message)s"
    LOGGER_DATE_FORMAT = "%d-%m-%y %H:%M:%S"

    custom_logger = logging.getLogger("logger")
    if custom_logger.handlers:
        return custom_logger
    custom_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt=LOGGER_FORMAT,
        datefmt=LOGGER_DATE_FORMAT
    )
    handler.setFormatter(formatter)

    custom_logger.addHandler(handler)
    custom_logger.propagate = False
    return custom_logger


airflow_logger = create_logger()


@dataclass
class TableContract:
    full_table_name: str
    data_uri: str
    ddl_uri: str

    tmp_name: str = field(init=False)
    old_name: str = field(init=False)

    def __post_init__(self):
        self.tmp_name = f"{self.full_table_name}_tmp"
        self.old_name = f"{self.full_table_name}_old"


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
    contract = json.loads(contract_json)
    return [
        TableContract(
            full_table_name=table["artifacts"]["engines"][engine]["target_table_name"],
            data_uri=table["artifacts"]["data_uri"],
            ddl_uri=table["artifacts"]["engines"][engine]["ddl_uri"],
        )
        for table in contract["tables"]
    ]


class BaseSynthLoader(ABC):

    def __init__(self, spark: SparkSession, run_id: str) -> None:
        self.run_id = run_id
        self.spark = spark

    @abstractmethod
    def write_to_tmp(self, data_uri: str, tmp_name: str) -> None:
        pass

    def read_ddl_from_s3(self, ddl_uri: str) -> str:
        return self.spark.sparkContext.wholeTextFiles(ddl_uri).values().first()

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
