import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List
from pyspark.sql import SparkSession


def create_logger() -> logging.Logger:
    custom_logger = logging.getLogger("logger")
    if custom_logger.handlers:
        return custom_logger
    custom_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='[DL_PLATFORM] %(asctime)s %(levelname)s: %(message)s',
        datefmt='%d-%m-%y %H:%M:%S'
    )
    handler.setFormatter(formatter)

    custom_logger.addHandler(handler)
    return custom_logger


logger = create_logger()


@dataclass
class TableContract:
    schema_name: str
    table_name: str
    data_uri: str
    ddl_uri: str

    full_name: str = field(init=False)
    tmp_name: str = field(init=False)
    old_name: str = field(init=False)

    def __post_init__(self):
        self.full_name = f"{self.schema_name}.{self.table_name}"
        self.tmp_name = f"{self.full_name}_tmp"
        self.old_name = f"{self.full_name}_old"


def parse_table_contracts(contract_json: str, ddl_target: str) -> List[TableContract]:
    """
    Парсит JSON-контракт от DataGen и извлекает из него список TableContract для указанного движка.

    Формат контракта:
    {
      "run_id": "20260312T120000Z_uuid",
      "tables": [
        {
          "schema_name": "sales",
          "table_name": "orders",
          "storage_type": "s3",
          "storage": {
            "data_uri": "s3a://bucket/runs/{run_id}/sales/orders/data/data.parquet",
            "ddl_uris": {
              "hive": "s3a://bucket/runs/{run_id}/sales/orders/ddl/hive.sql",
              "iceberg": "s3a://bucket/runs/{run_id}/sales/orders/ddl/iceberg.sql"
            }
          }
        }
      ]
    }

    DDL внутри файлов использует фиксированную БД per engine:
      hive.sql:    CREATE TABLE IF NOT EXISTS datagen_hive.sales__orders (...)
      iceberg.sql: CREATE TABLE IF NOT EXISTS datagen_iceberg.sales__orders (...)

    Аргументы:
        contract_json: JSON-строка с контрактом
        ddl_target: ключ движка в ddl_uris ("hive" или "iceberg")
    """
    contract = json.loads(contract_json)
    return [
        TableContract(
            schema_name=table["schema_name"],
            table_name=table["table_name"],
            data_uri=table["storage"]["data_uri"],
            ddl_uri=table["storage"]["ddl_uris"][ddl_target]
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
        return ddl.replace(table.full_name, table.tmp_name)

    def drop_table(self, table_name: str) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")

    def rename_table(self, from_name: str, to_name: str) -> None:
        self.spark.sql(f"ALTER TABLE {from_name} RENAME TO {to_name}")

    def swap_tables(self, table: TableContract) -> None:
        self.drop_table(table.old_name)
        self.rename_table(table.full_name, table.old_name)
        self.rename_table(table.tmp_name, table.full_name)
        self.drop_table(table.old_name)

    def commit_table(self, table: TableContract) -> None:
        if self.spark.catalog.tableExists(table.full_name):
            self.swap_tables(table)
        else:
            self.rename_table(table.tmp_name, table.full_name)

    def load_table(self, table: TableContract) -> None:
        logger.info(f"Loading table={table.full_name} run_id={self.run_id}")

        tmp_ddl = self.build_tmp_ddl(table)
        self.drop_table(table.tmp_name)

        try:
            self.spark.sql(tmp_ddl)
            self.write_to_tmp(table.data_uri, table.tmp_name)
            self.commit_table(table)
            logger.info(f"Successfully loaded table={table.full_name}")

        except Exception as error:
            logger.error(f"Failed to load table={table.full_name} error={error}")
            self.drop_table(table.tmp_name)
            raise

    def load_all(self, tables: List[TableContract]) -> None:
        for table in tables:
            self.load_table(table)
