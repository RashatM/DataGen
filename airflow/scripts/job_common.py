import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict

from pyspark.sql import SparkSession


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


logger = create_logger()


@dataclass
class TableContract:
    logical_table_name: str
    full_table_name: str
    data_uri: str
    ddl_uri: str

    tmp_name: str = field(init=False)
    old_name: str = field(init=False)

    def __post_init__(self) -> None:
        self.tmp_name = f"{self.full_table_name}_tmp"
        self.old_name = f"{self.full_table_name}_old"


@dataclass
class EngineTableContract:
    ddl_uri: str
    target_table_name: str


@dataclass
class ContractTable:
    schema_name: str
    table_name: str
    data_uri: str
    hive: EngineTableContract
    iceberg: EngineTableContract

    def build_table_contract(self, engine: str) -> TableContract:
        if engine == "hive":
            engine_contract = self.hive
        elif engine == "iceberg":
            engine_contract = self.iceberg
        else:
            raise ValueError(f"Unsupported engine={engine}")

        return TableContract(
            logical_table_name=f"{self.schema_name}.{self.table_name}",
            full_table_name=engine_contract.target_table_name,
            data_uri=self.data_uri,
            ddl_uri=engine_contract.ddl_uri,
        )


@dataclass
class EngineComparisonContract:
    query_uri: str
    result_uri: str


@dataclass
class ComparisonContract:
    query_uris: Dict[str, str]
    result_uris: Dict[str, str]
    report_uri: str

    def get_engine_contract(self, engine: str) -> EngineComparisonContract:
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


@dataclass
class JobContract:
    tables: list[ContractTable]
    comparison: ComparisonContract

    def build_table_contracts(self, engine: str) -> list[TableContract]:
        return [table.build_table_contract(engine) for table in self.tables]


def load_contract(contract_json: str) -> Dict[str, Any]:
    contract = json.loads(contract_json)
    if not isinstance(contract, dict):
        raise ValueError("Contract must be a JSON object")
    return contract


def require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Contract is missing non-empty '{field_name}'")
    return value


def require_object(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Contract is missing '{field_name}' object")
    return value


def parse_contract_tables(contract: Dict[str, Any]) -> list[ContractTable]:
    tables = contract.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError("Contract is missing non-empty 'tables'")

    parsed_tables = []
    for index, table_payload in enumerate(tables):
        table = require_object(table_payload, f"tables[{index}]")
        artifacts = require_object(table.get("artifacts"), f"tables[{index}].artifacts")
        engines = require_object(artifacts.get("engines"), f"tables[{index}].artifacts.engines")

        hive = require_object(engines.get("hive"), f"tables[{index}].artifacts.engines.hive")
        iceberg = require_object(engines.get("iceberg"), f"tables[{index}].artifacts.engines.iceberg")

        parsed_tables.append(
            ContractTable(
                schema_name=require_non_empty_string(table.get("schema_name"), f"tables[{index}].schema_name"),
                table_name=require_non_empty_string(table.get("table_name"), f"tables[{index}].table_name"),
                data_uri=require_non_empty_string(artifacts.get("data_uri"), f"tables[{index}].artifacts.data_uri"),
                hive=EngineTableContract(
                    ddl_uri=require_non_empty_string(
                        hive.get("ddl_uri"),
                        f"tables[{index}].artifacts.engines.hive.ddl_uri",
                    ),
                    target_table_name=require_non_empty_string(
                        hive.get("target_table_name"),
                        f"tables[{index}].artifacts.engines.hive.target_table_name",
                    ),
                ),
                iceberg=EngineTableContract(
                    ddl_uri=require_non_empty_string(
                        iceberg.get("ddl_uri"),
                        f"tables[{index}].artifacts.engines.iceberg.ddl_uri",
                    ),
                    target_table_name=require_non_empty_string(
                        iceberg.get("target_table_name"),
                        f"tables[{index}].artifacts.engines.iceberg.target_table_name",
                    ),
                ),
            )
        )

    return parsed_tables


def validate_comparison_contract(contract: Dict[str, Any]) -> Dict[str, Any]:
    comparison = require_object(contract.get("comparison"), "comparison")

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


def parse_job_contract(contract_json: str) -> JobContract:
    """Parse and validate the full DataGen runtime contract."""
    contract = load_contract(contract_json)
    comparison = validate_comparison_contract(contract)
    return JobContract(
        tables=parse_contract_tables(contract),
        comparison=ComparisonContract(
            query_uris=dict(comparison["query_uris"]),
            result_uris=dict(comparison["result_uris"]),
            report_uri=comparison["report_uri"],
        ),
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
