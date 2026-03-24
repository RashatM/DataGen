import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
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
    run_id: str
    tables: list[ContractTable]
    comparison: ComparisonContract

    def build_table_contracts(self, engine: str) -> list[TableContract]:
        return [table.build_table_contract(engine) for table in self.tables]


def load_contract(contract_json: str) -> Dict[str, Any]:
    contract = json.loads(contract_json)
    if not isinstance(contract, dict):
        raise ValueError("Contract must be a JSON object")
    return contract


def parse_contract_tables(tables_payload: list[Dict[str, Any]]) -> list[ContractTable]:
    return [
        ContractTable(
            schema_name=table["schema_name"],
            table_name=table["table_name"],
            data_uri=table["artifacts"]["data_uri"],
            hive=EngineTableContract(
                ddl_uri=table["artifacts"]["engines"]["hive"]["ddl_uri"],
                target_table_name=table["artifacts"]["engines"]["hive"]["target_table_name"],
            ),
            iceberg=EngineTableContract(
                ddl_uri=table["artifacts"]["engines"]["iceberg"]["ddl_uri"],
                target_table_name=table["artifacts"]["engines"]["iceberg"]["target_table_name"],
            ),
        )
        for table in tables_payload
    ]


def parse_job_contract(contract_json: str) -> JobContract:
    """Parse trusted runtime contract already validated by the DAG."""
    contract = load_contract(contract_json)
    try:
        comparison = contract["comparison"]
        return JobContract(
            run_id=contract["run_id"],
            tables=parse_contract_tables(contract["tables"]),
            comparison=ComparisonContract(
                query_uris=dict(comparison["query_uris"]),
                result_uris=dict(comparison["result_uris"]),
                report_uri=comparison["report_uri"],
            ),
        )
    except (KeyError, TypeError) as error:
        raise ValueError(f"Trusted DAG contract has invalid structure: {error}") from error


def write_json_to_uri(spark: "SparkSession", uri: str, payload: Dict[str, Any]) -> None:
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
