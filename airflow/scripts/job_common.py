import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast
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
TECHNICAL_FAILURE_CODE = "TECHNICAL_FAILURE"


class JobUserFacingError(RuntimeError):
    """Expected Spark-job failure that should be surfaced to the DataGen user."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class LoaderTableContract:
    """Контракт одной таблицы, уже подготовленный для конкретного loader-а и одного движка."""
    target_table_name: str  # Физическая target table, в которую пишет loader.
    data_uri: str  # URI parquet-артефакта с синтетическими данными.
    columns: tuple[str, ...]  # Колонки parquet, которые нужно загрузить в target table.
    write_mode: str  # Режим записи: append, overwrite table или overwrite partitions.


@dataclass
class EngineLoadContract:
    """Engine-specific часть общего table payload до выбора конкретного loader-а."""
    target_table_name: str  # Target table для конкретного движка: Hive или Iceberg.
    write_mode: str  # Режим записи для этого движка.
    columns: tuple[str, ...]  # Колонки, которые этот движок должен загрузить.


@dataclass
class PublishedTableContract:
    """Общий table entry из DAG payload с одним parquet-артефактом и двумя engine-specific load blocks."""
    table_name: str  # Логическое имя таблицы из DataGen contract.
    data_uri: str  # Общий parquet-артефакт, опубликованный DataGen.
    hive: EngineLoadContract  # Настройки загрузки этой таблицы в Hive.
    iceberg: EngineLoadContract  # Настройки загрузки этой таблицы в Iceberg.

    def build_loader_contract(self, engine: str) -> LoaderTableContract:
        """Разворачивает общий table entry в load contract для выбранного loader-а."""
        if engine == "hive":
            engine_contract = self.hive
        elif engine == "iceberg":
            engine_contract = self.iceberg
        else:
            raise ValueError(f"Unsupported engine={engine}")

        return LoaderTableContract(
            target_table_name=engine_contract.target_table_name,
            data_uri=self.data_uri,
            columns=engine_contract.columns,
            write_mode=engine_contract.write_mode,
        )


@dataclass
class EngineComparisonContract:
    """Ссылки и настройки comparison для одного конкретного движка."""
    query_uri: str  # URI SQL-запроса, который надо выполнить в этом движке.
    result_uri: str  # URI parquet-результата comparison query.
    excluded_columns: tuple[str, ...]  # Колонки результата, исключаемые из сравнения.


@dataclass
class ComparisonContract:
    """Comparison-блок runtime-контракта DAG после парсинга из JSON."""
    query_uris: dict[str, str]  # URI SQL-запросов по движкам.
    result_uris: dict[str, str]  # URI parquet-результатов по движкам.
    exclude_columns: dict[str, tuple[str, ...]]  # Пользовательские исключения колонок по движкам.
    report_uri: str  # URI итогового JSON-отчета comparison.

    def get_engine_contract(self, engine: str) -> EngineComparisonContract:
        """Достаёт и валидирует comparison contract для запрошенного движка."""
        query_uri = self.query_uris.get(engine)
        if not isinstance(query_uri, str) or not query_uri.strip():
            raise ValueError(f"Comparison contract is missing query_uri for engine={engine}")

        result_uri = self.result_uris.get(engine)
        if not isinstance(result_uri, str) or not result_uri.strip():
            raise ValueError(f"Comparison contract is missing result_uri for engine={engine}")

        return EngineComparisonContract(
            query_uri=query_uri,
            result_uri=result_uri,
            excluded_columns=tuple(self.exclude_columns.get(engine, ())),
        )


@dataclass
class JobContract:
    """Полный runtime-контракт Spark job-а: run_id, таблицы и comparison-настройки."""
    run_id: str  # Идентификатор запуска DataGen.
    tables: list[PublishedTableContract]  # Таблицы, которые нужно загрузить.
    comparison: ComparisonContract  # Настройки SQL-сверки после загрузки.

    def build_load_contracts(self, engine: str) -> list[LoaderTableContract]:
        """Разворачивает таблицы в список load contracts для одного конкретного loader-а."""
        return [table.build_loader_contract(engine) for table in self.tables]


def load_contract(contract_json: str) -> dict[str, Any]:
    contract = json.loads(contract_json)
    if not isinstance(contract, dict):
        raise ValueError("Contract must be a JSON object")
    return contract


def parse_published_tables(tables_payload: list[dict[str, Any]]) -> list[PublishedTableContract]:
    published_tables: list[PublishedTableContract] = []
    for table in tables_payload:
        load_payload = cast(dict[str, Any], table["load"])
        hive_payload = cast(dict[str, Any], load_payload["hive"])
        iceberg_payload = cast(dict[str, Any], load_payload["iceberg"])

        published_tables.append(
            PublishedTableContract(
                table_name=cast(str, table["table_name"]),
                data_uri=cast(str, table["data_uri"]),
                hive=EngineLoadContract(
                    target_table_name=cast(str, hive_payload["target_table_name"]),
                    write_mode=cast(str, hive_payload["write_mode"]),
                    columns=tuple(cast(list[str], hive_payload["columns"])),
                ),
                iceberg=EngineLoadContract(
                    target_table_name=cast(str, iceberg_payload["target_table_name"]),
                    write_mode=cast(str, iceberg_payload["write_mode"]),
                    columns=tuple(cast(list[str], iceberg_payload["columns"])),
                ),
            )
        )
    return published_tables


def parse_job_contract(contract_json: str) -> JobContract:
    """Парсит доверенный runtime-контракт, который уже прошёл валидацию на стороне DAG."""
    contract = load_contract(contract_json)
    try:
        comparison = cast(dict[str, Any], contract["comparison"])
        exclude_columns_payload = cast(dict[str, list[str]], comparison.get("exclude_columns", {}))
        return JobContract(
            run_id=cast(str, contract["run_id"]),
            tables=parse_published_tables(cast(list[dict[str, Any]], contract["tables"])),
            comparison=ComparisonContract(
                query_uris=cast(dict[str, str], comparison["query_uris"]),
                result_uris=cast(dict[str, str], comparison["result_uris"]),
                exclude_columns={
                    engine: tuple(columns)
                    for engine, columns in exclude_columns_payload.items()
                },
                report_uri=cast(str, comparison["report_uri"]),
            ),
        )
    except (KeyError, TypeError) as error:
        raise ValueError(f"Trusted DAG contract has invalid structure: {error}") from error


def write_json_to_uri(spark: "SparkSession", uri: str, payload: dict[str, Any]) -> None:
    """Записывает компактный JSON в URI через Hadoop FileSystem, доступный Spark runtime-у."""
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


def write_diagnostic_safely(
    spark: "SparkSession",
    diagnostic_uri: str,
    run_id: str,
    task_id: str,
    code: str,
    message: str,
) -> None:
    """Best-effort запись diagnostics: ошибка записи не должна маскировать исходное падение."""
    try:
        write_json_to_uri(
            spark=spark,
            uri=diagnostic_uri,
            payload={
                "run_id": run_id,
                "task_id": task_id,
                "message": message,
                "code": code,
            },
        )
        logger.info(
            f"Execution diagnostic written: run_id={run_id}, task_id={task_id}, "
            f"code={code}, uri={diagnostic_uri}"
        )
    except Exception:
        logger.exception(
            f"Failed to write execution diagnostic: run_id={run_id}, "
            f"task_id={task_id}, code={code}, uri={diagnostic_uri}"
        )


def run_with_diagnostic(
    spark: "SparkSession",
    diagnostic_uri: str,
    run_id: str,
    task_id: str,
    action: Callable[[], None],
) -> None:
    """Выполняет Spark task и пишет diagnostic artifact при падении."""
    try:
        action()
    except JobUserFacingError as error:
        logger.error(
            f"Spark task failed with user-facing error: run_id={run_id}, "
            f"task_id={task_id}, code={error.code}, error={error}"
        )
        write_diagnostic_safely(
            spark=spark,
            diagnostic_uri=diagnostic_uri,
            run_id=run_id,
            task_id=task_id,
            code=error.code,
            message=str(error),
        )
        raise
    except Exception:
        message = f"Technical failure in {task_id}. Stack trace is available in Airflow task logs."
        logger.exception(
            f"Spark task failed with technical error: run_id={run_id}, task_id={task_id}"
        )
        write_diagnostic_safely(
            spark=spark,
            diagnostic_uri=diagnostic_uri,
            run_id=run_id,
            task_id=task_id,
            code=TECHNICAL_FAILURE_CODE,
            message=message,
        )
        raise


def read_text_from_uri(spark: "SparkSession", uri: str) -> str:
    """Читает текстовый артефакт из URI через Hadoop FileSystem, не выходя из Spark runtime."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    file_system = path.getFileSystem(hadoop_conf)

    input_stream = file_system.open(path)
    scanner = jvm.java.util.Scanner(input_stream, "UTF-8").useDelimiter("\\A")
    try:
        if scanner.hasNext():
            return scanner.next()
        return ""
    finally:
        scanner.close()
