import uuid
from datetime import datetime
from typing import Any, List
from zoneinfo import ZoneInfo

from app.core.application.dto import DagRunResult, TablePublication
from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.core.application.services.generation_service import DataGenerationService
from app.core.application.services.publication_service import PublicationService
from app.core.domain.entities import GeneratedTableData
from app.infrastructure.converters.schema_converter import convert_to_generation_run
from app.providers import provide_dag_runner, provide_generation_service, provide_publication_service
from app.shared.config import load_app_settings
from app.shared.logger import pipeline_logger

DEFAULT_DAG_TIMEOUT_SECONDS = 600
logger = pipeline_logger


class DataPipelineExecutor:

    def __init__(
        self,
        generation_service: DataGenerationService,
        publication_service: PublicationService,
        dag_runner: DagRunnerPort,
        dag_timeout_seconds: int = DEFAULT_DAG_TIMEOUT_SECONDS,
    ) -> None:
        self.generation_service = generation_service
        self.publication_service = publication_service
        self.dag_runner = dag_runner
        self.dag_timeout_seconds = dag_timeout_seconds

    @staticmethod
    def generate_run_id() -> str:
        now = datetime.now(ZoneInfo("Europe/Moscow"))
        short_uid = uuid.uuid4().hex[:8]
        return f"{now:%Y%m%d_%H%M%S}_{short_uid}"

    def generate(
        self,
        run_id: str,
        raw_tables: List[Any],
    ) -> List[GeneratedTableData]:
        logger.info(f"Generation started. run_id={run_id}, raw_tables_count={len(raw_tables)}")
        generation_run = convert_to_generation_run(run_id=run_id, raw_tables=raw_tables)
        generated_tables = self.generation_service.generate_table_data(generation_run)

        table_names = [t.table.full_table_name for t in generated_tables]
        logger.info(
            f"Generation completed. run_id={run_id}, tables_count={len(generated_tables)}, tables={table_names}"
        )
        return generated_tables

    def publish(
        self,
        run_id: str,
        generated_tables: List[GeneratedTableData],
    ) -> List[TablePublication]:
        logger.info(f"Publication started. run_id={run_id}, tables_count={len(generated_tables)}")
        publications = self.publication_service.publish(
            run_id=run_id,
            generated_tables=generated_tables,
        )
        logger.info(f"Publication completed. run_id={run_id}, tables_count={len(publications)}")
        return publications

    def trigger_dag(
        self,
        run_id: str,
        publications: List[TablePublication],
    ) -> DagRunResult:
        logger.info(f"DAG execution started. run_id={run_id}, tables_count={len(publications)}")
        dag_result = self.dag_runner.trigger_and_wait(
            run_id=run_id,
            publications=publications,
            timeout_seconds=self.dag_timeout_seconds,
        )
        logger.info(f"DAG execution completed. run_id={run_id}, status={dag_result.status.value}")
        return dag_result

    def execute(self, raw_tables: List[Any]) -> DagRunResult:
        run_id = self.generate_run_id()
        logger.info(f"Pipeline started. run_id={run_id}")

        generated_tables = self.generate(run_id, raw_tables)
        publications = self.publish(run_id, generated_tables)
        dag_result = self.trigger_dag(run_id, publications)

        logger.info(f"Pipeline completed. run_id={run_id}, status={dag_result.status.value}")
        return dag_result


def run_app(env_name: str, raw_tables: List[Any]) -> None:
    logger.info(f"Application started. environment={env_name}")
    config = load_app_settings(env_name)

    pipeline = DataPipelineExecutor(
        generation_service=provide_generation_service(),
        publication_service=provide_publication_service(config.s3, config.target_storage),
        dag_runner=provide_dag_runner(config.airflow),
    )
    dag_result = pipeline.execute(raw_tables)

    logger.info(
        f"Application finished. environment={env_name}, dag_run_id={dag_result.run_id}, status={dag_result.status.value}"
    )
