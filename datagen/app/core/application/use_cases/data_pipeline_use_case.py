import uuid
from datetime import datetime, timezone
from typing import Any, List

from app.core.application.dto import DagRunResult, TablePublication
from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.core.application.services.generation_service import DataGenerationService
from app.core.application.services.publication_service import PublicationService
from app.core.domain.entities import GeneratedTableData
from app.shared.logger import logger

DEFAULT_DAG_TIMEOUT_SECONDS = 600


class DataPipelineUseCase:

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
        return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}_{uuid.uuid7()}"

    def generate(
        self,
        run_id: str,
        raw_tables: List[Any],
    ) -> List[GeneratedTableData]:
        from app.infrastructure.converters.schema_converter import convert_to_generation_run

        generation_run = convert_to_generation_run(run_id=run_id, raw_tables=raw_tables)
        generated_tables = self.generation_service.generate_table_data(generation_run)

        logger.info("Generated %d tables for run_id=%s", len(generated_tables), run_id)
        return generated_tables

    def publish(
        self,
        run_id: str,
        generated_tables: List[GeneratedTableData],
    ) -> List[TablePublication]:
        published_tables = self.publication_service.publish_tables(
            run_id=run_id,
            generated_tables=generated_tables,
        )
        logger.info("Published %d tables run_id=%s", len(published_tables), run_id)
        return published_tables

    def trigger_dag(
        self,
        run_id: str,
        published_tables: List[TablePublication],
    ) -> DagRunResult:
        dag_result = self.dag_runner.trigger_and_wait(
            run_id=run_id,
            publications=published_tables,
            timeout_seconds=self.dag_timeout_seconds,
        )
        logger.info("DAG finished run_id=%s status=%s", run_id, dag_result.status.value)
        return dag_result

    def execute(self, raw_tables: List[Any]) -> DagRunResult:
        run_id = self.generate_run_id()
        logger.info("Starting pipeline run_id=%s", run_id)

        generated_tables = self.generate(run_id, raw_tables)
        published_tables = self.publish(run_id, generated_tables)
        dag_result = self.trigger_dag(run_id, published_tables)

        logger.info("Pipeline completed run_id=%s", run_id)
        return dag_result
