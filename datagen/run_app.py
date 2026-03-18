import time
import uuid
from datetime import datetime
from typing import Any, List
from zoneinfo import ZoneInfo

from app.core.application.constants import DagRunStatus
from app.core.application.dto import DagRunResult, TablePublication
from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.core.application.services.publication_service import ArtifactPublicationService
from app.core.application.services.generation_service import DataGenerationService
from app.core.domain.entities import GeneratedTableData
from app.infrastructure.converters.schema_converter import convert_to_generation_run
from app.providers import provide_artifact_publication_service, provide_dag_runner, provide_generation_service
from app.shared.config import load_app_settings
from app.shared.logger import pipeline_logger

logger = pipeline_logger


class DataPipelineExecutor:

    def __init__(
        self,
        generation_service: DataGenerationService,
        artifact_publication_service: ArtifactPublicationService,
        dag_runner: DagRunnerPort,
        dag_timeout_seconds: int,
    ) -> None:
        self.generation_service = generation_service
        self.artifact_publication_service = artifact_publication_service
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
        generation_run = convert_to_generation_run(run_id=run_id, raw_tables=raw_tables)
        return self.generation_service.generate_table_data(generation_run)

    def publish(
        self,
        run_id: str,
        generated_tables: List[GeneratedTableData],
    ) -> List[TablePublication]:
        return self.artifact_publication_service.publish(
            run_id=run_id,
            generated_tables=generated_tables,
        )

    def trigger_dag(
        self,
        run_id: str,
        publications: List[TablePublication],
    ) -> DagRunResult:
        return self.dag_runner.trigger_and_wait(
            run_id=run_id,
            publications=publications,
            timeout_seconds=self.dag_timeout_seconds,
        )

    def execute(self, raw_tables: List[Any]) -> DagRunResult:
        run_id = self.generate_run_id()
        start = time.monotonic()
        logger.info(f"Pipeline started: run_id={run_id}")

        generated_tables = self.generate(run_id, raw_tables)
        publications = self.publish(run_id, generated_tables)
        dag_result = self.trigger_dag(run_id, publications)

        total = int(time.monotonic() - start)

        if dag_result.status == DagRunStatus.SUCCESS:
            logger.info(
                f"Pipeline completed: run_id={run_id}, "
                f"status={dag_result.status.value}, total={total}s"
            )
        else:
            logger.error(
                f"Pipeline failed: run_id={run_id}, "
                f"status={dag_result.status.value}, total={total}s"
            )

        return dag_result


def run_app(env_name: str, raw_tables: List[Any]) -> None:
    logger.info(f"Application started: environment={env_name}")
    config = load_app_settings(env_name)

    pipeline = DataPipelineExecutor(
        generation_service=provide_generation_service(),
        artifact_publication_service=provide_artifact_publication_service(config.s3, config.target_storage),
        dag_runner=provide_dag_runner(config.airflow),
        dag_timeout_seconds=config.airflow.dag_timeout_seconds,
    )
    dag_result = pipeline.execute(raw_tables)

    if dag_result.status == DagRunStatus.SUCCESS:
        logger.info(
            f"Application finished: environment={env_name}, "
            f"status={dag_result.status.value}"
        )
    else:
        logger.error(
            f"Application finished with error: environment={env_name}, "
            f"status={dag_result.status.value}"
        )
