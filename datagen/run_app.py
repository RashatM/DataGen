import uuid
from datetime import datetime
from typing import Any, List
from zoneinfo import ZoneInfo

from app.core.application.use_cases.execute_pipeline import ExecutePipelineUseCase
from app.infrastructure.converters.schema_converter import convert_to_generation_run
from app.providers import (
    provide_artifact_publication_service,
    provide_comparison_report_service,
    provide_execution_runner,
    provide_generation_service,
    provide_s3_client,
    provide_s3_object_storage,
)
from app.shared.config import load_app_settings
from app.shared.logger import pipeline_logger

logger = pipeline_logger


def generate_run_id() -> str:
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    short_uid = uuid.uuid4().hex[:8]
    return f"{now:%Y%m%d_%H%M%S}_{short_uid}"


def run_app(
    env_name: str,
    raw_tables: List[Any],
) -> None:
    logger.info(f"Application started: environment={env_name}")
    config = load_app_settings(env_name)
    s3_client = provide_s3_client(config.s3)
    object_storage = provide_s3_object_storage(
        bucket=config.s3.bucket,
        s3_client=s3_client,
    )
    generation_run = convert_to_generation_run(
        run_id=generate_run_id(),
        raw_tables=raw_tables,
    )

    use_case = ExecutePipelineUseCase(
        generation_service=provide_generation_service(),
        artifact_publication_service=provide_artifact_publication_service(object_storage, config.target_storage),
        comparison_report_service=provide_comparison_report_service(object_storage),
        execution_runner=provide_execution_runner(config.airflow, object_storage),
        execution_timeout_seconds=config.airflow.dag_timeout_seconds,
    )
    pipeline_result = use_case.execute(generation_run=generation_run)
    execution_result = pipeline_result.execution_result

    if execution_result.is_success():
        logger.info(
            f"Application finished: environment={env_name}, "
            f"run_id={pipeline_result.run_id}, status={execution_result.status.value}"
        )
        return

    logger.error(
        f"Application finished with error: environment={env_name}, "
        f"run_id={pipeline_result.run_id}, status={execution_result.status.value}"
    )
