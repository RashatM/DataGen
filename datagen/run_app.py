import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.core.application.use_cases.execute_pipeline import ExecutePipelineUseCase
from app.infrastructure.converters.contract.pipeline_spec_converter import convert_to_pipeline_execution_spec
from app.infrastructure.input.excel_raw_table_loader import load_workbook_specs
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
    raw_tables: list[Any] | None = None,
) -> None:
    logger.info(f"Application started: environment={env_name}")
    config = load_app_settings(env_name)
    s3_client = provide_s3_client(config.s3)
    object_storage = provide_s3_object_storage(
        bucket=config.s3.bucket,
        s3_client=s3_client,
    )
    try:
        if raw_tables is not None:
            raise ValueError("run_app no longer supports legacy raw_tables input")

        workbook_specs = load_workbook_specs(
            input_path=Path(__file__).resolve().parent / "params",
        )
        if len(workbook_specs) != 1:
            raise ValueError(
                f"Exactly one workbook spec is supported, got {len(workbook_specs)}"
            )

        run_id = generate_run_id()
        pipeline_spec = convert_to_pipeline_execution_spec(workbook_specs[0])

        use_case = ExecutePipelineUseCase(
            generation_service=provide_generation_service(),
            artifact_publication_service=provide_artifact_publication_service(object_storage),
            comparison_report_service=provide_comparison_report_service(object_storage),
            execution_runner=provide_execution_runner(config.airflow, object_storage),
            execution_timeout_seconds=config.airflow.dag_timeout_seconds,
        )
        pipeline_result = use_case.execute(run_id=run_id, pipeline_spec=pipeline_spec)
        execution_result = pipeline_result.execution_result
        execution_details = f", execution_id={execution_result.execution_id}"
        if execution_result.execution_url:
            execution_details += f", execution_url={execution_result.execution_url}"

        if execution_result.is_success():
            logger.info(
                f"Application finished: environment={env_name}, "
                f"run_id={pipeline_result.run_id}, status={execution_result.status.value}"
            )
            return

        if execution_result.is_wait_timeout():
            logger.warning(
                f"Application finished without terminal DAG result: environment={env_name}, "
                f"run_id={pipeline_result.run_id}, status={execution_result.status.value}, "
                f"{execution_details.removeprefix(', ')}"
            )
            return

        logger.error(
            f"Application finished with error: environment={env_name}, "
            f"run_id={pipeline_result.run_id}, status={execution_result.status.value}{execution_details}"
        )
    finally:
        object_storage.close()
