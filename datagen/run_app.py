import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.core.application.dto import TablePublication
from app.core.domain.entities import MockDataRun, MockDataRunResult
from app.infrastructure.converters.schema_converter import convert_to_mock_data_entity, convert_to_mock_data_run
from app.infrastructure.dag_payload_mapper import DagPayloadMapper
from app.shared.logger import logger
from app.shared.config import AppConfig, load_app_settings
from app.providers import provide_mock_service, provide_publication_service


def generate_run_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}_{uuid.uuid7()}"


def export_mock_artifacts_to_s3(
    mock_data_run_result: MockDataRunResult,
    config: AppConfig,
) -> Dict[str, Dict[str, Any]]:
    if not config.s3:
        logger.info("S3 export skipped: no targets configured")
        return {}

    dag_payloads_by_env: Dict[str, Dict[str, Any]] = {}
    run_id = mock_data_run_result.run_id

    for env_name, s3_target in config.s3.items():
        if not s3_target.bucket:
            logger.info(
                "S3 bucket not configured for environment=%s, skipping",
                env_name,
            )
            continue

        publication_service = provide_publication_service(run_id=run_id, s3_config=s3_target)
        published_tables: List[TablePublication] = publication_service.publish_entities(
            entity_results=mock_data_run_result.entity_results,
        )

        dag_payloads_by_env[env_name] = DagPayloadMapper.build_payload(
            run_id=run_id,
            table_publications=published_tables,
        )

    logger.info("Uploaded artifacts to S3 for run_id=%s", run_id)
    return dag_payloads_by_env


def run():
    config: AppConfig = load_app_settings()
    run_id = generate_run_id()

    raw_entities = []

    mock_data_run = convert_to_mock_data_run(
        run_id=run_id,
        raw_entities=raw_entities
    )

    mock_service = provide_mock_service()
    mock_data_run_result = mock_service.generate_entity_values(mock_data_run)

    uploaded_artifacts = export_mock_artifacts_to_s3(mock_data_run_result, config)
    _ = uploaded_artifacts


if __name__ == "__main__":
    run()
