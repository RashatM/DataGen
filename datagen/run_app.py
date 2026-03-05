import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from app.core.application.dto import TablePublication
from app.core.domain.entities import GeneratedTableData
from app.infrastructure.converters.schema_converter import convert_to_generation_run
from app.infrastructure.dag_payload_mapper import DagPayloadMapper
from app.shared.logger import logger
from app.shared.config import AppConfig, load_app_settings
from app.providers import provide_mock_service, provide_publication_service


def generate_run_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}_{uuid.uuid7()}"


def publish_run_artifacts(
    run_id: str,
    generated_tables: List[GeneratedTableData],
    config: AppConfig,
) -> Dict[str, Dict[str, Any]]:
    if not config.s3:
        logger.info("S3 export skipped: no targets configured")
        return {}

    dag_payloads_by_env: Dict[str, Dict[str, Any]] = {}
    for env_name, s3_config in config.s3.items():
        if not s3_config.bucket:
            logger.info(f"S3 bucket not configured for environment={env_name}, skipping")
            continue

        publication_service = provide_publication_service(run_id=run_id, s3_config=s3_config)
        published_tables: List[TablePublication] = publication_service.publish_entities(
            generated_tables=generated_tables,
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

    raw_tables = []

    generation_run = convert_to_generation_run(
        run_id=run_id,
        raw_tables=raw_tables,
    )

    mock_service = provide_mock_service()
    generated_tables = mock_service.generate_tables_data(generation_run)

    uploaded_artifacts = publish_run_artifacts(
        run_id=run_id,
        generated_tables=generated_tables,
        config=config,
    )
    _ = uploaded_artifacts


if __name__ == "__main__":
    run()
