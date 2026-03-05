from datetime import datetime, timezone
from typing import Any, Dict, List

from app.core.application.dto import TablePublication
from app.core.domain.entities import MockDataEntityResult
from app.infrastructure.converters.schema_converter import convert_to_mock_data_entity
from app.infrastructure.dag_payload_mapper import DagPayloadMapper
from app.shared.logger import logger
from app.shared.settings import AppSettings, load_app_settings
from app.providers import (
    provide_mock_factory,
    provide_mock_service,
    provide_publication_repository,
    provide_publication_service,
    provide_s3_client,
    provide_s3_object_storage,
)


def create_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def resolve_run_id(config: AppSettings) -> str:
    return config.s3.run_id or create_run_id()


def export_mock_artifacts_to_s3(
    mock_results: List[MockDataEntityResult],
    config: AppSettings,
) -> Dict[str, Dict[str, Any]]:
    if not config.s3.enabled:
        logger.info("S3 export disabled by configuration")
        return {}

    run_id = resolve_run_id(config)
    dag_payloads_by_env: Dict[str, Dict[str, Any]] = {}

    for s3_target in config.s3.targets:
        if not s3_target.bucket:
            logger.info(
                "S3 bucket not configured for environment=%s, skipping",
                s3_target.name,
            )
            continue


        publication_service = provide_publication_service(s3_target)
        published_tables: List[TablePublication] = []

        for mock_result in mock_results:
            table_publication: TablePublication = publication_service.publish(
                entity_result=mock_result,
                run_id=run_id,
            )
            published_tables.append(table_publication)

        dag_payloads_by_env[s3_target.name] = DagPayloadMapper.build_payload(
            run_id=run_id,
            table_publications=published_tables,
        )

    logger.info("Uploaded artifacts to S3 for run_id=%s", run_id)
    return dag_payloads_by_env


def run():
    config: AppSettings = load_app_settings()

    # Пример данных
    raw_entities = []

    entities = [convert_to_mock_data_entity(e) for e in raw_entities]

    mock_factory = provide_mock_factory()
    mock_service = provide_mock_service(mock_factory)
    mock_results = mock_service.generate_entity_values(entities)

    uploaded_artifacts = export_mock_artifacts_to_s3(mock_results, config)
    _ = uploaded_artifacts


if __name__ == "__main__":
    run()
