from datetime import datetime, timezone
from typing import Dict, List

from app.core.application.dto import MockEntityArtifacts
from app.core.domain.entities import MockDataEntityResult
from app.infrastructure.converters.schema_converter import convert_to_mock_data_entity
from app.shared.logger import logger
from app.shared.settings import AppSettings, load_app_settings
from app.providers import (
    provide_mock_factory,
    provide_mock_service,
    provide_s3_client,
    provide_s3_object_storage,
    provide_mock_artifact_repository,
    provide_mock_artifact_service,
    provide_run_state_repository,
)


def create_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def resolve_run_id(config: AppSettings) -> str:
    return config.s3.run_id or create_run_id()


def export_mock_artifacts_to_s3(
    mock_results: List[MockDataEntityResult],
    config: AppSettings,
) -> Dict[str, Dict[str, MockEntityArtifacts]]:
    if not config.s3.enabled:
        logger.info("S3 export disabled by configuration")
        return {}

    run_id = resolve_run_id(config)
    uploaded_artifacts_by_env: Dict[str, Dict[str, MockEntityArtifacts]] = {}

    for s3_target in config.s3.targets:
        if not s3_target.bucket:
            logger.info(
                "S3 bucket not configured for environment=%s, skipping",
                s3_target.name,
            )
            continue

        s3_client = provide_s3_client(s3_target)
        object_storage = provide_s3_object_storage(
            bucket=s3_target.bucket,
            prefix=s3_target.prefix,
            s3_client=s3_client,
        )

        artifact_repository = provide_mock_artifact_repository(object_storage)
        run_state_repository = provide_run_state_repository(object_storage)
        artifact_service = provide_mock_artifact_service(artifact_repository, run_state_repository)

        uploaded_tables: Dict[str, MockEntityArtifacts] = {}

        for mock_result in mock_results:
            table_artifacts: MockEntityArtifacts = artifact_service.persist(
                entity_result=mock_result,
                run_id=run_id,
            )
            uploaded_tables[mock_result.entity.full_table_name] = table_artifacts

        uploaded_artifacts_by_env[s3_target.name] = uploaded_tables

    logger.info("Uploaded artifacts to S3 for run_id=%s", run_id)
    return uploaded_artifacts_by_env


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