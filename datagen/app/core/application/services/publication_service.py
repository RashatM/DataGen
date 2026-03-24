from typing import Any

from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.publication import EngineLoadPayload, EnginePair, TablePublication
from app.core.application.dto.run_artifacts import ArtifactPublicationResult
from app.core.application.ports.comparison_query_renderer_port import ComparisonQueryRendererPort
from app.core.application.ports.publication_repository_port import ArtifactPublicationRepositoryPort
from app.core.application.ports.table_load_payload_builder_port import TableLoadPayloadBuilderPort
from app.core.domain.entities import GeneratedTableData
from app.shared.logger import publication_logger

logger = publication_logger


class ArtifactPublicationService:
    def __init__(
            self,
            repository: ArtifactPublicationRepositoryPort,
            hive_load_payload_builder: TableLoadPayloadBuilderPort,
            iceberg_load_payload_builder: TableLoadPayloadBuilderPort,
            comparison_query_renderer: ComparisonQueryRendererPort,
    ):
        self.repository = repository
        self.hive_load_payload_builder = hive_load_payload_builder
        self.iceberg_load_payload_builder = iceberg_load_payload_builder
        self.comparison_query_renderer = comparison_query_renderer

    def build_engine_load_payloads(self, table_data: GeneratedTableData) -> EnginePair[EngineLoadPayload]:
        return EnginePair(
            hive=self.hive_load_payload_builder.build_load_payload(table_data.table),
            iceberg=self.iceberg_load_payload_builder.build_load_payload(table_data.table),
        )

    def cleanup_run_artifacts(self, artifact_layout: RunArtifactKeyLayout) -> None:
        logger.info(f"Artifact cleanup started: run_id={artifact_layout.run_id}")
        self.repository.cleanup_run_artifacts(artifact_layout=artifact_layout)
        logger.info(f"Artifact cleanup completed: run_id={artifact_layout.run_id}")

    def read_latest_table_data(
            self,
            schema_name: str,
            table_name: str,
    ) -> dict[str, Any] | None:
        latest_staged_run_id = self.repository.get_latest_run_id(
            schema_name=schema_name,
            table_name=table_name,
        )
        if not latest_staged_run_id:
            return None

        return self.repository.read_table_data(
            schema_name=schema_name,
            table_name=table_name,
            run_id=latest_staged_run_id,
        )

    def stage_tables(
            self,
            artifact_layout: RunArtifactKeyLayout,
            generated_tables: list[GeneratedTableData],
    ) -> list[TablePublication]:
        table_publications = []

        for table_data in generated_tables:
            table_publication = self.repository.stage_table_artifacts(
                table_data=table_data,
                artifact_layout=artifact_layout,
                engine_load_payloads=self.build_engine_load_payloads(table_data),
            )
            table_publications.append(table_publication)
            table = table_data.table
            logger.info(
                f"Artifacts uploaded: table={table.full_table_name}, rows={table.total_rows}, columns={len(table.columns)}"
            )

        return table_publications

    def stage_comparison_queries(
        self,
        artifact_layout: RunArtifactKeyLayout,
        table_publications: list[TablePublication]
    ) -> EnginePair[str]:
        rendered_queries = self.comparison_query_renderer.render(table_publications)
        comparison_query_uris = self.repository.stage_comparison_queries(
            artifact_layout=artifact_layout,
            rendered_queries=rendered_queries,
        )
        logger.info(f"Comparison queries uploaded: run_id={artifact_layout.run_id}")
        return comparison_query_uris

    def publish(
            self,
            artifact_layout: RunArtifactKeyLayout,
            generated_tables: list[GeneratedTableData]
    ) -> ArtifactPublicationResult:
        try:
            table_publications = self.stage_tables(artifact_layout, generated_tables)
            comparison_query_uris = self.stage_comparison_queries(
                artifact_layout=artifact_layout,
                table_publications=table_publications
            )
        except Exception:
            logger.exception(f"Artifact upload failed: run_id={artifact_layout.run_id}")
            self.cleanup_run_artifacts(artifact_layout=artifact_layout)
            raise

        for publication in table_publications:
            self.repository.commit_pointer(
                schema_name=publication.schema_name,
                table_name=publication.table_name,
                run_id=publication.run_id,
            )
            logger.info(f"Pointer updated: table={publication.schema_name}.{publication.table_name}, run_id={publication.run_id}")

        return ArtifactPublicationResult(
            table_publications=table_publications,
            comparison_query_uris=comparison_query_uris,
        )
