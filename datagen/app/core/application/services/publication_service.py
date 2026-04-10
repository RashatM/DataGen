from typing import Any

from app.core.application.dto.pipeline import ComparisonQuerySpec, TableExecutionSpec
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.publication import EnginePair, TablePublication
from app.core.application.ports.publication_repository_port import ArtifactPublicationRepositoryPort
from app.core.domain.entities import GeneratedTableData
from app.shared.logger import publication_logger

logger = publication_logger


class ArtifactPublicationService:
    """Application-сервис публикации parquet-артефактов, comparison SQL и per-table pointers."""
    def __init__(
            self,
            repository: ArtifactPublicationRepositoryPort,
    ):
        self.repository = repository

    def cleanup_run_artifacts(self, artifact_layout: RunArtifactKeyLayout) -> None:
        logger.info(f"Artifact cleanup started: run_id={artifact_layout.run_id}")
        self.repository.cleanup_run_artifacts(artifact_layout=artifact_layout)
        logger.info(f"Artifact cleanup completed: run_id={artifact_layout.run_id}")

    def read_latest_table_data(
            self,
            table_name: str,
    ) -> dict[str, Any] | None:
        latest_run_id = self.repository.get_latest_run_id(
            table_name=table_name,
        )
        if not latest_run_id:
            return None

        return self.repository.read_table_data(
            table_name=table_name,
            run_id=latest_run_id,
        )

    def stage_table(
        self,
        artifact_layout: RunArtifactKeyLayout,
        table_execution: TableExecutionSpec,
        table_data: GeneratedTableData,
    ) -> TablePublication:
        """Публикует одну таблицу и возвращает publication-объект для построения runtime-contract."""
        table_publication = self.repository.stage_table_artifacts(
            table_data=table_data,
            artifact_layout=artifact_layout,
            load_spec=table_execution.load_spec,
        )
        table = table_data.table
        logger.info(
            f"Artifacts uploaded: table={table.table_name}, rows={table.total_rows}, columns={len(table.columns)}"
        )
        return table_publication

    def stage_comparison_queries(
        self,
        artifact_layout: RunArtifactKeyLayout,
        comparison_spec: ComparisonQuerySpec,
    ) -> EnginePair[str]:
        comparison_query_uris = self.repository.stage_comparison_queries(
            artifact_layout=artifact_layout,
            rendered_queries=EnginePair(
                hive=comparison_spec.hive_sql,
                iceberg=comparison_spec.iceberg_sql,
            ),
        )
        logger.info(f"Comparison queries uploaded: run_id={artifact_layout.run_id}")
        return comparison_query_uris

    def commit_pointers(self, table_publications: list[TablePublication]) -> None:
        """Обновляет per-table pointers после технически успешного выполнения внешнего runtime."""
        for publication in table_publications:
            self.repository.commit_pointer(
                table_name=publication.table_name,
                run_id=publication.run_id,
            )
            logger.info(
                f"Pointer updated: "
                f"table={publication.table_name}, run_id={publication.run_id}"
            )
