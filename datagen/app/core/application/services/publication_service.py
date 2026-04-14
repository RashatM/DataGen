from typing import Any

from app.core.application.dto.pipeline import ComparisonQuerySpec, TableExecutionSpec
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.publication import EnginePair, RunRetentionCleanupResult, TablePublication
from app.core.application.internal.run_artifact_retention_policy import select_run_ids_to_remove
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

    def cleanup_old_run_artifacts(self, min_retained_runs: int) -> RunRetentionCleanupResult:
        if (
            not isinstance(min_retained_runs, int)
            or isinstance(min_retained_runs, bool)
            or min_retained_runs < 1
        ):
            raise ValueError("min_retained_runs must be a positive integer")

        logger.info(f"Old run artifact cleanup started: min_retained_runs={min_retained_runs}")
        run_ids = self.repository.list_run_ids()
        removed_run_ids = select_run_ids_to_remove(
            run_ids=run_ids,
            protected_run_ids=self.repository.list_pointer_run_ids(),
            min_retained_runs=min_retained_runs,
        )
        removed_object_count = 0
        for run_id in removed_run_ids:
            removed_object_count += self.repository.delete_run_artifacts(run_id)

        cleanup_result = RunRetentionCleanupResult(
            removed_run_ids=removed_run_ids,
            removed_object_count=removed_object_count,
        )
        logger.info(
            f"Old run artifact cleanup completed: removed_runs={len(cleanup_result.removed_run_ids)}, "
            f"removed_objects={cleanup_result.removed_object_count}, min_retained_runs={min_retained_runs}"
        )
        return cleanup_result

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
            f"Artifacts uploaded: table={table.table_name}"
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
