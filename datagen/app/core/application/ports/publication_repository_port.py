from abc import ABC, abstractmethod
from typing import Any

from app.core.application.dto.pipeline import TableLoadSpec
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.publication import EnginePair, TablePublication
from app.core.domain.entities import GeneratedTableData


class ArtifactPublicationRepositoryPort(ABC):
    """Порт публикации артефактов run-а и управления per-table pointers."""
    @abstractmethod
    def stage_table_artifacts(
        self,
        table_data: GeneratedTableData,
        artifact_layout: RunArtifactKeyLayout,
        load_spec: TableLoadSpec,
    ) -> TablePublication:
        pass

    @abstractmethod
    def stage_comparison_queries(
        self,
        artifact_layout: RunArtifactKeyLayout,
        rendered_queries: EnginePair[str],
    ) -> EnginePair[str]:
        pass

    @abstractmethod
    def commit_pointer(
        self,
        table_name: str,
        run_id: str,
    ) -> None:
        pass

    @abstractmethod
    def get_latest_run_id(
        self,
        table_name: str,
    ) -> str | None:
        pass

    @abstractmethod
    def read_table_data(
        self,
        table_name: str,
        run_id: str,
    ) -> dict[str, Any]:
        pass

    @abstractmethod
    def cleanup_run_artifacts(self, artifact_layout: RunArtifactKeyLayout) -> None:
        pass

    @abstractmethod
    def list_run_ids(self) -> list[str]:
        pass

    @abstractmethod
    def list_pointer_run_ids(self) -> list[str]:
        pass

    @abstractmethod
    def delete_run_artifacts(self, run_id: str) -> int:
        pass
