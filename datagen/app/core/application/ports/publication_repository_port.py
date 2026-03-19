from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from app.core.application.layouts.storage_layout import RunArtifactLayout
from app.core.application.dto.publication import EngineLoadPayload, TablePublication
from app.core.domain.entities import GeneratedTableData


class IArtifactPublicationRepository(ABC):
    @abstractmethod
    def stage_table_artifacts(
        self,
        table_data: GeneratedTableData,
        layout: RunArtifactLayout,
        engine_load_payloads: Dict[str, EngineLoadPayload],
    ) -> TablePublication:
        pass

    @abstractmethod
    def stage_comparison_queries(
        self,
        layout: RunArtifactLayout,
        rendered_queries: Dict[str, str],
    ) -> Dict[str, str]:
        pass

    @abstractmethod
    def commit_pointer(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
    ) -> None:
        pass

    @abstractmethod
    def get_latest_run_id(
        self,
        schema_name: str,
        table_name: str,
    ) -> Optional[str]:
        pass

    @abstractmethod
    def read_table_data(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def cleanup_run_artifacts(self, layout: RunArtifactLayout) -> None:
        pass
