from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from app.core.application.dto import TablePublication
from app.core.domain.entities import GeneratedTableData


class IPublicationRepository(ABC):
    @abstractmethod
    def stage_artifacts(
        self,
        table_data: GeneratedTableData,
        run_id: str,
        ddl_queries: Dict[str, str],
    ) -> TablePublication:
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
    def cleanup_run_artifacts(self, run_id: str) -> None:
        pass
