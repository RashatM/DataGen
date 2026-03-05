from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from app.core.application.dto import TablePublication
from app.core.domain.entities import MockDataEntityResult


class IPublicationRepository(ABC):
    @abstractmethod
    def publish(
        self,
        entity_result: MockDataEntityResult,
        run_id: str,
        ddl_queries: Dict[str, str],
    ) -> TablePublication:
        pass

    @abstractmethod
    def get_latest_run_id(
        self,
        schema_name: str,
        table_name: str,
    ) -> Optional[str]:
        pass

    @abstractmethod
    def read_entity_data(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
    ) -> Dict[str, Any]:
        pass
