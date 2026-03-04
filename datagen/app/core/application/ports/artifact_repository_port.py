from abc import ABC, abstractmethod
from typing import Any, Dict, List


class IArtifactRepository(ABC):
    @abstractmethod
    def save_entity_ddl(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
        table_format: str,
        ddl_query: str,
    ) -> str:
        pass

    @abstractmethod
    def save_entity_data(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
        records: Dict[str, List[Any]],
    ) -> str:
        pass

    @abstractmethod
    def read_entity_data(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
    ) -> Dict[str, List[Any]]:
        pass
