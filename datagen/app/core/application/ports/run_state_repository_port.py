from abc import ABC, abstractmethod
from typing import Optional


class IRunStateRepository(ABC):
    @abstractmethod
    def save_latest_run_pointer(
            self,
            schema_name: str,
            table_name: str,
            run_id: str,
    ) -> str:
        pass

    @abstractmethod
    def get_latest_run_id(
            self,
            schema_name: str,
            table_name: str,
    ) -> Optional[str]:
        pass
