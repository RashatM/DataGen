from abc import ABC, abstractmethod
from typing import Any, Dict, List


class IMockRepository(ABC):
    @abstractmethod
    def create_db_schema(self, schema_name: str):
        pass

    @abstractmethod
    def create_as_table(self, ddl_query: str, full_table_name: str, generated_data: Dict[str, List[Any]]):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass
