from abc import ABC, abstractmethod
from typing import List, Dict, Any



class IMockRepository(ABC):
    @abstractmethod
    def create_db_schema(self, schema_name: str):pass

    @abstractmethod
    def create_and_save(self, ddl_query: str, full_table_name: str , generated_data: Dict[str, List[Any]]):
        pass