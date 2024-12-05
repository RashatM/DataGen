from abc import ABC, abstractmethod


from app.dto.entities import  MockEntityResult


class IStorageService(ABC):
    @abstractmethod
    def create_table(self, ddl_query: str) -> None:pass
    @abstractmethod
    def save_to_source(self, mock_data: MockEntityResult) -> None: pass
