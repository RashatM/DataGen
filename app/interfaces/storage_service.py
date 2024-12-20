from abc import ABC, abstractmethod


from app.dto.mock_data import  MockDataEntityResult


class IStorageService(ABC):
    # @abstractmethod
    # def create_table(self, ddl_query: str) -> None:pass
    # @abstractmethod
    # def save_to_source(self, mock_data: MockDataEntityResult) -> None: pass
    @abstractmethod
    def create_and_save_to_source(self, ddl_query: str, mock_data: MockDataEntityResult):
        pass