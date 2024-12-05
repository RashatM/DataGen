from app.dto.mock_data import MockDataEntityResult
from app.interfaces.storage_service import IStorageService


class StorageService(IStorageService):
    def create_table(self, ddl_query: str) -> None:
        pass

    def save_to_source(self, mock_data: MockDataEntityResult) -> None:
        pass