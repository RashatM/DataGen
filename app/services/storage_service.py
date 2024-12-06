from app.dto.mock_data import MockDataEntityResult
from app.interfaces.repository import IMockRepository
from app.interfaces.storage_service import IStorageService


class MockStorageService(IStorageService):
    def __init__(self, mock_repository: IMockRepository):
        self.mock_repository = mock_repository

    # def create_table(self, ddl_query: str) -> None:
    #     pass
    #
    # def save_to_source(self, mock_data: MockDataEntityResult) -> None:
    #     pass

    def create_and_save_to_source(self, ddl_query: str, mock_data: MockDataEntityResult):
        self.mock_repository.create_and_save(
            ddl_query=ddl_query,
            full_table_name=mock_data.entity.full_table_name,
            generated_data=mock_data.generated_data
        )