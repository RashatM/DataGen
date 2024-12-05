from abc import ABC, abstractmethod

from app.dto.mock_data import MockDataEntity


class IQueryBuilderService(ABC):
    @abstractmethod
    def create_ddl(self, entity: MockDataEntity) -> str: pass