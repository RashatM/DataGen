from abc import ABC, abstractmethod

from app.core.domain.entities import MockDataEntity


class IQueryBuilder(ABC):
    @abstractmethod
    def generate_ddl(self, entity: MockDataEntity) -> str:
        pass
