from abc import ABC, abstractmethod

from app.dto.entities import MockEntity


class IQueryBuilderService(ABC):
    @abstractmethod
    def create_ddl(self, entity: MockEntity) -> str: pass