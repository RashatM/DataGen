from abc import ABC, abstractmethod

from app.core.domain.entities import TableSpec


class IQueryBuilder(ABC):
    @abstractmethod
    def generate_ddl(self, table: TableSpec) -> str:
        pass
