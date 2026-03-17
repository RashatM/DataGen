from abc import ABC, abstractmethod

from app.core.domain.entities import TableSpec


class IQueryBuilder(ABC):
    @abstractmethod
    def build_target_table_name(self, table: TableSpec) -> str:
        pass

    @abstractmethod
    def generate_table_ddl(self, table: TableSpec, target_table_name: str) -> str:
        pass
