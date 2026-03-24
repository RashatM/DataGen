from abc import ABC, abstractmethod

from app.core.domain.entities import TableSpec


class TableDependencyPlannerPort(ABC):
    @abstractmethod
    def plan(self, tables: list[TableSpec]) -> list[TableSpec]:
        pass
