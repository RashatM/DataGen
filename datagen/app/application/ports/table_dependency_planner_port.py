from abc import ABC, abstractmethod

from app.domain.entities import TableSpec


class TableDependencyPlannerPort(ABC):
    """Порт планировщика, который упорядочивает таблицы с учётом FK-зависимостей."""
    @abstractmethod
    def plan(self, tables: list[TableSpec]) -> list[TableSpec]:
        pass
