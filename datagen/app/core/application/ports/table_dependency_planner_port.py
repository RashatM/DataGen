from abc import ABC, abstractmethod
from typing import List

from app.core.domain.entities import TableSpec


class ITableDependencyPlanner(ABC):
    @abstractmethod
    def plan(self, tables: List[TableSpec]) -> List[TableSpec]:
        pass
