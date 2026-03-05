from abc import ABC, abstractmethod
from typing import List

from app.core.domain.entities import TableSpec


class IDependencyGraphBuilder(ABC):
    @abstractmethod
    def build_graph(self, tables: List[TableSpec]) -> List[TableSpec]:
        pass
