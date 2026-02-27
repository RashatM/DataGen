from abc import ABC, abstractmethod
from typing import List

from app.core.domain.entities import MockDataEntity


class IDependencyGraphBuilder(ABC):
    @abstractmethod
    def build_graph(self, entities: List[MockDataEntity]) -> List[MockDataEntity]:
        pass
