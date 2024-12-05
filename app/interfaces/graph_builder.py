from abc import ABC, abstractmethod
from typing import List


from app.dto.entities import MockEntity


class IDependencyGraphBuilder(ABC):

    @abstractmethod
    def build_graph(self, entities: List[MockEntity]) -> List[MockEntity]:
        pass