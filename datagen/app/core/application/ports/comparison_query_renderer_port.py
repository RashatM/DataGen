from abc import ABC, abstractmethod
from typing import List

from app.core.application.dto.publication import EnginePair, TablePublication


class ComparisonQueryRendererPort(ABC):

    @abstractmethod
    def render(self, publications: List[TablePublication]) -> EnginePair[str]:
        ...
