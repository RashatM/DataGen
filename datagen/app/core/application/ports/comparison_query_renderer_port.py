from abc import ABC, abstractmethod

from app.core.application.dto.publication import EnginePair, TablePublication


class ComparisonQueryRendererPort(ABC):

    @abstractmethod
    def render(self, publications: list[TablePublication]) -> EnginePair[str]:
        ...
