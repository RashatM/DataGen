from abc import ABC, abstractmethod
from typing import Dict, List

from app.core.application.dto.publication import TablePublication


class ComparisonQueryRendererPort(ABC):

    @abstractmethod
    def render(self, publications: List[TablePublication]) -> Dict[str, str]:
        ...
