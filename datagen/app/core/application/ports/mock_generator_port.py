from abc import ABC, abstractmethod
from typing import Any, Generic, List

from app.core.domain.typevars import TConstraints


class IMockDataGenerator(ABC, Generic[TConstraints]):
    @abstractmethod
    def generate_values(self, total_rows: int, constraints: TConstraints) -> List[Any]:
        pass
