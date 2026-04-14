from abc import ABC, abstractmethod
from typing import Any, Generic

from app.domain.constraints import OutputConstraints
from app.domain.typevars import TConstraints


class DataGeneratorPort(ABC, Generic[TConstraints]):
    """Контракт генератора, который строит исходные значения колонки по её source constraints."""
    @abstractmethod
    def generate_values(
        self,
        total_rows: int,
        constraints: TConstraints,
        output_constraints: OutputConstraints,
    ) -> list[Any]:
        pass
