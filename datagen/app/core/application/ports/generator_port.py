from abc import ABC, abstractmethod
from typing import Any, Generic, List

from app.core.domain.constraints import OutputConstraints
from app.core.domain.typevars import TConstraints


class DataGeneratorPort(ABC, Generic[TConstraints]):
    @abstractmethod
    def generate_values(
        self,
        total_rows: int,
        constraints: TConstraints,
        output_constraints: OutputConstraints,
    ) -> List[Any]:
        pass
