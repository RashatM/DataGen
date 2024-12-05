from abc import ABC, abstractmethod
from typing import Any, List

from app.dto.entities import MockEntity, MockColumn, MockEntityResult


class IMockDataService(ABC):
    @abstractmethod
    def generate_column_values(self, total_rows: int, entity_column: MockColumn) -> List[Any]: pass
    @abstractmethod
    def generate_entity_values(self, entities: List[MockEntity]) -> List[MockEntityResult]: pass
