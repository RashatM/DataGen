from abc import ABC, abstractmethod
from typing import Any, List

from app.dto.mock_data import MockDataEntity, MockDataColumn, MockDataEntityResult


class IMockDataService(ABC):
    @abstractmethod
    def generate_column_values(self, total_rows: int, entity_column: MockDataColumn) -> List[Any]: pass
    @abstractmethod
    def generate_entity_values(self, entities: List[MockDataEntity]) -> List[MockDataEntityResult]: pass
