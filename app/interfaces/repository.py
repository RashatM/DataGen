from abc import ABC, abstractmethod
from typing import List

from app.dto.mock_data import MockDataSchema


class IMockRepository(ABC):

    @abstractmethod
    def get_entity_schemas(self) -> List[MockDataSchema]:
        pass