from abc import ABC, abstractmethod
from typing import List

from app.dto.entities import MockSchema


class IMockRepository(ABC):

    @abstractmethod
    def get_entity_schemas(self) -> List[MockSchema]:
        pass