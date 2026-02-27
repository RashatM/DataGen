from abc import ABC, abstractmethod
from typing import Any

from app.core.application.ports.mock_generator_port import IMockDataGenerator
from app.core.domain.constraints import Constraints
from app.core.domain.enums import DataType


class IMockFactory(ABC):
    @abstractmethod
    def register(self, data_type: DataType, mock_generator: IMockDataGenerator[Any]) -> None:
        pass

    @abstractmethod
    def get(self, data_type: DataType) -> IMockDataGenerator[Constraints]:
        pass
