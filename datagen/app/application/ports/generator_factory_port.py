from abc import ABC, abstractmethod
from typing import Any

from app.application.ports.generator_port import DataGeneratorPort
from app.domain.constraints import Constraints
from app.domain.enums import DataType


class DataGeneratorFactoryPort(ABC):
    """Порт registry/factory для генераторов source data types."""
    @abstractmethod
    def register(self, data_type: DataType, data_generator: DataGeneratorPort[Any]) -> None:
        pass

    @abstractmethod
    def get(self, data_type: DataType) -> DataGeneratorPort[Constraints]:
        pass
