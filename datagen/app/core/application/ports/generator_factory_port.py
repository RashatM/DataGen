from abc import ABC, abstractmethod
from typing import Any

from app.core.application.ports.generator_port import IDataGenerator
from app.core.domain.constraints import Constraints
from app.core.domain.enums import DataType


class IDataGeneratorFactory(ABC):
    @abstractmethod
    def register(self, data_type: DataType, data_generator: IDataGenerator[Any]) -> None:
        pass

    @abstractmethod
    def get(self, data_type: DataType) -> IDataGenerator[Constraints]:
        pass
