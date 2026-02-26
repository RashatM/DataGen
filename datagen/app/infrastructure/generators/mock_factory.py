from typing import Any, Dict

from app.core.application.ports.mock_factory_port import IMockFactory
from app.core.application.ports.mock_generator_port import IMockDataGenerator
from app.core.domain.constraints import Constraints
from app.core.domain.enums import DataType


class MockFactory(IMockFactory):
    def __init__(self):
        self.mock_generators: Dict[DataType, IMockDataGenerator[Any]] = {}

    def register(self, data_type: DataType, mock_generator: IMockDataGenerator[Any]) -> None:
        self.mock_generators[data_type] = mock_generator

    def get(self, data_type: DataType) -> IMockDataGenerator[Constraints]:
        if data_type in self.mock_generators:
            return self.mock_generators[data_type]
        raise ValueError(f"Unknown generator data type: {data_type.value}")
