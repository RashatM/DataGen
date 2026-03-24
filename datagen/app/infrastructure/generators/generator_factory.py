from typing import Any, Dict

from app.core.application.ports.generator_factory_port import DataGeneratorFactoryPort
from app.core.application.ports.generator_port import DataGeneratorPort
from app.core.domain.constraints import Constraints
from app.core.domain.enums import DataType
from app.infrastructure.errors import DataGeneratorNotRegisteredError


class DataGeneratorFactory(DataGeneratorFactoryPort):
    def __init__(self):
        self.data_generators: Dict[DataType, DataGeneratorPort[Any]] = {}

    def register(self, data_type: DataType, data_generator: DataGeneratorPort[Any]) -> None:
        self.data_generators[data_type] = data_generator

    def get(self, data_type: DataType) -> DataGeneratorPort[Constraints]:
        if data_type in self.data_generators:
            return self.data_generators[data_type]
        raise DataGeneratorNotRegisteredError(f"Unknown generator data type: {data_type.value}")
