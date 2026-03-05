from typing import Any, Dict

from app.core.application.ports.generator_factory_port import IDataGeneratorFactory
from app.core.application.ports.generator_port import IDataGenerator
from app.core.domain.constraints import Constraints
from app.core.domain.enums import DataType
from app.infrastructure.errors import DataGeneratorNotRegisteredError


class DataGeneratorFactory(IDataGeneratorFactory):
    def __init__(self):
        self.data_generators: Dict[DataType, IDataGenerator[Any]] = {}

    def register(self, data_type: DataType, data_generator: IDataGenerator[Any]) -> None:
        self.data_generators[data_type] = data_generator

    def get(self, data_type: DataType) -> IDataGenerator[Constraints]:
        if data_type in self.data_generators:
            return self.data_generators[data_type]
        raise DataGeneratorNotRegisteredError(f"Unknown generator data type: {data_type.value}")
