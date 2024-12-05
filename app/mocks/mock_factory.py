from app.enums import DataType
from app.interfaces.mock_factory import IMockFactory
from app.interfaces.mock_generator import IMockDataGenerator


class MockFactory(IMockFactory):

    def __init__(self):
        self.mock_generators = {}

    def register(self, data_type: DataType, mock_generator: IMockDataGenerator) -> None:
        self.mock_generators[data_type] = mock_generator

    def get(self, data_type: DataType) -> IMockDataGenerator:
        if data_type in self.mock_generators:
            return self.mock_generators[data_type]
        else:
            raise ValueError(f"Неизвестный тип данных: {data_type.value}")


