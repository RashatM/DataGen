from abc import ABC, abstractmethod

from app.enums import DataType
from app.interfaces.mock_generator import IMockDataGenerator


class IMockFactory(ABC):
    @abstractmethod
    def register(self, data_type: DataType, mock_generator: IMockDataGenerator) -> None:
        pass


    @abstractmethod
    def get(self, data_type: DataType) -> IMockDataGenerator:
        pass
