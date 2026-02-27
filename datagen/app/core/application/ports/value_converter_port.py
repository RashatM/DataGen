from abc import ABC, abstractmethod
from typing import Any, Generic, List

from app.core.domain.entities import MockDataColumn
from app.core.domain.enums import DataType
from app.core.domain.typevars import TConstraints


class IValueConverter(ABC):
    @abstractmethod
    def convert(self, values: List[Any], column: MockDataColumn) -> List[Any]:
        pass


class ISourceValueConverter(ABC, Generic[TConstraints]):
    @property
    @abstractmethod
    def source_type(self) -> DataType:
        pass

    @abstractmethod
    def convert(
        self,
        values: List[Any],
        constraints: TConstraints,
        target_type: DataType,
        column_name: str,
    ) -> List[Any]:
        pass
