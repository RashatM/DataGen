from abc import ABC, abstractmethod
from typing import Any, Generic

from app.core.domain.enums import DataType
from app.core.domain.typevars import TConstraints


class SourceValueConverter(ABC, Generic[TConstraints]):
    """Контракт внутреннего конвертера, который отвечает за один конкретный source data type."""

    @property
    @abstractmethod
    def source_type(self) -> DataType:
        pass

    @abstractmethod
    def convert(
        self,
        values: list[Any],
        constraints: TConstraints,
        target_type: DataType,
        column_name: str,
    ) -> list[Any]:
        pass
