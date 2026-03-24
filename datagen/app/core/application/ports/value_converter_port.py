from abc import ABC, abstractmethod
from typing import Any

from app.core.domain.entities import TableColumnSpec


class ValueConverterPort(ABC):
    @abstractmethod
    def convert(self, values: list[Any], table_column: TableColumnSpec) -> list[Any]:
        pass
