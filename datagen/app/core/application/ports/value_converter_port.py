from abc import ABC, abstractmethod
from typing import Any, List

from app.core.domain.entities import TableColumnSpec


class ValueConverterPort(ABC):
    @abstractmethod
    def convert(self, values: List[Any], table_column: TableColumnSpec) -> List[Any]:
        pass
