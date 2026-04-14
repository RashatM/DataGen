from abc import ABC, abstractmethod
from typing import Any

from app.domain.entities import TableColumnSpec


class ValueConverterPort(ABC):
    """Порт преобразования исходных значений генератора в итоговый output type колонки."""
    @abstractmethod
    def convert(self, values: list[Any], table_column: TableColumnSpec) -> list[Any]:
        pass
