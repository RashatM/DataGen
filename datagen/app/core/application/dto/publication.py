from dataclasses import dataclass
from typing import Generic, TypeVar

from app.core.application.constants import EngineName
from app.core.application.dto.pipeline import TableLoadSpec

T = TypeVar("T")


@dataclass(slots=True)
class EnginePair(Generic[T]):
    """Фиксированная пара значений для двух поддерживаемых движков: Hive и Iceberg."""

    hive: T
    iceberg: T

    def get_value(self, engine: EngineName) -> T:
        if engine == EngineName.HIVE:
            return self.hive
        return self.iceberg


@dataclass(slots=True)
class TablePublication:
    """Результат staging-этапа для одной таблицы: где лежит parquet и как его надо загрузить."""
    table_name: str
    run_id: str
    data_uri: str
    load_spec: TableLoadSpec
