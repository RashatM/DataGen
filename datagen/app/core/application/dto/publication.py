from dataclasses import dataclass
from typing import Generic, TypeVar

from app.core.application.constants import EngineName
from app.core.application.dto.pipeline import TableLoadSpec

T = TypeVar("T")


@dataclass(slots=True)
class EnginePair(Generic[T]):
    """Fixed pair of supported engines: hive and iceberg."""

    hive: T
    iceberg: T

    def get_value(self, engine: EngineName) -> T:
        if engine == EngineName.HIVE:
            return self.hive
        return self.iceberg


@dataclass(slots=True)
class TablePublication:
    table_name: str
    run_id: str
    data_uri: str
    load_spec: TableLoadSpec
