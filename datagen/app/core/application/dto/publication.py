from dataclasses import dataclass
from typing import Generic, TypeVar

from app.core.application.constants import EngineName

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
class EngineLoadArtifact:
    ddl_uri: str
    target_table_name: str


@dataclass(slots=True)
class PublicationArtifacts:
    data_uri: str
    engines: EnginePair[EngineLoadArtifact]


@dataclass(slots=True)
class EngineLoadPayload:
    ddl_query: str
    target_table_name: str


@dataclass(slots=True)
class TablePublication:
    schema_name: str
    table_name: str
    run_id: str
    artifacts: PublicationArtifacts
