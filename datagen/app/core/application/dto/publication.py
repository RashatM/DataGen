from dataclasses import dataclass
from typing import Dict


@dataclass(slots=True)
class EngineLoadArtifact:
    ddl_uri: str
    target_table_name: str


@dataclass(slots=True)
class PublicationArtifacts:
    data_uri: str
    engines: Dict[str, EngineLoadArtifact]


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
