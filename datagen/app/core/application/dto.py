from dataclasses import dataclass, field
from typing import Any, Dict

from app.core.application.constants import DagRunStatus


@dataclass(slots=True)
class EngineLoadSpec:
    ddl_uri: str
    target_table_name: str


@dataclass(slots=True)
class PublicationArtifacts:
    data_uri: str
    engines: Dict[str, EngineLoadSpec]


@dataclass(slots=True)
class EngineArtifactDraft:
    ddl_query: str
    target_table_name: str


@dataclass(slots=True)
class TablePublication:
    schema_name: str
    table_name: str
    run_id: str
    storage_type: str
    artifacts: PublicationArtifacts


@dataclass
class DagRunResult:
    run_id: str
    dag_id: str
    status: DagRunStatus
    raw_response: Dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        return self.status == DagRunStatus.SUCCESS
