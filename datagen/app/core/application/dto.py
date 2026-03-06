from dataclasses import asdict, dataclass, field
from typing import Any, Dict

from app.core.application.constants import DagRunStatus


@dataclass(slots=True)
class TablePublication:
    storage_type: str
    schema_name: str
    table_name: str
    run_id: str
    storage: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DagRunResult:
    run_id: str
    dag_id: str
    status: DagRunStatus
    raw_response: Dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        return self.status == DagRunStatus.SUCCESS
