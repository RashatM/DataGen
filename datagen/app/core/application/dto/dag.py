from dataclasses import dataclass, field
from typing import Any, Dict

from app.core.application.constants import DagRunStatus


@dataclass(slots=True)
class DagRunResult:
    run_id: str
    dag_id: str
    dag_run_id: str
    status: DagRunStatus
    raw_response: Dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        return self.status == DagRunStatus.SUCCESS
