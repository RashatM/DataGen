from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class DagRunState:
    dag_run_id: str
    state: str
    raw: Dict[str, Any]

    def is_terminal(self) -> bool:
        return self.state in {"success", "failed", "upstream_failed"}

    def is_success(self) -> bool:
        return self.state == "success"
