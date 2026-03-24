from dataclasses import dataclass
from typing import Any

TERMINAL_STATES = {"success", "failed", "upstream_failed"}
SUCCESS_STATE = "success"


@dataclass
class DagRunState:
    dag_run_id: str
    state: str
    raw: dict[str, Any]

    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def is_success(self) -> bool:
        return self.state == SUCCESS_STATE
