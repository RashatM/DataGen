from dataclasses import dataclass

from app.core.application.constants import ExecutionStatus


@dataclass(slots=True)
class ExecutionResult:
    run_id: str
    execution_id: str
    status: ExecutionStatus

    def is_success(self) -> bool:
        return self.status == ExecutionStatus.SUCCESS
