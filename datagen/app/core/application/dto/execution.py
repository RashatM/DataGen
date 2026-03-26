from dataclasses import dataclass

from app.core.application.constants import ExecutionStatus


@dataclass(slots=True)
class ExecutionResult:
    run_id: str
    execution_id: str
    status: ExecutionStatus
    execution_url: str | None = None

    def is_success(self) -> bool:
        return self.status == ExecutionStatus.SUCCESS

    def is_wait_timeout(self) -> bool:
        return self.status == ExecutionStatus.WAIT_TIMEOUT
