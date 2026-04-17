from dataclasses import dataclass, field

from app.application.constants import ExecutionStatus
from app.application.dto.comparison import ComparisonReport


@dataclass(slots=True)
class ExecutionDiagnostic:
    """Короткая диагностика failed external execution task, прочитанная из run artifacts."""
    run_id: str
    task_id: str
    message: str
    code: str


@dataclass(slots=True)
class ExecutionResult:
    """Технический результат исполнения внешнего runner-а без доменной интерпретации сверки."""
    run_id: str
    execution_id: str
    status: ExecutionStatus
    execution_url: str | None = None
    failed_task_ids: list[str] = field(default_factory=list)

    def is_success(self) -> bool:
        return self.status == ExecutionStatus.SUCCESS

    def is_wait_timeout(self) -> bool:
        return self.status == ExecutionStatus.WAIT_TIMEOUT


@dataclass(slots=True)
class PipelineExecutionResult:
    """Финальный результат use case-а: статус выполнения пайплайна и, при успехе, comparison-report."""
    run_id: str
    execution_result: ExecutionResult
    comparison_report: ComparisonReport | None = None
    diagnostics: list[ExecutionDiagnostic] = field(default_factory=list)
