from abc import ABC, abstractmethod

from app.application.dto.execution_result import ExecutionDiagnostic


class ExecutionDiagnosticRepositoryPort(ABC):
    """Порт чтения diagnostics artifacts failed external execution."""

    @abstractmethod
    def load_diagnostics(self, diagnostics_prefix: str, expected_run_id: str) -> list[ExecutionDiagnostic]:
        pass
