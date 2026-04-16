from abc import ABC, abstractmethod

from app.application.dto.pipeline import ComparisonQuerySpec
from app.application.layouts.storage_layout import RunArtifactKeyLayout
from app.application.dto.execution_result import ExecutionResult
from app.application.dto.publication import EnginePair, TablePublication


class ExecutionRunnerPort(ABC):
    """Порт запуска и ожидания внешнего runtime, который materializes load и compare стадии."""

    @abstractmethod
    def trigger_and_wait(
        self,
        artifact_layout: RunArtifactKeyLayout,
        publications: list[TablePublication],
        comparison_spec: ComparisonQuerySpec,
        comparison_query_uris: EnginePair[str],
        timeout_seconds: int,
    ) -> ExecutionResult:
        ...
