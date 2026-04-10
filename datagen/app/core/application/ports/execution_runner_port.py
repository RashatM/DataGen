from abc import ABC, abstractmethod

from app.core.application.dto.pipeline import ComparisonQuerySpec
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.execution_result import ExecutionResult
from app.core.application.dto.publication import EnginePair, TablePublication


class ExecutionRunnerPort(ABC):

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
