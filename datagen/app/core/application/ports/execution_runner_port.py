from abc import ABC, abstractmethod
from typing import Dict, List

from app.core.application.layouts.storage_layout import RunArtifactLayout
from app.core.application.dto.execution import ExecutionResult
from app.core.application.dto.publication import TablePublication


class ExecutionRunnerPort(ABC):

    @abstractmethod
    def trigger_and_wait(
        self,
        layout: RunArtifactLayout,
        publications: List[TablePublication],
        comparison_query_uris: Dict[str, str],
        timeout_seconds: int,
    ) -> ExecutionResult:
        ...
