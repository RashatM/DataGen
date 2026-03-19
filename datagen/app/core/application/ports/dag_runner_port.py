from abc import ABC, abstractmethod
from typing import Dict, List

from app.core.application.dto import DagRunResult, RunArtifactLayout, TablePublication


class DagRunnerPort(ABC):

    @abstractmethod
    def trigger_and_wait(
        self,
        layout: RunArtifactLayout,
        publications: List[TablePublication],
        comparison_query_uris: Dict[str, str],
        timeout_seconds: int,
    ) -> DagRunResult:
        ...
