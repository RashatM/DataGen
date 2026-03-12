from abc import ABC, abstractmethod
from typing import List

from app.core.application.dto import DagRunResult, TablePublication


class DagRunnerPort(ABC):

    @abstractmethod
    def trigger_and_wait(
        self,
        run_id: str,
        publications: List[TablePublication],
        timeout_seconds: int,
    ) -> DagRunResult:
        ...
