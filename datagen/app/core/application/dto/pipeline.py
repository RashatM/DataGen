from dataclasses import dataclass
from typing import Optional

from app.core.application.dto.comparison import ComparisonReport
from app.core.application.dto.execution import ExecutionResult


@dataclass(slots=True)
class PipelineExecutionResult:
    run_id: str
    execution_result: ExecutionResult
    comparison_report: Optional[ComparisonReport] = None
