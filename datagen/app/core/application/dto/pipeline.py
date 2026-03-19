from dataclasses import dataclass
from typing import Optional

from app.core.application.dto.comparison import ComparisonReport
from app.core.application.dto.dag import DagRunResult


@dataclass(slots=True)
class PipelineExecutionResult:
    run_id: str
    dag_result: DagRunResult
    comparison_report: Optional[ComparisonReport] = None
