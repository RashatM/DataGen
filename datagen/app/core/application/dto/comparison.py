from dataclasses import dataclass

from app.core.application.constants import ComparisonStatus


@dataclass(slots=True)
class EngineCountSummary:
    hive: int
    iceberg: int


@dataclass(slots=True)
class ComparisonReportArtifacts:
    hive_result_uri: str
    iceberg_result_uri: str


@dataclass(slots=True)
class ComparisonReportSummary:
    row_count: EngineCountSummary
    unmatched_row_count: EngineCountSummary


@dataclass(slots=True)
class ComparisonReport:
    run_id: str
    checked_at: str
    status: ComparisonStatus
    summary: ComparisonReportSummary
    artifacts: ComparisonReportArtifacts

    def is_match(self) -> bool:
        return self.status == ComparisonStatus.MATCH
