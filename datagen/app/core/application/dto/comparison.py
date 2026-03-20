from dataclasses import dataclass

from app.core.application.constants import ComparisonStatus


@dataclass(slots=True)
class EngineCountSummary:
    hive: int
    iceberg: int


@dataclass(slots=True)
class EngineRatioSummary:
    hive: float
    iceberg: float


@dataclass(slots=True)
class ComparisonReportArtifacts:
    hive_result_uri: str
    iceberg_result_uri: str


@dataclass(slots=True)
class ComparisonReportSummary:
    """Summary section of comparison_result.json.

    row_count:
        Total rows materialized by each engine.
    row_count_delta:
        Absolute difference between hive and iceberg row counts.
    exclusive_row_count:
        Rows left only on one side after bilateral exceptAll comparison.
    exclusive_row_ratio:
        exclusive_row_count / row_count for each engine, or 0.0 when row_count is 0.
    """

    row_count: EngineCountSummary
    row_count_delta: int
    exclusive_row_count: EngineCountSummary
    exclusive_row_ratio: EngineRatioSummary


@dataclass(slots=True)
class ComparisonReport:
    run_id: str
    checked_at: str
    status: ComparisonStatus
    summary: ComparisonReportSummary
    artifacts: ComparisonReportArtifacts

    def is_match(self) -> bool:
        return self.status == ComparisonStatus.MATCH
