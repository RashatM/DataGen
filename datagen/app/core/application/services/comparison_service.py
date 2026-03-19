from app.core.application.constants import ComparisonStatus
from app.core.application.dto import ComparisonReport, EngineCountSummary
from app.core.application.ports.comparison_repository_port import IComparisonReportRepository
from app.shared.logger import comparison_logger

logger = comparison_logger


class ComparisonService:

    def __init__(self, repository: IComparisonReportRepository) -> None:
        self.repository = repository

    @staticmethod
    def validate_count_summary(summary: EngineCountSummary, summary_name: str) -> None:
        if summary.hive < 0 or summary.iceberg < 0:
            raise ValueError(f"Comparison report '{summary_name}' values must be non-negative")

    @staticmethod
    def resolve_status(report: ComparisonReport) -> ComparisonStatus:
        if report.summary.unmatched_row_count.hive == 0 and report.summary.unmatched_row_count.iceberg == 0:
            return ComparisonStatus.MATCH
        return ComparisonStatus.MISMATCH

    def validate_report(self, report: ComparisonReport) -> None:
        self.validate_count_summary(report.summary.row_count, "row_count")
        self.validate_count_summary(report.summary.unmatched_row_count, "unmatched_row_count")

        if report.summary.unmatched_row_count.hive > report.summary.row_count.hive:
            raise ValueError("Comparison report is inconsistent: unmatched_row_count.hive exceeds row_count.hive")
        if report.summary.unmatched_row_count.iceberg > report.summary.row_count.iceberg:
            raise ValueError("Comparison report is inconsistent: unmatched_row_count.iceberg exceeds row_count.iceberg")

        expected_status = self.resolve_status(report)
        if report.status != expected_status:
            raise ValueError(
                f"Comparison report status mismatch: expected={expected_status.value}, actual={report.status.value}"
            )

    @staticmethod
    def format_report_summary(report: ComparisonReport) -> str:
        return (
            f"comparison_status={report.status.value}, "
            f"hive_row_count={report.summary.row_count.hive}, "
            f"iceberg_row_count={report.summary.row_count.iceberg}, "
            f"hive_unmatched_row_count={report.summary.unmatched_row_count.hive}, "
            f"iceberg_unmatched_row_count={report.summary.unmatched_row_count.iceberg}"
        )

    def read_report(self, report_key: str, run_id: str) -> ComparisonReport:
        logger.info(f"Comparison report read started: run_id={run_id}, report_key={report_key}")
        report = self.repository.read_report(report_key=report_key, expected_run_id=run_id)
        self.validate_report(report)
        logger.info(
            f"Comparison report read completed: run_id={run_id}, "
            f"{self.format_report_summary(report)}"
        )
        return report
