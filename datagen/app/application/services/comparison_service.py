from app.application.constants import ComparisonStatus
from app.application.dto.comparison import ComparisonReport, EngineCountSummary, EngineRatioSummary
from app.application.ports.comparison_repository_port import ComparisonReportRepositoryPort
from app.shared.logger import comparison_logger

logger = comparison_logger


class ComparisonReportService:
    """Проверяет, интерпретирует и форматирует итоговый comparison-report после чтения из репозитория."""
    SHARE_EPSILON = 1e-6

    def __init__(self, repository: ComparisonReportRepositoryPort) -> None:
        self.repository = repository

    @staticmethod
    def validate_count_summary(summary: EngineCountSummary, summary_name: str) -> None:
        if summary.hive < 0 or summary.iceberg < 0:
            raise ValueError(f"Comparison report '{summary_name}' values must be non-negative")

    @staticmethod
    def validate_ratio_summary(summary: EngineRatioSummary, summary_name: str) -> None:
        if summary.hive < 0 or summary.hive > 1:
            raise ValueError(f"Comparison report '{summary_name}.hive' must be in range [0, 1]")
        if summary.iceberg < 0 or summary.iceberg > 1:
            raise ValueError(f"Comparison report '{summary_name}.iceberg' must be in range [0, 1]")

    @staticmethod
    def calculate_ratio(exclusive_count: int, total_count: int) -> float:
        if total_count == 0:
            return 0.0
        return round(exclusive_count / total_count, 6)

    @staticmethod
    def resolve_status(report: ComparisonReport) -> ComparisonStatus:
        if report.summary.exclusive_row_count.hive == 0 and report.summary.exclusive_row_count.iceberg == 0:
            return ComparisonStatus.MATCH
        return ComparisonStatus.MISMATCH

    def validate_report(self, report: ComparisonReport) -> None:
        """Проверяет внутреннюю согласованность полей comparison-report, а не только их наличие."""
        self.validate_count_summary(report.summary.row_count, "row_count")
        self.validate_count_summary(report.summary.exclusive_row_count, "exclusive_row_count")
        self.validate_ratio_summary(report.summary.exclusive_row_ratio, "exclusive_row_ratio")

        if report.summary.exclusive_row_count.hive > report.summary.row_count.hive:
            raise ValueError("Comparison report is inconsistent: exclusive_row_count.hive exceeds row_count.hive")
        if report.summary.exclusive_row_count.iceberg > report.summary.row_count.iceberg:
            raise ValueError("Comparison report is inconsistent: exclusive_row_count.iceberg exceeds row_count.iceberg")
        if report.summary.row_count_delta != abs(report.summary.row_count.hive - report.summary.row_count.iceberg):
            raise ValueError("Comparison report is inconsistent: row_count_delta does not match row_count difference")

        expected_hive_ratio = self.calculate_ratio(
            report.summary.exclusive_row_count.hive,
            report.summary.row_count.hive,
        )
        expected_iceberg_ratio = self.calculate_ratio(
            report.summary.exclusive_row_count.iceberg,
            report.summary.row_count.iceberg,
        )
        if abs(report.summary.exclusive_row_ratio.hive - expected_hive_ratio) > self.SHARE_EPSILON:
            raise ValueError("Comparison report is inconsistent: exclusive_row_ratio.hive does not match counts")
        if abs(report.summary.exclusive_row_ratio.iceberg - expected_iceberg_ratio) > self.SHARE_EPSILON:
            raise ValueError("Comparison report is inconsistent: exclusive_row_ratio.iceberg does not match counts")

        expected_status = self.resolve_status(report)
        if report.status != expected_status:
            raise ValueError(
                f"Comparison report status mismatch: expected={expected_status.value}, actual={report.status.value}"
            )

    @staticmethod
    def format_report_summary(report: ComparisonReport) -> str:
        excluded_columns_lines = []
        if report.excluded_columns.hive:
            excluded_columns_lines.append(f"    hive: {report.excluded_columns.hive}")
        if report.excluded_columns.iceberg:
            excluded_columns_lines.append(f"    iceberg: {report.excluded_columns.iceberg}")
        if report.excluded_columns.temporal:
            excluded_columns_lines.append(f"    temporal: {report.excluded_columns.temporal}")
        excluded_columns_text = ""
        if excluded_columns_lines:
            excluded_columns_text = "\n  excluded_columns:\n" + "\n".join(excluded_columns_lines)

        return (
            f"comparison:\n"
            f"  status: {report.status.value}\n"
            f"  row_count:\n"
            f"    hive: {report.summary.row_count.hive}\n"
            f"    iceberg: {report.summary.row_count.iceberg}\n"
            f"  row_count_delta: {report.summary.row_count_delta}\n"
            f"  exclusive_row_count:\n"
            f"    hive: {report.summary.exclusive_row_count.hive}\n"
            f"    iceberg: {report.summary.exclusive_row_count.iceberg}\n"
            f"  exclusive_row_ratio:\n"
            f"    hive: {report.summary.exclusive_row_ratio.hive}\n"
            f"    iceberg: {report.summary.exclusive_row_ratio.iceberg}"
            f"{excluded_columns_text}"
        )

    def load_report(self, report_key: str, run_id: str) -> ComparisonReport:
        report = self.repository.load_report(report_key=report_key, expected_run_id=run_id)
        self.validate_report(report)
        logger.info(
            f"Comparison report loaded: run_id={run_id}, comparison_status={report.status.value}"
        )
        return report
