from app.core.application.constants import ComparisonStatus
from app.core.application.dto.comparison import (
    ComparisonReport,
    ComparisonReportArtifacts,
    ComparisonReportSummary,
    EngineCountSummary,
    EngineRatioSummary,
)
from app.core.application.ports.comparison_repository_port import IComparisonReportRepository
from app.core.application.ports.object_storage_port import IObjectStorage
from app.infrastructure.errors import ObjectPayloadFormatError


class S3ComparisonReportRepository(IComparisonReportRepository):

    def __init__(self, object_storage: IObjectStorage):
        self.object_storage = object_storage

    @staticmethod
    def require_string(payload: dict, key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be a non-empty string")
        return value

    @staticmethod
    def require_int(payload: dict, key: str) -> int:
        value = payload.get(key)
        if not isinstance(value, int):
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be int")
        return value

    @staticmethod
    def require_number(payload: dict, key: str) -> float:
        value = payload.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be number")
        return float(value)

    def parse_engine_count_summary(self, payload: dict, section_name: str) -> EngineCountSummary:
        section = payload.get(section_name)
        if not isinstance(section, dict):
            raise ObjectPayloadFormatError(f"Comparison report field '{section_name}' must be object")
        return EngineCountSummary(
            hive=self.require_int(section, "hive"),
            iceberg=self.require_int(section, "iceberg"),
        )

    def parse_engine_ratio_summary(self, payload: dict, section_name: str) -> EngineRatioSummary:
        section = payload.get(section_name)
        if not isinstance(section, dict):
            raise ObjectPayloadFormatError(f"Comparison report field '{section_name}' must be object")
        return EngineRatioSummary(
            hive=self.require_number(section, "hive"),
            iceberg=self.require_number(section, "iceberg"),
        )

    def parse_report(self, payload: dict, expected_run_id: str) -> ComparisonReport:
        run_id = self.require_string(payload, "run_id")
        if run_id != expected_run_id:
            raise ObjectPayloadFormatError(
                f"Comparison report run_id mismatch: expected={expected_run_id}, actual={run_id}"
            )

        summary = payload.get("summary")
        if not isinstance(summary, dict):
            raise ObjectPayloadFormatError("Comparison report field 'summary' must be object")

        artifacts = payload.get("artifacts")
        if not isinstance(artifacts, dict):
            raise ObjectPayloadFormatError("Comparison report field 'artifacts' must be object")

        try:
            status = ComparisonStatus(self.require_string(payload, "status"))
        except ValueError as error:
            raise ObjectPayloadFormatError("Comparison report field 'status' has unsupported value") from error

        row_count = self.parse_engine_count_summary(summary, "row_count")
        exclusive_row_count = self.parse_engine_count_summary(summary, "exclusive_row_count")
        row_count_delta = self.require_int(summary, "row_count_delta")
        exclusive_row_ratio = self.parse_engine_ratio_summary(summary, "exclusive_row_ratio")

        return ComparisonReport(
            run_id=run_id,
            checked_at=self.require_string(payload, "checked_at"),
            status=status,
            summary=ComparisonReportSummary(
                row_count=row_count,
                row_count_delta=row_count_delta,
                exclusive_row_count=exclusive_row_count,
                exclusive_row_ratio=exclusive_row_ratio,
            ),
            artifacts=ComparisonReportArtifacts(
                hive_result_uri=self.require_string(artifacts, "hive_result_uri"),
                iceberg_result_uri=self.require_string(artifacts, "iceberg_result_uri"),
            ),
        )

    def load_report(self, report_key: str, expected_run_id: str) -> ComparisonReport:
        payload = self.object_storage.get_json(key=report_key)
        return self.parse_report(payload, expected_run_id=expected_run_id)
