from app.core.application.constants import ComparisonStatus
from app.core.application.dto.comparison import (
    ComparisonReport,
    ComparisonReportArtifacts,
    ComparisonReportExcludedColumns,
    ComparisonReportSummary,
    EngineCountSummary,
    EngineRatioSummary,
)
from app.core.application.ports.comparison_repository_port import ComparisonReportRepositoryPort
from app.infrastructure.errors import ObjectPayloadFormatError
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter


class S3ComparisonReportRepository(ComparisonReportRepositoryPort):
    """Читает comparison_result.json из S3 и валидирует его структурно на уровне infrastructure."""

    def __init__(self, object_storage: S3StorageAdapter):
        self.object_storage = object_storage

    @staticmethod
    def require_object(payload: dict, key: str) -> dict:
        value = payload.get(key)
        if not isinstance(value, dict):
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be object")
        return value

    @staticmethod
    def require_string(payload: dict, key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be a non-empty string")
        return value

    @staticmethod
    def require_int(payload: dict, key: str) -> int:
        value = payload.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be int")
        return value

    @staticmethod
    def require_number(payload: dict, key: str) -> float:
        value = payload.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be number")
        return float(value)

    @staticmethod
    def require_string_list(payload: dict, key: str) -> list[str]:
        value = payload.get(key)
        if not isinstance(value, list):
            raise ObjectPayloadFormatError(f"Comparison report field '{key}' must be list")
        for item in value:
            if not isinstance(item, str):
                raise ObjectPayloadFormatError(f"Comparison report field '{key}' must contain only strings")
        return value

    def parse_engine_count_summary(self, payload: dict, section_name: str) -> EngineCountSummary:
        section = self.require_object(payload, section_name)
        return EngineCountSummary(
            hive=self.require_int(section, "hive"),
            iceberg=self.require_int(section, "iceberg"),
        )

    def parse_engine_ratio_summary(self, payload: dict, section_name: str) -> EngineRatioSummary:
        section = self.require_object(payload, section_name)
        return EngineRatioSummary(
            hive=self.require_number(section, "hive"),
            iceberg=self.require_number(section, "iceberg"),
        )

    def parse_excluded_columns(self, payload: dict) -> ComparisonReportExcludedColumns:
        section = payload.get("excluded_columns")
        if section is None:
            return ComparisonReportExcludedColumns(hive=[], iceberg=[], temporal=[])
        if not isinstance(section, dict):
            raise ObjectPayloadFormatError("Comparison report field 'excluded_columns' must be object")
        return ComparisonReportExcludedColumns(
            hive=self.require_string_list(section, "hive"),
            iceberg=self.require_string_list(section, "iceberg"),
            temporal=self.require_string_list(section, "temporal"),
        )

    def parse_report(self, payload: dict, expected_run_id: str) -> ComparisonReport:
        """Преобразует сырой JSON comparison-report в типизированный DTO и проверяет базовые поля контракта."""
        run_id = self.require_string(payload, "run_id")
        if run_id != expected_run_id:
            raise ObjectPayloadFormatError(
                f"Comparison report run_id mismatch: expected={expected_run_id}, actual={run_id}"
            )

        summary = self.require_object(payload, "summary")
        artifacts = self.require_object(payload, "artifacts")

        try:
            status = ComparisonStatus(self.require_string(payload, "status"))
        except ValueError as error:
            raise ObjectPayloadFormatError("Comparison report field 'status' has unsupported value") from error

        row_count = self.parse_engine_count_summary(summary, "row_count")
        exclusive_row_count = self.parse_engine_count_summary(summary, "exclusive_row_count")
        row_count_delta = self.require_int(summary, "row_count_delta")
        exclusive_row_ratio = self.parse_engine_ratio_summary(summary, "exclusive_row_ratio")
        excluded_columns = self.parse_excluded_columns(payload)

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
            excluded_columns=excluded_columns,
        )

    def load_report(self, report_key: str, expected_run_id: str) -> ComparisonReport:
        """Загружает comparison-report из S3 и парсит его в application DTO."""
        payload = self.object_storage.get_json(key=report_key)
        return self.parse_report(payload, expected_run_id=expected_run_id)
