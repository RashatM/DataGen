from app.application.dto.execution_result import ExecutionDiagnostic
from app.application.ports.diagnostic_repository_port import ExecutionDiagnosticRepositoryPort
from app.infrastructure.errors import ObjectPayloadFormatError
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter
from app.shared.logger import pipeline_logger

logger = pipeline_logger


class S3ExecutionDiagnosticRepository(ExecutionDiagnosticRepositoryPort):
    """Читает execution diagnostics из S3 run artifacts."""

    def __init__(self, object_storage: S3StorageAdapter):
        self.object_storage = object_storage

    @staticmethod
    def require_string(payload: dict, key: str) -> str:
        value = payload.get(key)
        if not isinstance(value, str) or not value:
            raise ObjectPayloadFormatError(f"Execution diagnostic field '{key}' must be a non-empty string")
        return value

    def parse_diagnostic(self, payload: dict, expected_run_id: str) -> ExecutionDiagnostic:
        run_id = self.require_string(payload, "run_id")
        if run_id != expected_run_id:
            raise ObjectPayloadFormatError(
                f"Execution diagnostic run_id mismatch: expected={expected_run_id}, actual={run_id}"
            )

        return ExecutionDiagnostic(
            run_id=run_id,
            task_id=self.require_string(payload, "task_id"),
            message=self.require_string(payload, "message"),
            code=self.require_string(payload, "code"),
        )

    def load_diagnostics(self, diagnostics_prefix: str, expected_run_id: str) -> list[ExecutionDiagnostic]:
        diagnostics: list[ExecutionDiagnostic] = []
        for key in self.object_storage.list_object_keys(diagnostics_prefix):
            if not key.endswith(".json"):
                continue
            try:
                payload = self.object_storage.get_json(key=key)
                diagnostics.append(
                    self.parse_diagnostic(
                        payload=payload,
                        expected_run_id=expected_run_id,
                    )
                )
            except Exception:
                logger.exception(f"Skipping malformed execution diagnostic: key={key}")

        return diagnostics
