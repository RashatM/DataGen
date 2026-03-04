from datetime import datetime, timezone
from typing import Optional

from app.core.application.ports.run_state_repository_port import IRunStateRepository
from app.infrastructure.errors import (
    ObjectNotFoundError,
    RunStateCorruptedError,
)
from app.infrastructure.ports.object_storage_port import IObjectStorage


class S3RunStateRepository(IRunStateRepository):
    def __init__(self, object_storage: IObjectStorage):
        self.object_storage = object_storage

    @staticmethod
    def build_state_key(schema_name: str, table_name: str) -> str:
        return f"{schema_name.strip('/')}/{table_name.strip('/')}/state/latest_generated.json"

    @staticmethod
    def build_pointer_payload(run_id: str) -> dict[str, str]:
        return {
            "run_id": run_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def save_latest_run_pointer(
            self,
            schema_name: str,
            table_name: str,
            run_id: str,
    ) -> str:
        key = self.build_state_key(schema_name, table_name)
        payload = self.build_pointer_payload(run_id)
        return self.object_storage.put_json(key=key, payload=payload)

    def get_latest_run_id(
            self,
            schema_name: str,
            table_name: str,
    ) -> Optional[str]:
        key = self.build_state_key(schema_name, table_name)
        try:
            payload = self.object_storage.get_json(key=key)
        except ObjectNotFoundError:
            return None

        run_id = payload.get("run_id")

        if not isinstance(run_id, str) or not run_id:
            raise RunStateCorruptedError(
                f"latest_generated pointer is corrupted for key={key}: invalid run_id"
            )
        return run_id
