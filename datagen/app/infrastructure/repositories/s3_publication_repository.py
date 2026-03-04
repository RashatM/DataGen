from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from app.core.application.dto import TablePublication
from app.core.application.ports.publication_repository_port import (
    IPublicationRepository,
)
from app.core.domain.entities import MockDataEntityResult
from app.infrastructure.errors import ObjectNotFoundError, RunStateCorruptedError
from app.infrastructure.ports.object_storage_port import IObjectStorage


class S3PublicationRepository(IPublicationRepository):
    def __init__(self, object_storage: IObjectStorage):
        self.object_storage = object_storage

    @staticmethod
    def base_prefix(schema_name: str, table_name: str, run_id: str) -> str:
        return (
            f"{schema_name.strip('/')}/"
            f"{table_name.strip('/')}/"
            f"runs/{run_id.strip('/')}"
        )

    @staticmethod
    def build_data_key(schema_name: str, table_name: str, run_id: str) -> str:
        prefix = S3PublicationRepository.base_prefix(schema_name, table_name, run_id)
        return f"{prefix}/data/data.parquet"

    @staticmethod
    def build_ddl_key(
        schema_name: str,
        table_name: str,
        run_id: str,
        target: str,
    ) -> str:
        prefix = S3PublicationRepository.base_prefix(schema_name, table_name, run_id)
        return f"{prefix}/ddl/{target}.sql"

    @staticmethod
    def build_pointer_key(schema_name: str, table_name: str) -> str:
        return f"{schema_name.strip('/')}/{table_name.strip('/')}/state/latest_generated.json"

    @staticmethod
    def serialize_parquet(records: Dict[str, List[Any]]) -> bytes:
        table = pa.Table.from_pydict(records)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        return sink.getvalue().to_pybytes()

    @staticmethod
    def deserialize_parquet(payload: bytes) -> Dict[str, List[Any]]:
        table = pq.read_table(pa.BufferReader(payload))
        return table.to_pydict()

    def publish(
        self,
        entity_result: MockDataEntityResult,
        run_id: str,
        ddl_map: Dict[str, str],
    ) -> TablePublication:
        entity = entity_result.entity

        data_key = self.build_data_key(entity.schema_name, entity.table_name, run_id)
        parquet_bytes = self.serialize_parquet(entity_result.generated_data)
        data_uri = self.object_storage.put_bytes(key=data_key, body=parquet_bytes)

        ddl_uris: Dict[str, str] = {}
        for target, ddl_query in ddl_map.items():
            ddl_key = self.build_ddl_key(
                entity.schema_name,
                entity.table_name,
                run_id,
                target,
            )
            ddl_uris[target] = self.object_storage.put_text(
                key=ddl_key,
                content=f"{ddl_query.strip()}\n",
            )

        pointer_key = self.build_pointer_key(entity.schema_name, entity.table_name)
        pointer_uri = self.object_storage.build_uri(pointer_key)

        publication = TablePublication(
            contract_version="3",
            storage_type="s3",
            schema_name=entity.schema_name,
            table_name=entity.table_name,
            run_id=run_id,
            storage={
                "data_uri": data_uri,
                "ddl_uris": ddl_uris,
                "pointer_uri": pointer_uri,
            },
        )

        self.object_storage.put_json(
            key=pointer_key,
            payload={
                "run_id": run_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        return publication

    def get_latest_run_id(
        self,
        schema_name: str,
        table_name: str,
    ) -> Optional[str]:
        pointer_key = self.build_pointer_key(schema_name, table_name)
        try:
            payload = self.object_storage.get_json(key=pointer_key)
        except ObjectNotFoundError:
            return None

        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise RunStateCorruptedError(
                f"latest_generated pointer is corrupted for key={pointer_key}: invalid run_id"
            )
        return run_id

    def read_entity_data(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
    ) -> Dict[str, Any]:
        key = self.build_data_key(schema_name, table_name, run_id)
        payload = self.object_storage.get_bytes(key=key)
        return self.deserialize_parquet(payload)
