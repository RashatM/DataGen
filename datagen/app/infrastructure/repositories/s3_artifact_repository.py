from typing import Any, Dict, List
import pyarrow as pa
import pyarrow.parquet as pq

from app.core.application.ports.artifact_repository_port import IArtifactRepository
from app.infrastructure.ports.object_storage_port import IObjectStorage


class S3ArtifactRepository(IArtifactRepository):
    def __init__(self, object_storage: IObjectStorage):
        self.object_storage = object_storage

    @staticmethod
    def base_prefix(schema_name: str, table_name: str, run_id: str) -> str:
        return (
            f"{schema_name.strip('/')}/"
            f"{table_name.strip('/')}/"
            f"runs/{run_id.strip('/')}"
        )

    def build_data_key(self, schema_name: str, table_name: str, run_id: str) -> str:
        prefix = self.base_prefix(schema_name, table_name, run_id)
        return f"{prefix}/data/data.parquet"


    def build_ddl_key(
            self,
            schema_name: str,
            table_name: str,
            run_id: str,
            table_format: str,
    ) -> str:
        prefix = self.base_prefix(schema_name, table_name, run_id)
        return f"{prefix}/ddl/{table_format}.sql"

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

    def save_entity_ddl(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
        table_format: str,
        ddl_query: str,
    ) -> str:
        key = self.build_ddl_key(schema_name, table_name, run_id, table_format)
        return self.object_storage.put_text(key=key, content=f"{ddl_query.strip()}\n")

    def save_entity_data(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
        records: Dict[str, List[Any]],
    ) -> str:
        key = self.build_data_key(schema_name, table_name, run_id)
        parquet_bytes = self.serialize_parquet(records)
        return self.object_storage.put_bytes(key=key, body=parquet_bytes)

    def read_entity_data(
        self,
        schema_name: str,
        table_name: str,
        run_id: str,
    ) -> Dict[str, List[Any]]:
        key = self.build_data_key(schema_name, table_name, run_id)
        payload = self.object_storage.get_bytes(key=key)
        return self.deserialize_parquet(payload)
