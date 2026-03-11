from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq

from app.core.application.dto import TablePublication
from app.core.application.ports.object_storage_port import IObjectStorage
from app.core.application.ports.publication_repository_port import IPublicationRepository
from app.core.domain.entities import GeneratedTableData
from app.infrastructure.constants import StorageType
from app.infrastructure.errors import ObjectNotFoundError, RunStateCorruptedError
from app.infrastructure.parquet.arrow_schema_builder import ArrowSchemaBuilder


class S3PublicationRepository(IPublicationRepository):
    def __init__(self, object_storage: IObjectStorage, schema_builder: ArrowSchemaBuilder):
        self.object_storage = object_storage
        self.schema_builder = schema_builder

    @staticmethod
    def run_prefix(run_id: str) -> str:
        return f"runs/{run_id.strip('/')}"

    @staticmethod
    def build_data_key(run_id: str, schema_name: str, table_name: str) -> str:
        return f"runs/{run_id.strip('/')}/{schema_name.strip('/')}/{table_name.strip('/')}/data/data.parquet"

    @staticmethod
    def build_ddl_key(run_id: str, schema_name: str, table_name: str, engine_name: str) -> str:
        return f"runs/{run_id.strip('/')}/{schema_name.strip('/')}/{table_name.strip('/')}/ddl/{engine_name}.sql"

    @staticmethod
    def build_pointer_key(schema_name: str, table_name: str) -> str:
        return f"tables/{schema_name.strip('/')}/{table_name.strip('/')}/pointer.json"

    @staticmethod
    def build_comparison_result_key(run_id: str) -> str:
        return f"runs/{run_id.strip('/')}/result/comparison_result.json"

    def serialize_parquet(self, table_data: GeneratedTableData) -> bytes:
        schema = self.schema_builder.build_schema(table_data.table)
        table = pa.Table.from_pydict(table_data.generated_data, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="snappy")
        return sink.getvalue().to_pybytes()

    @staticmethod
    def deserialize_parquet(payload: bytes) -> Dict[str, List[Any]]:
        table = pq.read_table(pa.BufferReader(payload))
        return table.to_pydict()

    def stage_artifacts(
            self, table_data: GeneratedTableData,
            run_id: str,
            ddl_queries: Dict[str, str]
    ) -> TablePublication:
        table = table_data.table
        data_key = self.build_data_key(table.schema_name, table.table_name, run_id)
        parquet_bytes = self.serialize_parquet(table_data)
        data_uri = self.object_storage.put_bytes(key=data_key, body=parquet_bytes)

        ddl_uris: Dict[str, str] = {}
        for engine_name, ddl_query in ddl_queries.items():
            ddl_key = self.build_ddl_key(table.schema_name, table.table_name, run_id, engine_name)
            ddl_uris[engine_name] = self.object_storage.put_text(key=ddl_key, content=f"{ddl_query.strip()}\n")

        publication = TablePublication(
            storage_type=StorageType.S3.value,
            schema_name=table.schema_name,
            table_name=table.table_name,
            run_id=run_id,
            storage={
                "data_uri": data_uri,
                "ddl_uris": ddl_uris,
            },
        )
        return publication

    def commit_pointer(self, schema_name: str, table_name: str, run_id: str) -> None:
        pointer_key = self.build_pointer_key(schema_name, table_name)
        previous_run_id = self.get_latest_run_id(schema_name, table_name)

        self.object_storage.put_json(
            key=pointer_key,
            payload={
                "run_id": run_id,
                "previous_run_id": previous_run_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def get_latest_run_id(self, schema_name: str, table_name: str) -> Optional[str]:
        pointer_key = self.build_pointer_key(schema_name, table_name)
        try:
            payload = self.object_storage.get_json(key=pointer_key)
        except ObjectNotFoundError:
            return None

        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise RunStateCorruptedError(f"latest_generated pointer is corrupted for key={pointer_key}: invalid run_id")
        return run_id

    def read_table_data(self, schema_name: str, table_name: str, run_id: str) -> Dict[str, Any]:
        key = self.build_data_key(schema_name, table_name, run_id)
        payload = self.object_storage.get_bytes(key=key)
        return self.deserialize_parquet(payload)

    def cleanup_run_artifacts(self, run_id: str) -> None:
        self.object_storage.delete_prefix(self.run_prefix(run_id))