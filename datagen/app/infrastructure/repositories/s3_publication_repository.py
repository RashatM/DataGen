from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
import pyarrow as pa
import pyarrow.parquet as pq

from app.core.application.dto import (
    EngineLoadArtifact,
    EngineLoadPayload,
    PublicationArtifacts,
    RunArtifactLayout,
    TablePublication,
)
from app.core.application.ports.object_storage_port import IObjectStorage
from app.core.application.ports.publication_repository_port import IArtifactPublicationRepository
from app.core.domain.entities import GeneratedTableData
from app.infrastructure.errors import ObjectNotFoundError, RunStateCorruptedError
from app.infrastructure.parquet.arrow_schema_builder import ArrowSchemaBuilder


class S3PublicationRepository(IArtifactPublicationRepository):
    def __init__(self, object_storage: IObjectStorage, schema_builder: ArrowSchemaBuilder):
        self.object_storage = object_storage
        self.schema_builder = schema_builder

    def build_pointer_key(self, schema_name: str, table_name: str) -> str:
        return f"tables/{schema_name.strip('/')}/{table_name.strip('/')}/pointer.json"

    def build_data_key(self, run_id: str, schema_name: str, table_name: str) -> str:
        run_prefix = f"runs/{run_id.strip('/')}"
        return f"{run_prefix}/{schema_name.strip('/')}/{table_name.strip('/')}/data/data.parquet"

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

    def stage_data_artifact(self, table_data: GeneratedTableData, layout: RunArtifactLayout) -> str:
        table = table_data.table
        data_key = layout.data_key(table.schema_name, table.table_name)
        parquet_bytes = self.serialize_parquet(table_data)
        return self.object_storage.put_bytes(key=data_key, body=parquet_bytes)

    def stage_engine_artifacts(
            self,
            table_data: GeneratedTableData,
            layout: RunArtifactLayout,
            engine_load_payloads: Dict[str, EngineLoadPayload],
    ) -> Dict[str, EngineLoadArtifact]:
        table = table_data.table
        engines: Dict[str, EngineLoadArtifact] = {}
        for engine_name, load_payload in engine_load_payloads.items():
            ddl_key = layout.ddl_key(table.schema_name, table.table_name, engine_name)
            ddl_uri = self.object_storage.put_text(key=ddl_key, content=f"{load_payload.ddl_query.strip()}\n")
            engines[engine_name] = EngineLoadArtifact(
                ddl_uri=ddl_uri,
                target_table_name=load_payload.target_table_name,
            )
        return engines

    def stage_table_artifacts(
            self,
            table_data: GeneratedTableData,
            layout: RunArtifactLayout,
            engine_load_payloads: Dict[str, EngineLoadPayload],
    ) -> TablePublication:
        data_uri = self.stage_data_artifact(table_data, layout)
        engines = self.stage_engine_artifacts(table_data, layout, engine_load_payloads)
        table = table_data.table
        return TablePublication(
            schema_name=table.schema_name,
            table_name=table.table_name,
            run_id=layout.run_id,
            artifacts=PublicationArtifacts(
                data_uri=data_uri,
                engines=engines,
            ),
        )

    def stage_comparison_queries(
        self,
        layout: RunArtifactLayout,
        rendered_queries: Dict[str, str],
    ) -> Dict[str, str]:
        query_uris: Dict[str, str] = {}
        for engine_name, rendered_query in rendered_queries.items():
            query_uris[engine_name] = self.object_storage.put_text(
                key=layout.comparison_query_key(engine_name),
                content=f"{rendered_query.strip()}\n",
            )
        return query_uris

    def commit_pointer(self, schema_name: str, table_name: str, run_id: str) -> None:
        pointer_key = self.build_pointer_key(schema_name, table_name)
        previous_run_id = self.get_latest_run_id(schema_name, table_name)

        self.object_storage.put_json(
            key=pointer_key,
            payload={
                "run_id": run_id,
                "previous_run_id": previous_run_id,
                "updated_at": datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S"),
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
        key = self.build_data_key(run_id, schema_name, table_name)
        payload = self.object_storage.get_bytes(key=key)
        return self.deserialize_parquet(payload)

    def cleanup_run_artifacts(self, layout: RunArtifactLayout) -> None:
        self.object_storage.delete_prefix(layout.run_prefix)
