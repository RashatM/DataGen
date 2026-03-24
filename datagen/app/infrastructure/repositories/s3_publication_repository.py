from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo
import pyarrow as pa
import pyarrow.parquet as pq

from app.core.application.constants import EngineName
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout, TableStateKeyLayout
from app.core.application.dto.publication import (
    EngineLoadArtifact,
    EngineLoadPayload,
    EnginePair,
    PublicationArtifacts,
    TablePublication,
)
from app.core.application.ports.publication_repository_port import ArtifactPublicationRepositoryPort
from app.core.domain.entities import GeneratedTableData
from app.infrastructure.errors import ObjectNotFoundError, RunStateCorruptedError
from app.infrastructure.parquet.arrow_schema_builder import ArrowSchemaBuilder
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter


class S3PublicationRepository(ArtifactPublicationRepositoryPort):
    def __init__(self, object_storage: S3StorageAdapter, schema_builder: ArrowSchemaBuilder):
        self.object_storage = object_storage
        self.schema_builder = schema_builder

    def serialize_parquet(self, table_data: GeneratedTableData) -> bytes:
        schema = self.schema_builder.build_schema(table_data.table)
        table = pa.Table.from_pydict(table_data.generated_data, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="snappy")
        return sink.getvalue().to_pybytes()

    @staticmethod
    def deserialize_parquet(payload: bytes) -> dict[str, list[Any]]:
        table = pq.read_table(pa.BufferReader(payload))
        return table.to_pydict()

    def stage_data_artifact(self, table_data: GeneratedTableData, artifact_layout: RunArtifactKeyLayout) -> str:
        table = table_data.table
        data_key = artifact_layout.data_key(table.schema_name, table.table_name)
        parquet_bytes = self.serialize_parquet(table_data)
        return self.object_storage.put_bytes(key=data_key, body=parquet_bytes)

    def stage_engine_artifact(
        self,
        schema_name: str,
        table_name: str,
        artifact_layout: RunArtifactKeyLayout,
        engine_name: EngineName,
        load_payload: EngineLoadPayload,
    ) -> EngineLoadArtifact:
        ddl_key = artifact_layout.ddl_key(schema_name, table_name, engine_name)
        ddl_uri = self.object_storage.put_text(
            key=ddl_key,
            content=f"{load_payload.ddl_query.strip()}\n",
        )
        return EngineLoadArtifact(
            ddl_uri=ddl_uri,
            target_table_name=load_payload.target_table_name,
        )

    def stage_comparison_query(
        self,
        artifact_layout: RunArtifactKeyLayout,
        engine_name: EngineName,
        rendered_query: str,
    ) -> str:
        return self.object_storage.put_text(
            key=artifact_layout.comparison_query_key(engine_name),
            content=f"{rendered_query.strip()}\n",
        )

    def stage_engine_artifacts(
            self,
            table_data: GeneratedTableData,
            artifact_layout: RunArtifactKeyLayout,
            engine_load_payloads: EnginePair[EngineLoadPayload],
    ) -> EnginePair[EngineLoadArtifact]:
        table = table_data.table
        return EnginePair(
            hive=self.stage_engine_artifact(
                schema_name=table.schema_name,
                table_name=table.table_name,
                artifact_layout=artifact_layout,
                engine_name=EngineName.HIVE,
                load_payload=engine_load_payloads.hive,
            ),
            iceberg=self.stage_engine_artifact(
                schema_name=table.schema_name,
                table_name=table.table_name,
                artifact_layout=artifact_layout,
                engine_name=EngineName.ICEBERG,
                load_payload=engine_load_payloads.iceberg,
            ),
        )

    def stage_table_artifacts(
            self,
            table_data: GeneratedTableData,
            artifact_layout: RunArtifactKeyLayout,
            engine_load_payloads: EnginePair[EngineLoadPayload],
    ) -> TablePublication:
        data_uri = self.stage_data_artifact(table_data, artifact_layout)
        engines = self.stage_engine_artifacts(table_data, artifact_layout, engine_load_payloads)
        table = table_data.table
        return TablePublication(
            schema_name=table.schema_name,
            table_name=table.table_name,
            run_id=artifact_layout.run_id,
            artifacts=PublicationArtifacts(
                data_uri=data_uri,
                engines=engines,
            ),
        )

    def stage_comparison_queries(
        self,
        artifact_layout: RunArtifactKeyLayout,
        rendered_queries: EnginePair[str],
    ) -> EnginePair[str]:
        return EnginePair(
            hive=self.stage_comparison_query(
                artifact_layout=artifact_layout,
                engine_name=EngineName.HIVE,
                rendered_query=rendered_queries.hive,
            ),
            iceberg=self.stage_comparison_query(
                artifact_layout=artifact_layout,
                engine_name=EngineName.ICEBERG,
                rendered_query=rendered_queries.iceberg,
            ),
        )

    def commit_pointer(self, schema_name: str, table_name: str, run_id: str) -> None:
        state_layout = TableStateKeyLayout(schema_name=schema_name, table_name=table_name)
        pointer_key = state_layout.pointer_key
        previous_run_id = self.get_latest_run_id(schema_name, table_name)

        self.object_storage.put_json(
            key=pointer_key,
            payload={
                "run_id": run_id,
                "previous_run_id": previous_run_id,
                "updated_at": datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

    def get_latest_run_id(self, schema_name: str, table_name: str) -> str | None:
        state_layout = TableStateKeyLayout(schema_name=schema_name, table_name=table_name)
        pointer_key = state_layout.pointer_key
        try:
            payload = self.object_storage.get_json(key=pointer_key)
        except ObjectNotFoundError:
            return None

        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise RunStateCorruptedError(f"latest_generated pointer is corrupted for key={pointer_key}: invalid run_id")
        return run_id

    def read_table_data(self, schema_name: str, table_name: str, run_id: str) -> dict[str, Any]:
        artifact_layout = RunArtifactKeyLayout(run_id=run_id)
        key = artifact_layout.data_key(schema_name, table_name)
        payload = self.object_storage.get_bytes(key=key)
        return self.deserialize_parquet(payload)

    def cleanup_run_artifacts(self, artifact_layout: RunArtifactKeyLayout) -> None:
        self.object_storage.delete_prefix(artifact_layout.run_prefix)
