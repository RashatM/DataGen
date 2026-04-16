from datetime import datetime
from tempfile import TemporaryFile
from typing import IO
from zoneinfo import ZoneInfo
import pyarrow as pa
import pyarrow.parquet as pq

from app.application.constants import EngineName
from app.application.dto.pipeline import TableLoadSpec
from app.application.layouts.storage_layout import (
    RUNS_ROOT_PREFIX,
    TABLES_ROOT_PREFIX,
    RunArtifactKeyLayout,
    TableStateKeyLayout,
)
from app.application.dto.publication import (
    EnginePair,
    TablePublication,
)
from app.application.ports.publication_repository_port import ArtifactPublicationRepositoryPort
from app.domain.entities import GeneratedTableData
from app.domain.value_types import GeneratedColumnsByName
from app.infrastructure.errors import ObjectNotFoundError, RunStateCorruptedError
from app.infrastructure.parquet.arrow_schema_builder import ArrowSchemaBuilder
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter


class S3PublicationRepository(ArtifactPublicationRepositoryPort):
    """Публикует parquet-данные, comparison query и per-table pointers в S3."""
    PARQUET_WRITE_BATCH_SIZE = 100_000

    def __init__(self, object_storage: S3StorageAdapter, schema_builder: ArrowSchemaBuilder):
        self.object_storage = object_storage
        self.schema_builder = schema_builder

    @classmethod
    def build_parquet_chunk(
        cls,
        table_data: GeneratedTableData,
        schema: pa.Schema,
        start: int,
        stop: int,
    ) -> pa.Table:
        """Собирает маленький Arrow chunk из диапазона строк, чтобы не материализовывать всю таблицу целиком."""
        return pa.table(
            data={
                column_name: table_data.generated_data[column_name][start:stop]
                for column_name in schema.names
            },
            schema=schema,
        )

    def write_parquet(self, table_data: GeneratedTableData, destination: IO[bytes]) -> None:
        """Пишет parquet чанками в поток, уменьшая peak memory на больших таблицах."""
        schema = self.schema_builder.build_schema(table_data.table)
        total_rows = table_data.table.total_rows

        with pq.ParquetWriter(destination, schema=schema) as writer:
            if total_rows == 0:
                writer.write_table(self.build_parquet_chunk(table_data, schema, 0, 0))
            else:
                for start in range(0, total_rows, self.PARQUET_WRITE_BATCH_SIZE):
                    stop = min(start + self.PARQUET_WRITE_BATCH_SIZE, total_rows)
                    writer.write_table(self.build_parquet_chunk(table_data, schema, start, stop))
        destination.flush()

    @staticmethod
    def deserialize_parquet(payload: bytes) -> GeneratedColumnsByName:
        table = pq.read_table(pa.BufferReader(payload))
        return table.to_pydict()

    def stage_data_artifact(self, table_data: GeneratedTableData, artifact_layout: RunArtifactKeyLayout) -> str:
        """Публикует parquet-артефакт одной таблицы и возвращает S3 URI для DAG contract."""
        table = table_data.table
        data_key = artifact_layout.data_key(table.table_name)
        with TemporaryFile(mode="w+b") as temp_file:
            self.write_parquet(table_data, temp_file)
            return self.object_storage.upload_stream(
                key=data_key,
                stream=temp_file,
            )

    def stage_comparison_query(
        self,
        artifact_layout: RunArtifactKeyLayout,
        engine_name: EngineName,
        rendered_query: str,
    ) -> str:
        """Публикует comparison SQL конкретного движка как отдельный текстовый артефакт."""
        return self.object_storage.put_text(
            key=artifact_layout.comparison_query_key(engine_name),
            content=f"{rendered_query.strip()}\n",
        )

    def stage_table_artifacts(
            self,
            table_data: GeneratedTableData,
            artifact_layout: RunArtifactKeyLayout,
            load_spec: TableLoadSpec,
    ) -> TablePublication:
        """Публикует данные таблицы и возвращает publication-объект для дальнейшего построения DAG payload."""
        data_uri = self.stage_data_artifact(table_data, artifact_layout)
        table = table_data.table
        return TablePublication(
            table_name=table.table_name,
            run_id=artifact_layout.run_id,
            data_uri=data_uri,
            load_spec=load_spec,
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

    def commit_pointer(self, table_name: str, run_id: str) -> None:
        """Обновляет per-table pointer на последний технически успешный run."""
        state_layout = TableStateKeyLayout(table_name=table_name)
        pointer_key = state_layout.pointer_key
        previous_run_id = self.get_latest_run_id(table_name)

        self.object_storage.put_json(
            key=pointer_key,
            payload={
                "run_id": run_id,
                "previous_run_id": previous_run_id,
                "updated_at": datetime.now(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S"),
            },
        )

    def get_latest_run_id(self, table_name: str) -> str | None:
        """Читает текущий per-table pointer и возвращает последний известный run_id для таблицы."""
        state_layout = TableStateKeyLayout(table_name=table_name)
        pointer_key = state_layout.pointer_key
        try:
            payload = self.object_storage.get_json(key=pointer_key)
        except ObjectNotFoundError:
            return None

        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise RunStateCorruptedError(f"latest_generated pointer is corrupted for key={pointer_key}: invalid run_id")
        return run_id

    def read_table_data(self, table_name: str, run_id: str) -> GeneratedColumnsByName:
        """Загружает опубликованный parquet таблицы назад в python-словарь колонок."""
        artifact_layout = RunArtifactKeyLayout(run_id=run_id)
        key = artifact_layout.data_key(table_name)
        payload = self.object_storage.get_bytes(key=key)
        return self.deserialize_parquet(payload)

    def cleanup_run_artifacts(self, artifact_layout: RunArtifactKeyLayout) -> None:
        """Удаляет все опубликованные артефакты конкретного run по его S3 prefix."""
        self.object_storage.delete_prefix(f"{artifact_layout.run_prefix}/")

    def delete_run_artifacts(self, run_id: str) -> int:
        """Удаляет все опубликованные артефакты конкретного run_id и возвращает число удалённых объектов."""
        artifact_layout = RunArtifactKeyLayout(run_id=run_id)
        return self.object_storage.delete_prefix(f"{artifact_layout.run_prefix}/")

    def list_run_ids(self) -> list[str]:
        """Возвращает run_id, найденные по непосредственным child-prefixes под runs/."""
        run_prefixes = self.object_storage.list_child_prefixes(RUNS_ROOT_PREFIX)
        run_ids: list[str] = []
        root_prefix = f"{RUNS_ROOT_PREFIX}/"
        for run_prefix in run_prefixes:
            if not run_prefix.startswith(root_prefix):
                continue
            run_id = run_prefix[len(root_prefix):].strip("/")
            if run_id:
                run_ids.append(run_id)
        return sorted(run_ids)

    def list_pointer_run_ids(self) -> list[str]:
        """Возвращает run_id из per-table pointers, которые нельзя удалять retention cleanup-ом."""
        table_prefixes = self.object_storage.list_child_prefixes(TABLES_ROOT_PREFIX)
        pointer_run_ids: set[str] = set()
        root_prefix = f"{TABLES_ROOT_PREFIX}/"

        for table_prefix in table_prefixes:
            if not table_prefix.startswith(root_prefix):
                continue

            table_name = table_prefix[len(root_prefix):].strip("/")
            if not table_name:
                continue

            state_layout = TableStateKeyLayout(table_name=table_name)
            try:
                payload = self.object_storage.get_json(key=state_layout.pointer_key)
            except ObjectNotFoundError:
                continue

            run_id = payload.get("run_id")
            if not isinstance(run_id, str) or not run_id:
                raise RunStateCorruptedError(
                    f"latest_generated pointer is corrupted for key={state_layout.pointer_key}: invalid run_id"
                )
            pointer_run_ids.add(run_id)

        return sorted(pointer_run_ids)
