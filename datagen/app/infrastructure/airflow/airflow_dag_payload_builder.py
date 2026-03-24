from dataclasses import asdict
from typing import Any

from app.core.application.constants import EngineName
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.publication import EnginePair, TablePublication
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter


class AirflowDagPayloadBuilder:

    def __init__(self, object_storage: S3StorageAdapter) -> None:
        self.object_storage = object_storage

    @staticmethod
    def build_table_entry(publication: TablePublication) -> dict[str, Any]:
        return {
            "schema_name": publication.schema_name,
            "table_name": publication.table_name,
            "artifacts": asdict(publication.artifacts),
        }

    def build_comparison_entry(
        self,
        artifact_layout: RunArtifactKeyLayout,
        comparison_query_uris: EnginePair[str],
    ) -> dict[str, Any]:
        return {
            "query_uris": {
                engine_name.value: comparison_query_uris.get_value(engine_name)
                for engine_name in EngineName
            },
            "report_uri": self.object_storage.build_uri(artifact_layout.comparison_report_key),
            "result_uris": {
                engine_name.value: self.object_storage.build_uri(
                    artifact_layout.comparison_query_result_key(engine_name)
                )
                for engine_name in EngineName
            },
        }

    def build(
        self,
        artifact_layout: RunArtifactKeyLayout,
        publications: list[TablePublication],
        comparison_query_uris: EnginePair[str],
    ) -> dict[str, Any]:
        return {
            "run_id": artifact_layout.run_id,
            "tables": [self.build_table_entry(publication) for publication in publications],
            "comparison": self.build_comparison_entry(
                artifact_layout=artifact_layout,
                comparison_query_uris=comparison_query_uris,
            ),
        }
