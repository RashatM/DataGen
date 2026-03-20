from dataclasses import asdict
from typing import Any, Dict, List

from app.core.application.constants import EngineName
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.publication import EnginePair, TablePublication
from app.core.application.ports.object_storage_port import IObjectStorage


class AirflowDagPayloadBuilder:

    def __init__(self, object_storage: IObjectStorage) -> None:
        self.object_storage = object_storage

    def build_table_entry(self, publication: TablePublication) -> Dict[str, Any]:
        return {
            "schema_name": publication.schema_name,
            "table_name": publication.table_name,
            "artifacts": asdict(publication.artifacts),
        }

    def build_comparison_entry(
        self,
        artifact_layout: RunArtifactKeyLayout,
        comparison_query_uris: EnginePair[str],
    ) -> Dict[str, Any]:
        return {
            "query_uris": {
                EngineName.HIVE.value: comparison_query_uris.hive,
                EngineName.ICEBERG.value: comparison_query_uris.iceberg,
            },
            "report_uri": self.object_storage.build_uri(artifact_layout.comparison_report_key),
            "result_uris": {
                engine_name.value: self.object_storage.build_uri(
                    artifact_layout.engine_result_key(engine_name)
                )
                for engine_name in EngineName.all()
            },
        }

    def build(
        self,
        artifact_layout: RunArtifactKeyLayout,
        publications: List[TablePublication],
        comparison_query_uris: EnginePair[str],
    ) -> Dict[str, Any]:
        return {
            "run_id": artifact_layout.run_id,
            "tables": [self.build_table_entry(publication) for publication in publications],
            "comparison": self.build_comparison_entry(
                artifact_layout=artifact_layout,
                comparison_query_uris=comparison_query_uris,
            ),
        }
