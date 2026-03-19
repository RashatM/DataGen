from dataclasses import asdict
from typing import Any, Dict, List

from app.core.application.dto import RunArtifactLayout, TablePublication
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
        layout: RunArtifactLayout,
        comparison_query_uris: Dict[str, str],
    ) -> Dict[str, Any]:
        return {
            "query_uris": dict(comparison_query_uris),
            "report_uri": self.object_storage.build_uri(layout.comparison_report_key()),
            "result_uris": {
                engine_name: self.object_storage.build_uri(layout.engine_result_key(engine_name))
                for engine_name in comparison_query_uris
            },
        }

    def build(
        self,
        layout: RunArtifactLayout,
        publications: List[TablePublication],
        comparison_query_uris: Dict[str, str],
    ) -> Dict[str, Any]:
        return {
            "run_id": layout.run_id,
            "tables": [self.build_table_entry(publication) for publication in publications],
            "comparison": self.build_comparison_entry(
                layout=layout,
                comparison_query_uris=comparison_query_uris,
            ),
        }
