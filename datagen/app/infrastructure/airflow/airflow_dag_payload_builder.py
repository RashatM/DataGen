from typing import Any

from app.application.constants import EngineName
from app.application.dto.pipeline import ComparisonQuerySpec
from app.application.layouts.storage_layout import RunArtifactKeyLayout
from app.application.dto.publication import EnginePair, TablePublication
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter


class AirflowDagPayloadBuilder:
    """Собирает компактный runtime-contract для Airflow DAG из опубликованных артефактов и execution spec."""
    DIAGNOSTIC_TASK_IDS = (
        "load_iceberg_tables",
        "load_hive_tables",
        "compare_query_results",
    )

    def __init__(self, object_storage: S3StorageAdapter) -> None:
        self.object_storage = object_storage

    @staticmethod
    def build_table_entry(publication: TablePublication) -> dict[str, Any]:
        """Сериализует одну опубликованную таблицу в engine-specific load contract для DAG."""
        load_spec = publication.load_spec
        return {
            "table_name": publication.table_name,
            "data_uri": publication.data_uri,
            "load": {
                "hive": {
                    "target_table_name": load_spec.hive_target_table,
                    "write_mode": load_spec.write_mode.value,
                    "columns": list(load_spec.hive_columns),
                },
                "iceberg": {
                    "target_table_name": load_spec.iceberg_target_table,
                    "write_mode": load_spec.write_mode.value,
                    "columns": list(load_spec.iceberg_columns),
                },
            },
        }

    def build_diagnostics_entry(self, artifact_layout: RunArtifactKeyLayout) -> dict[str, Any]:
        return {
            "uris": {
                task_id: self.object_storage.build_uri(artifact_layout.diagnostic_key(task_id))
                for task_id in self.DIAGNOSTIC_TASK_IDS
            },
        }

    def build_comparison_entry(
        self,
        artifact_layout: RunArtifactKeyLayout,
        comparison_spec: ComparisonQuerySpec,
        comparison_query_uris: EnginePair[str],
    ) -> dict[str, Any]:
        """Строит comparison-блок payload: query/result URIs, excludes и report URI."""
        return {
            "query_uris": {
                engine_name.value: comparison_query_uris.get_value(engine_name)
                for engine_name in EngineName
            },
            "exclude_columns": {
                EngineName.HIVE.value: list(comparison_spec.hive_exclude_columns),
                EngineName.ICEBERG.value: list(comparison_spec.iceberg_exclude_columns),
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
        comparison_spec: ComparisonQuerySpec,
        comparison_query_uris: EnginePair[str],
    ) -> dict[str, Any]:
        """Собирает полный payload запуска DAG для одного run."""
        return {
            "run_id": artifact_layout.run_id,
            "tables": [self.build_table_entry(publication) for publication in publications],
            "diagnostics": self.build_diagnostics_entry(artifact_layout),
            "comparison": self.build_comparison_entry(
                artifact_layout=artifact_layout,
                comparison_spec=comparison_spec,
                comparison_query_uris=comparison_query_uris,
            ),
        }
