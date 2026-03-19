from dataclasses import dataclass
from typing import Dict, List

from app.core.application.dto.publication import TablePublication


@dataclass(frozen=True, slots=True)
class RunArtifactLayout:
    run_id: str

    @property
    def run_prefix(self) -> str:
        return f"runs/{self.run_id.strip('/')}"

    def data_key(self, schema_name: str, table_name: str) -> str:
        return f"{self.run_prefix}/{schema_name.strip('/')}/{table_name.strip('/')}/data/data.parquet"

    def ddl_key(self, schema_name: str, table_name: str, engine_name: str) -> str:
        return f"{self.run_prefix}/{schema_name.strip('/')}/{table_name.strip('/')}/ddl/{engine_name}.sql"

    def comparison_report_key(self) -> str:
        return f"{self.run_prefix}/result/comparison_result.json"

    def engine_result_key(self, engine_name: str) -> str:
        return f"{self.run_prefix}/result/query/{engine_name}.parquet"

    def comparison_query_key(self, engine_name: str) -> str:
        return f"{self.run_prefix}/comparison/{engine_name}.sql"


@dataclass(slots=True)
class ArtifactPublicationResult:
    table_publications: List[TablePublication]
    comparison_query_uris: Dict[str, str]
