from dataclasses import dataclass, field

from app.core.application.constants import EngineName


@dataclass(slots=True)
class RunArtifactKeyLayout:
    run_id: str
    run_prefix: str = field(init=False)
    comparison_report_key: str = field(init=False)

    def __post_init__(self) -> None:
        run_prefix = f"runs/{self.run_id.strip('/')}"
        self.run_prefix = run_prefix
        self.comparison_report_key = f"{run_prefix}/result/comparison_result.json"

    def data_key(self, table_name: str) -> str:
        return f"{self.run_prefix}/tables/{table_name.strip('/')}/data/data.parquet"

    def comparison_query_result_key(self, engine_name: EngineName) -> str:
        return f"{self.run_prefix}/comparison/query_results/{engine_name.value}"

    def comparison_query_key(self, engine_name: EngineName) -> str:
        return f"{self.run_prefix}/comparison/queries/{engine_name.value}.sql"


@dataclass(slots=True)
class TableStateKeyLayout:
    table_name: str
    pointer_key: str = field(init=False)

    def __post_init__(self) -> None:
        pointer_key = f"tables/{self.table_name.strip('/')}/pointer.json"
        self.pointer_key = pointer_key
