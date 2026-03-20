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

    def data_key(self, schema_name: str, table_name: str) -> str:
        return f"{self.run_prefix}/{schema_name.strip('/')}/{table_name.strip('/')}/data/data.parquet"

    def ddl_key(self, schema_name: str, table_name: str, engine_name: EngineName) -> str:
        return f"{self.run_prefix}/{schema_name.strip('/')}/{table_name.strip('/')}/ddl/{engine_name.value}.sql"

    def engine_result_key(self, engine_name: EngineName) -> str:
        return f"{self.run_prefix}/result/query/{engine_name.value}.parquet"

    def comparison_query_key(self, engine_name: EngineName) -> str:
        return f"{self.run_prefix}/comparison/{engine_name.value}.sql"


@dataclass(slots=True)
class TableStateKeyLayout:
    schema_name: str
    table_name: str
    pointer_key: str = field(init=False)

    def __post_init__(self) -> None:
        pointer_key = f"tables/{self.schema_name.strip('/')}/{self.table_name.strip('/')}/pointer.json"
        self.pointer_key = pointer_key
