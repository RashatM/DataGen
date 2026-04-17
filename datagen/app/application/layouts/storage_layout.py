from dataclasses import dataclass, field

from app.application.constants import EngineName

RUNS_ROOT_PREFIX = "runs"
TABLES_ROOT_PREFIX = "tables"


@dataclass(slots=True)
class RunArtifactKeyLayout:
    """Вычисляет S3-ключи всех артефактов, относящихся к одному run."""
    run_id: str
    run_prefix: str = field(init=False)
    comparison_report_key: str = field(init=False)
    diagnostics_prefix: str = field(init=False)

    def __post_init__(self) -> None:
        run_prefix = f"{RUNS_ROOT_PREFIX}/{self.run_id.strip('/')}"
        self.run_prefix = run_prefix
        self.comparison_report_key = f"{run_prefix}/result/comparison_result.json"
        self.diagnostics_prefix = f"{run_prefix}/result/diagnostics"

    def data_key(self, table_name: str) -> str:
        return f"{self.run_prefix}/tables/{table_name.strip('/')}/data/data.parquet"

    def comparison_query_result_key(self, engine_name: EngineName) -> str:
        return f"{self.run_prefix}/comparison/query_results/{engine_name.value}"

    def comparison_query_key(self, engine_name: EngineName) -> str:
        return f"{self.run_prefix}/comparison/queries/{engine_name.value}.sql"

    def diagnostic_key(self, task_id: str) -> str:
        return f"{self.diagnostics_prefix}/{task_id.strip('/')}.json"


@dataclass(slots=True)
class TableStateKeyLayout:
    """Вычисляет S3-ключи состояния, живущего на уровне одной таблицы, а не целого run."""
    table_name: str
    pointer_key: str = field(init=False)

    def __post_init__(self) -> None:
        pointer_key = f"{TABLES_ROOT_PREFIX}/{self.table_name.strip('/')}/pointer.json"
        self.pointer_key = pointer_key
