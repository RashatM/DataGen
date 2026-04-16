from dataclasses import dataclass

from app.application.constants import WriteMode
from app.domain.entities import TableSpec


@dataclass(frozen=True, slots=True)
class TableLoadSpec:
    """Runtime-контракт загрузки одной таблицы в Hive и Iceberg после публикации parquet-артефакта."""
    hive_target_table: str
    iceberg_target_table: str
    write_mode: WriteMode
    hive_columns: tuple[str, ...]
    iceberg_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ComparisonQuerySpec:
    """Пара comparison-query и списки колонок, которые нужно исключить из сверки по каждому engine."""
    hive_sql: str
    iceberg_sql: str
    hive_exclude_columns: tuple[str, ...] = ()
    iceberg_exclude_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TableExecutionSpec:
    """Связывает доменную таблицу с runtime-настройками её публикации и загрузки."""
    table: TableSpec
    load_spec: TableLoadSpec


@dataclass(frozen=True, slots=True)
class PipelineExecutionSpec:
    """Полная спецификация запуска: какие таблицы генерировать и как сравнивать результат."""
    tables: tuple[TableExecutionSpec, ...]
    comparison: ComparisonQuerySpec
