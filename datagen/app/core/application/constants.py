from enum import Enum


class ExecutionStatus(Enum):
    """Технический статус выполнения внешнего runner-а."""
    SUCCESS = "success"
    FAILED = "failed"
    WAIT_TIMEOUT = "wait_timeout"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ComparisonStatus(Enum):
    """Бизнес-статус сверки результатов двух движков."""
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"


class EngineName(str, Enum):
    """Поддерживаемые execution engines проекта."""
    HIVE = "hive"
    ICEBERG = "iceberg"


class WriteMode(str, Enum):
    """Поддерживаемые режимы загрузки generated данных в целевые таблицы."""
    OVERWRITE_TABLE = "OVERWRITE_TABLE"
    OVERWRITE_PARTITIONS = "OVERWRITE_PARTITIONS"
    APPEND = "APPEND"
    APPEND_DISTINCT_PARTITIONS = "APPEND_DISTINCT_PARTITIONS"
