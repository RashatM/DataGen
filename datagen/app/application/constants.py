from enum import Enum


class ExecutionStatus(Enum):
    """Технический статус выполнения внешнего runner-а."""
    SUCCESS = "success"
    FAILED = "failed"
    WAIT_TIMEOUT = "wait_timeout"


class ComparisonStatus(Enum):
    """Бизнес-статус сверки результатов двух движков."""
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"


class EngineName(str, Enum):
    """Поддерживаемые execution engines проекта."""
    HIVE = "hive"
    ICEBERG = "iceberg"


class EngineScope(str, Enum):
    """Input-level правило, в какой engine-specific список загрузки должна попасть колонка."""
    ALL = "ALL"
    HIVE_ONLY = "HIVE_ONLY"
    ICEBERG_ONLY = "ICEBERG_ONLY"


class WriteMode(str, Enum):
    """Поддерживаемые режимы загрузки generated данных в целевые таблицы."""
    OVERWRITE_TABLE = "OVERWRITE_TABLE"
    OVERWRITE_PARTITIONS = "OVERWRITE_PARTITIONS"
    APPEND = "APPEND"
