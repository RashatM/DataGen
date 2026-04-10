from enum import Enum


class ExecutionStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    WAIT_TIMEOUT = "wait_timeout"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ComparisonStatus(Enum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"


class EngineName(str, Enum):
    HIVE = "hive"
    ICEBERG = "iceberg"


class WriteMode(str, Enum):
    OVERWRITE_TABLE = "OVERWRITE_TABLE"
    OVERWRITE_PARTITIONS = "OVERWRITE_PARTITIONS"
    APPEND = "APPEND"
    APPEND_DISTINCT_PARTITIONS = "APPEND_DISTINCT_PARTITIONS"
