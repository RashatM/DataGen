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
