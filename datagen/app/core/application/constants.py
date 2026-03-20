from enum import Enum


class ExecutionStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ComparisonStatus(Enum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"


class EngineName(str, Enum):
    HIVE = "hive"
    ICEBERG = "iceberg"

    @classmethod
    def all(cls) -> tuple["EngineName", "EngineName"]:
        return cls.HIVE, cls.ICEBERG
