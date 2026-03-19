from enum import Enum


class ExecutionStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ComparisonStatus(Enum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
