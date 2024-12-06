from enum import Enum


class RelationType(Enum):
    ONE_TO_ONE: str = "ONE_TO_ONE"
    ONE_TO_MANY: str = "ONE_TO_MANY"


class DataType(Enum):
    STRING: str = "STRING"
    INT: str = "INT"
    FLOAT: str = "FLOAT"
    DATE: str = "DATE"
    TIMESTAMP: str = "TIMESTAMP"
    BOOLEAN: str = "BOOLEAN"


class SourceType(Enum):
    POSTGRES: str = "POSTGRES"
    ORACLE: str = "ORACLE"
