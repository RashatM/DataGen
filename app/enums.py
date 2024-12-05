from enum import Enum


class RelationType(Enum):
    ONE_TO_ONE: str = "ONE_TO_ONE"
    MANY_TO_ONE: str = "MANY_TO_ONE"


class DataType(Enum):
    STRING: str = "STRING"
    INT: str = "INT"
    FLOAT: str = "FLOAT"
    DATE: str = "DATE"
    TIMESTAMP: str = "TIMESTAMP"
    BOOLEAN: str = "BOOLEAN"


class DataBaseType(Enum):
    POSTGRES: str = "POSTGRES"
    ORACLE: str = "ORACLE"
