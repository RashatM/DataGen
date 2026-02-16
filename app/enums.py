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
    DATE_IN_STRING: str = "DATE_STRING"
    TIMESTAMP_IN_STRING: str = "TIMESTAMP_STRING"


class SourceType(Enum):
    POSTGRES: str = "POSTGRES"
    ORACLE: str = "ORACLE"


class CharacterSet(Enum):
    DIGITS = "digits"
    LETTERS = "letters"
    ALPHANUMERIC = "alphanumeric"

class CaseMode(Enum):
    LOWER = "lower"
    UPPER = "upper"
    MIXED = "mixed"
