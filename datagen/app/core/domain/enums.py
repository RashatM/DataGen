from enum import Enum


class RelationType(Enum):
    ONE_TO_ONE = "ONE_TO_ONE"
    ONE_TO_MANY = "ONE_TO_MANY"


class DataType(Enum):
    STRING = "STRING"
    INT = "INT"
    FLOAT = "FLOAT"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"

class CharacterSet(Enum):
    DIGITS = "digits"
    LETTERS = "letters"
    ALPHANUMERIC = "alphanumeric"


class CaseMode(Enum):
    LOWER = "lower"
    UPPER = "upper"
    MIXED = "mixed"
