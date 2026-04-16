from enum import Enum


class ReferenceCardinality(Enum):
    """Кардинальность использования значений reference-колонки относительно parent column."""
    ONE_TO_ONE = "ONE_TO_ONE"
    ONE_TO_MANY = "ONE_TO_MANY"


class DataType(Enum):
    """Поддерживаемые логические типы данных внутри генератора."""
    STRING = "STRING"
    INT = "INT"
    FLOAT = "FLOAT"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"


class DerivationRule(str, Enum):
    """Поддерживаемые правила вычисления производных колонок из исходного значения."""
    YYYYMMDD = "YYYYMMDD"
    YYYY = "YYYY"
    MM = "MM"


class CharacterSet(Enum):
    """Наборы символов, из которых строковый генератор может строить значения."""
    DIGITS = "digits"
    LETTERS = "letters"
    ALPHANUMERIC = "alphanumeric"


class CaseMode(Enum):
    """Режим приведения регистра для строковых значений."""
    LOWER = "lower"
    UPPER = "upper"
    MIXED = "mixed"
