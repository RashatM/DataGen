from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.core.domain.enums import CaseMode, CharacterSet
from app.core.domain.validation_errors import InvalidConstraintsError


@dataclass
class Constraints:
    """Базовый контейнер ограничений, общих для всех типов генерации."""
    allowed_values: tuple[Any, ...] | None


@dataclass
class ValueConstraints(Constraints):
    """Базовый класс ограничений для типов, где допускаются конкретные значения."""
    pass


@dataclass
class OutputConstraints:
    """Ограничения уже на выходные значения колонки: nullable и уникальность."""
    null_ratio: float = 0.0
    is_unique: bool = False


@dataclass
class StringConstraints(ValueConstraints):
    """Ограничения для строкового генератора: длина, regex и допустимый набор символов."""
    length: int
    regular_expr: str | None
    character_set: CharacterSet = CharacterSet.LETTERS
    case_mode: CaseMode = CaseMode.MIXED


@dataclass
class IntConstraints(ValueConstraints):
    """Ограничения для целочисленного генератора в виде замкнутого диапазона."""
    min_value: int
    max_value: int

    def __post_init__(self) -> None:
        if self.max_value < self.min_value:
            raise InvalidConstraintsError("max_value must be greater than or equal to min_value")


@dataclass
class FloatConstraints(ValueConstraints):
    """Ограничения для генерации вещественных чисел с заданной точностью округления."""
    min_value: float
    max_value: float
    precision: int


@dataclass
class DateConstraints(ValueConstraints):
    """Ограничения для дат в виде диапазона и формата представления источника."""
    min_date: date
    max_date: date
    date_format: str = "%Y-%m-%d"


@dataclass
class TimestampConstraints(ValueConstraints):
    """Ограничения для timestamp-значений в виде диапазона и строкового формата источника."""
    min_timestamp: datetime
    max_timestamp: datetime
    timestamp_format: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class BooleanConstraints(Constraints):
    """Ограничения для bool-генератора: либо фиксированный список allowed_values, либо обе константы."""
    pass
