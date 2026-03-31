from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from app.core.domain.enums import CaseMode, CharacterSet
from app.core.domain.validation_errors import InvalidConstraintsError


@dataclass
class Constraints:
    allowed_values: tuple[Any, ...] | None


@dataclass
class ValueConstraints(Constraints):
    pass


@dataclass
class OutputConstraints:
    null_ratio: float = 0.0
    is_unique: bool = False


@dataclass
class StringConstraints(ValueConstraints):
    length: int
    regular_expr: str | None
    character_set: CharacterSet = CharacterSet.LETTERS
    case_mode: CaseMode = CaseMode.MIXED


@dataclass
class IntConstraints(ValueConstraints):
    min_value: int
    max_value: int
    digits_count: int | None = None

    @staticmethod
    def range_from_digits_count(digits_count: int) -> tuple[int, int]:
        if isinstance(digits_count, bool) or not isinstance(digits_count, int):
            raise InvalidConstraintsError("digits_count must be integer")
        if digits_count <= 0:
            raise InvalidConstraintsError("digits_count must be greater than 0")
        if digits_count == 1:
            return 0, 9
        return 10 ** (digits_count - 1), 10 ** digits_count - 1

    def __post_init__(self) -> None:
        if self.max_value < self.min_value:
            raise InvalidConstraintsError("max_value must be greater than or equal to min_value")

        if self.digits_count is not None:
            if self.allowed_values is not None:
                raise InvalidConstraintsError("digits_count cannot be used together with allowed_values")

            expected_min_value, expected_max_value = self.range_from_digits_count(self.digits_count)
            if self.min_value != expected_min_value or self.max_value != expected_max_value:
                raise InvalidConstraintsError("digits_count must match resolved min_value/max_value")


@dataclass
class FloatConstraints(ValueConstraints):
    min_value: float
    max_value: float
    precision: int


@dataclass
class DateConstraints(ValueConstraints):
    min_date: date
    max_date: date
    date_format: str = "%Y-%m-%d"


@dataclass
class TimestampConstraints(ValueConstraints):
    min_timestamp: datetime
    max_timestamp: datetime
    timestamp_format: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class BooleanConstraints(Constraints):
    pass
