from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Any
from enum import Enum

from app.enums import CharacterSet, CaseMode


@dataclass
class Constraints:
    null_ratio: int
    allowed_values: List[Any]



@dataclass
class ValueConstraints(Constraints):
    is_unique: bool


@dataclass
class StringConstraints(ValueConstraints):
    length: int
    regular_expr: str
    character_set: CharacterSet = CharacterSet.LETTERS
    case_mode: CaseMode = CaseMode.MIXED


@dataclass
class IntConstraints(ValueConstraints):
    min_value: int
    max_value: int

@dataclass
class FloatConstraints(ValueConstraints):
    min_value: float
    max_value: float
    precision: int


@dataclass
class DateConstraints(ValueConstraints):
    min_date: date
    max_date: date
    date_format: str


@dataclass
class TimestampConstraints(ValueConstraints):
    min_timestamp: datetime = "2024-01-01 00:00:00"
    max_timestamp: datetime = "2024-12-31 00:00:00"
    timestamp_format: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class BooleanConstraints(Constraints):
    pass
