import random
from abc import abstractmethod
from datetime import datetime
from typing import List, TypeVar, Generic

from app.dto.constraints import TimestampConstraints
from app.interfaces.mock_generator import IMockDataGenerator
from app.shared.utils import random_choices_from_constants

T = TypeVar('T', str, datetime)

class BaseTimestampGeneratorMock(IMockDataGenerator, Generic[T]):
    @staticmethod
    @abstractmethod
    def calculate_offset_timestamp(start_ts: datetime, offset_days: int, ts_format: str = None) -> T:
        pass


    def generate_values(self, total_rows: int, constraints: TimestampConstraints) -> List[T]:
        start_ts = constraints.min_timestamp
        end_ts = constraints.max_timestamp
        delta_seconds = int((end_ts - start_ts).total_seconds())

        if constraints.is_unique:
            if delta_seconds + 1 < total_rows:
                raise ValueError("Недостаточно уникальных значений timestamp в указанном диапазоне.")

            all_possible_timestamps = [
                self.calculate_offset_timestamp(
                    start_ts=start_ts,
                    offset_days=i,
                    ts_format=constraints.timestamp_format
                ) for i in range(delta_seconds + 1)
            ]
            return random.sample(all_possible_timestamps, total_rows)

        if constraints.allowed_values:
            values = random_choices_from_constants(constraints.allowed_values, total_rows)
        else:
            values = [
                self.calculate_offset_timestamp(
                    start_ts=start_ts,
                    offset_days=random.randint(0, delta_seconds),
                    ts_format=constraints.timestamp_format
                ) for _ in range(total_rows)
            ]

        return values
