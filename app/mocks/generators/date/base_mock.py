import random
from abc import abstractmethod
from datetime import date
from typing import List, TypeVar, Generic

from app.dto.constraints import DateConstraints
from app.interfaces.mock_generator import IMockDataGenerator
from app.utils import random_choices_from_constants

T = TypeVar('T', str, date)


class BaseDateGeneratorMock(IMockDataGenerator, Generic[T]):
    @staticmethod
    @abstractmethod
    def calculate_offset_date(start_date: date, offset_days: int, date_format: str = None) -> str:
        pass

    def generate_values(self, total_rows: int, constraints: DateConstraints) -> List[T]:
        start_date =constraints.min_date
        end_date = constraints.max_date
        delta_days = (end_date - start_date).days

        if constraints.is_unique:
            if delta_days + 1 < total_rows:
                raise ValueError("Недостаточно уникальных дат в указанном диапазоне.")

            all_possible_dates = [
                self.calculate_offset_date(start_date, i, constraints.date_format) for i in range(delta_days)
            ]
            return random.sample(all_possible_dates, total_rows)

        if constraints.allowed_values:
            values = random_choices_from_constants(constraints.allowed_values, total_rows)
        else:
            values = [
                self.calculate_offset_date(
                    start_date=start_date,
                    offset_days=random.randint(0, delta_days),
                    date_format=constraints.date_format
                ) for _ in range(total_rows)
            ]

        return values
