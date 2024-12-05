import random
from datetime import timedelta, date
from typing import List


from app.dto.constraints import DateConstraints
from app.interfaces.mock_generator import IMockDataGenerator
from app.utils import random_choices_from_constants


class DateGeneratorMock(IMockDataGenerator):


    def generate_values(self, total_rows: int, constraints: DateConstraints) -> List[date]:
        start_date = max(constraints.min_date, constraints.greater_than)
        end_date = min(constraints.max_date, constraints.less_than)
        delta_days = (end_date - start_date).days

        if constraints.is_unique:
            if delta_days + 1 < total_rows:
                raise ValueError("Недостаточно уникальных дат в указанном диапазоне.")

            all_possible_dates = [
                (start_date + timedelta(days=i)).strftime(constraints.date_format)
                for i in range(delta_days + 1)
            ]
            return random.sample(all_possible_dates, total_rows)

        if constraints.allowed_values:
            values = random_choices_from_constants(constraints.allowed_values, total_rows)
        else:
            values = [
                (start_date + timedelta(days=random.randint(0, delta_days))).strftime(constraints.date_format)
                for _ in range(total_rows)
            ]

        return values
