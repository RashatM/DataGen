import random
from datetime import date, timedelta
from typing import List

from app.core.application.ports.mock_generator_port import IMockDataGenerator
from app.core.domain.constraints import DateConstraints
from app.shared.utils import random_choices_from_constants


class DateGeneratorMock(IMockDataGenerator[DateConstraints]):
    def generate_values(self, total_rows: int, constraints: DateConstraints) -> List[date]:
        start_date = constraints.min_date
        end_date = constraints.max_date
        if end_date < start_date:
            raise ValueError("max_date must be greater than or equal to min_date")
        delta_days = (end_date - start_date).days

        if constraints.allowed_values:
            if constraints.is_unique:
                unique_values = list(dict.fromkeys(constraints.allowed_values))
                if len(unique_values) < total_rows:
                    raise ValueError("Недостаточно уникальных allowed_values для date.")
                return random.sample(unique_values, total_rows)
            return random_choices_from_constants(constraints.allowed_values, total_rows)

        if constraints.is_unique:
            if delta_days + 1 < total_rows:
                raise ValueError("Недостаточно уникальных дат в указанном диапазоне.")
            sampled_offsets = random.sample(range(delta_days + 1), total_rows)
            return [start_date + timedelta(days=offset) for offset in sampled_offsets]

        return [start_date + timedelta(days=random.randint(0, delta_days)) for _ in range(total_rows)]
