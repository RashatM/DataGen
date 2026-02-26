import random
from datetime import datetime, timedelta
from typing import List

from app.core.application.ports.mock_generator_port import IMockDataGenerator
from app.core.domain.constraints import TimestampConstraints
from app.shared.utils import random_choices_from_constants


class TimestampGeneratorMock(IMockDataGenerator[TimestampConstraints]):
    def generate_values(self, total_rows: int, constraints: TimestampConstraints) -> List[datetime]:
        start_ts = constraints.min_timestamp
        end_ts = constraints.max_timestamp
        if end_ts < start_ts:
            raise ValueError("max_timestamp must be greater than or equal to min_timestamp")
        delta_seconds = int((end_ts - start_ts).total_seconds())

        if constraints.allowed_values:
            if constraints.is_unique:
                unique_values = list(dict.fromkeys(constraints.allowed_values))
                if len(unique_values) < total_rows:
                    raise ValueError("Недостаточно уникальных allowed_values для timestamp.")
                return random.sample(unique_values, total_rows)
            return random_choices_from_constants(constraints.allowed_values, total_rows)

        if constraints.is_unique:
            if delta_seconds + 1 < total_rows:
                raise ValueError("Недостаточно уникальных значений timestamp в указанном диапазоне.")
            sampled_offsets = random.sample(range(delta_seconds + 1), total_rows)
            return [start_ts + timedelta(seconds=offset) for offset in sampled_offsets]

        return [start_ts + timedelta(seconds=random.randint(0, delta_seconds)) for _ in range(total_rows)]
