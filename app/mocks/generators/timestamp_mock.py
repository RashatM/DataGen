import random
from datetime import timedelta, datetime
from typing import List


from app.dto.constraints import TimestampConstraints
from app.interfaces.mock_generator import IMockDataGenerator
from app.utils import random_choices_from_constants


class TimestampGeneratorMock(IMockDataGenerator):


    def generate_values(self, total_rows: int, constraints: TimestampConstraints) -> List[datetime]:
        start_ts = max(constraints.min_timestamp, constraints.greater_than)
        end_ts = min(constraints.max_timestamp, constraints.less_than)
        delta_seconds = int((end_ts - start_ts).total_seconds())

        if constraints.is_unique:
            if delta_seconds + 1 < total_rows:
                raise ValueError("Недостаточно уникальных значений timestamp в указанном диапазоне.")

            all_possible_timestamps = [
                (start_ts + timedelta(seconds=i)).strftime(constraints.timestamp_format)
                for i in range(delta_seconds + 1)
            ]
            return random.sample(all_possible_timestamps, total_rows)

        if constraints.allowed_values:
            values = random_choices_from_constants(constraints.allowed_values, total_rows)
        else:
            values = [
                (start_ts + timedelta(seconds=random.randint(0, delta_seconds))).strftime(constraints.timestamp_format)
                for _ in range(total_rows)
            ]

        return values
