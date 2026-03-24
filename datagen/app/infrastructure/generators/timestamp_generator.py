import random
from datetime import datetime, timedelta

from app.core.application.ports.generator_port import DataGeneratorPort
from app.core.domain.constraints import OutputConstraints, TimestampConstraints
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.utils import random_choices_from_constants


class TimestampDataGenerator(DataGeneratorPort[TimestampConstraints]):
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def generate_values(
        self,
        total_rows: int,
        constraints: TimestampConstraints,
        output_constraints: OutputConstraints,
    ) -> list[datetime]:
        start_ts = constraints.min_timestamp
        end_ts = constraints.max_timestamp
        if end_ts < start_ts:
            raise InvalidConstraintsError("max_timestamp must be greater than or equal to min_timestamp")
        delta_seconds = int((end_ts - start_ts).total_seconds())

        if constraints.allowed_values:
            if output_constraints.is_unique:
                unique_values = list(dict.fromkeys(constraints.allowed_values))
                if len(unique_values) < total_rows:
                    raise UnsatisfiableConstraintsError("Not enough unique allowed_values for timestamp")
                return self.rng.sample(unique_values, total_rows)
            return random_choices_from_constants(list(constraints.allowed_values), total_rows, self.rng)

        if output_constraints.is_unique:
            if delta_seconds + 1 < total_rows:
                raise UnsatisfiableConstraintsError("Not enough unique timestamps in the specified range")
            sampled_offsets = self.rng.sample(range(delta_seconds + 1), total_rows)
            return [start_ts + timedelta(seconds=offset) for offset in sampled_offsets]

        return [start_ts + timedelta(seconds=self.rng.randint(0, delta_seconds)) for _ in range(total_rows)]
