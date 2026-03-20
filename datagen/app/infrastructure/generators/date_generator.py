import random
from datetime import date, timedelta
from typing import List

from app.core.application.ports.generator_port import IDataGenerator
from app.core.domain.constraints import DateConstraints, OutputConstraints
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.utils import random_choices_from_constants


class DateDataGenerator(IDataGenerator[DateConstraints]):
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def generate_values(
        self,
        total_rows: int,
        constraints: DateConstraints,
        output_constraints: OutputConstraints,
    ) -> List[date]:
        start_date = constraints.min_date
        end_date = constraints.max_date
        if end_date < start_date:
            raise InvalidConstraintsError("max_date must be greater than or equal to min_date")
        delta_days = (end_date - start_date).days

        if constraints.allowed_values:
            if output_constraints.is_unique:
                unique_values = list(dict.fromkeys(constraints.allowed_values))
                if len(unique_values) < total_rows:
                    raise UnsatisfiableConstraintsError("Not enough unique allowed_values for date")
                return self.rng.sample(unique_values, total_rows)
            return random_choices_from_constants(list(constraints.allowed_values), total_rows, self.rng)

        if output_constraints.is_unique:
            if delta_days + 1 < total_rows:
                raise UnsatisfiableConstraintsError("Not enough unique dates in the specified range")
            sampled_offsets = self.rng.sample(range(delta_days + 1), total_rows)
            return [start_date + timedelta(days=offset) for offset in sampled_offsets]

        return [start_date + timedelta(days=self.rng.randint(0, delta_days)) for _ in range(total_rows)]
