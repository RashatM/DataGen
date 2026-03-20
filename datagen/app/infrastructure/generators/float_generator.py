import random
from typing import List

from app.core.application.ports.generator_port import IDataGenerator
from app.core.domain.constraints import FloatConstraints, OutputConstraints
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.utils import random_choices_from_constants


class FloatDataGenerator(IDataGenerator[FloatConstraints]):
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def generate_values(
        self,
        total_rows: int,
        constraints: FloatConstraints,
        output_constraints: OutputConstraints,
    ) -> List[float]:
        min_value = constraints.min_value
        max_value = constraints.max_value
        precision = constraints.precision
        if max_value < min_value:
            raise InvalidConstraintsError("max_value must be greater than or equal to min_value")
        if precision < 0:
            raise InvalidConstraintsError("precision must be greater than or equal to 0")

        if constraints.allowed_values:
            if output_constraints.is_unique:
                unique_values = list(dict.fromkeys(constraints.allowed_values))
                if len(unique_values) < total_rows:
                    raise UnsatisfiableConstraintsError("Not enough unique allowed_values for float")
                return self.rng.sample(unique_values, total_rows)
            return random_choices_from_constants(list(constraints.allowed_values), total_rows, self.rng)

        if not output_constraints.is_unique:
            return [round(self.rng.uniform(min_value, max_value), precision) for _ in range(total_rows)]

        scale = 10 ** precision
        min_scaled = int(round(min_value * scale))
        max_scaled = int(round(max_value * scale))
        total_possible = max_scaled - min_scaled + 1

        if total_possible < total_rows:
            raise UnsatisfiableConstraintsError(
                "Not enough unique float values for specified range and precision"
            )

        selected = self.rng.sample(range(min_scaled, max_scaled + 1), total_rows)
        return [value / scale for value in selected]
