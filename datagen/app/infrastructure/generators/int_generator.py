import random

from app.core.application.ports.generator_port import DataGeneratorPort
from app.core.domain.constraints import IntConstraints, OutputConstraints
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.utils import random_choices_from_constants


class IntDataGenerator(DataGeneratorPort[IntConstraints]):
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def generate_values(
        self,
        total_rows: int,
        constraints: IntConstraints,
        output_constraints: OutputConstraints,
    ) -> list[int]:
        min_value = constraints.min_value
        max_value = constraints.max_value
        if max_value < min_value:
            raise InvalidConstraintsError("max_value must be greater than or equal to min_value")

        if constraints.allowed_values:
            if output_constraints.is_unique:
                unique_values = list(dict.fromkeys(constraints.allowed_values))
                if len(unique_values) < total_rows:
                    raise UnsatisfiableConstraintsError("Not enough unique allowed_values for int")
                return self.rng.sample(unique_values, total_rows)
            return random_choices_from_constants(list(constraints.allowed_values), total_rows, self.rng)

        if output_constraints.is_unique:
            if max_value - min_value + 1 < total_rows:
                raise UnsatisfiableConstraintsError("Not enough unique values in the specified int range")
            return self.rng.sample(range(min_value, max_value + 1), total_rows)

        return [self.rng.randint(min_value, max_value) for _ in range(total_rows)]
