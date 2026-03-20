import random
from typing import List

from app.core.application.ports.generator_port import IDataGenerator
from app.core.domain.constraints import BooleanConstraints, OutputConstraints
from app.core.domain.validation_errors import UnsatisfiableConstraintsError
from app.shared.utils import random_choices_from_constants


class BooleanDataGenerator(IDataGenerator[BooleanConstraints]):
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def generate_values(
        self,
        total_rows: int,
        constraints: BooleanConstraints,
        output_constraints: OutputConstraints,
    ) -> List[bool]:
        if constraints.allowed_values:
            values = list(dict.fromkeys(constraints.allowed_values))
        else:
            values = [True, False]

        if output_constraints.is_unique:
            if total_rows > len(values):
                raise UnsatisfiableConstraintsError("Not enough unique allowed_values for boolean")
            return self.rng.sample(values, total_rows)

        return random_choices_from_constants(values, total_rows, self.rng)
