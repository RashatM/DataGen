import random

from app.application.ports.generator_port import DataGeneratorPort
from app.domain.constraints import BooleanConstraints, OutputConstraints
from app.domain.validation_errors import UnsatisfiableConstraintsError
from app.shared.utils import random_choices_from_constants


class BooleanDataGenerator(DataGeneratorPort[BooleanConstraints]):
    """Генератор булевых значений с поддержкой allowed_values и режима уникальности."""
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def generate_values(
        self,
        total_rows: int,
        constraints: BooleanConstraints,
        output_constraints: OutputConstraints,
    ) -> list[bool]:
        if constraints.allowed_values:
            values = list(dict.fromkeys(constraints.allowed_values))
        else:
            values = [True, False]

        if output_constraints.is_unique:
            if total_rows > len(values):
                raise UnsatisfiableConstraintsError("Not enough unique allowed_values for boolean")
            return self.rng.sample(values, total_rows)

        return random_choices_from_constants(values, total_rows, self.rng)
