from typing import List

from app.core.application.ports.mock_generator_port import IMockDataGenerator
from app.core.domain.constraints import BooleanConstraints
from app.shared.utils import random_choices_from_constants


class BooleanGeneratorMock(IMockDataGenerator[BooleanConstraints]):
    def generate_values(self, total_rows: int, constraints: BooleanConstraints) -> List[bool]:
        if constraints.allowed_values:
            return random_choices_from_constants(constraints.allowed_values, total_rows)
        return random_choices_from_constants([True, False], total_rows)
