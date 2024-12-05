import random
from typing import List

from app.dto.constraints import IntConstraints, BooleanConstraints
from app.interfaces.mock_generator import IMockDataGenerator
from app.utils import random_choices_from_constants


class BooleanGeneratorMock(IMockDataGenerator):


    def generate_values(self, total_rows: int, constraints: BooleanConstraints) -> List[int]:
        if constraints.allowed_values:
            return random_choices_from_constants(constraints.allowed_values, total_rows)
        return [random.choice([True, False]) for _ in range(total_rows)]
