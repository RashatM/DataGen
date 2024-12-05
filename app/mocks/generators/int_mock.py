import random
from typing import List

from app.dto.constraints import IntConstraints
from app.interfaces.mock_generator import IMockDataGenerator
from app.utils import random_choices_from_constants


class IntGeneratorMock(IMockDataGenerator):


    def generate_values(self, total_rows: int, constraints: IntConstraints) -> List[int]:
        min_value = max(constraints.min_value, constraints.greater_than)
        max_value = min(constraints.max_value, constraints.less_than)

        if constraints.is_unique:
            if max_value - min_value + 1 < total_rows:
                raise ValueError("Недостаточно уникальных значений в указанном диапазоне.")
            return random.sample(range(min_value, max_value + 1), total_rows)

        if constraints.allowed_values:
            values = random_choices_from_constants(constraints.allowed_values, total_rows)
        else:
            values = [random.randint(min_value, max_value) for _ in range(total_rows)]

        return values