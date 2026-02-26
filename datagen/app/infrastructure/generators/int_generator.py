import random
from typing import List

from app.core.application.ports.mock_generator_port import IMockDataGenerator
from app.core.domain.constraints import IntConstraints
from app.shared.utils import random_choices_from_constants


class IntGeneratorMock(IMockDataGenerator[IntConstraints]):
    def generate_values(self, total_rows: int, constraints: IntConstraints) -> List[int]:
        min_value = constraints.min_value
        max_value = constraints.max_value
        if max_value < min_value:
            raise ValueError("max_value must be greater than or equal to min_value")

        if constraints.allowed_values:
            if constraints.is_unique:
                unique_values = list(dict.fromkeys(constraints.allowed_values))
                if len(unique_values) < total_rows:
                    raise ValueError("Недостаточно уникальных allowed_values для int.")
                return random.sample(unique_values, total_rows)
            return random_choices_from_constants(constraints.allowed_values, total_rows)

        if constraints.is_unique:
            if max_value - min_value + 1 < total_rows:
                raise ValueError("Недостаточно уникальных значений в указанном диапазоне.")
            return random.sample(range(min_value, max_value + 1), total_rows)

        return [random.randint(min_value, max_value) for _ in range(total_rows)]
