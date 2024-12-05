import random
from typing import List


from app.dto.constraints import FloatConstraints
from app.interfaces.mock_generator import IMockDataGenerator
from app.utils import random_choices_from_constants


class FloatGeneratorMock(IMockDataGenerator):


    def generate_values(self, total_rows: int, constraints: FloatConstraints) -> List[float]:
        step = 10 ** -constraints.precision

        min_value = max(constraints.min_value, constraints.greater_than)
        max_value = min(constraints.max_value, constraints.less_than)

        if constraints.is_unique:
            total_possible_values = int((max_value - min_value) / step)
            if total_possible_values < total_rows:
                raise ValueError("Недостаточно уникальных значений в указанном диапазоне для указанной точности.")

            return random.sample(
                [round(min_value + i * step, constraints.precision) for i in range(total_possible_values)],
                total_rows
            )

        if constraints.allowed_values:
            values = random_choices_from_constants(constraints.allowed_values, total_rows)
        else:
            values = [round(random.uniform(min_value, max_value), constraints.precision) for _ in range(total_rows)]


        return values
