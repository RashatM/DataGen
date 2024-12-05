import random
import string
from typing import List
import rstr

from app.dto.constraints import StringConstraints
from app.interfaces.mock_generator import IMockDataGenerator



class StringGeneratorMock(IMockDataGenerator):


    def generate_values(self, total_rows: int, constraints: StringConstraints) -> List[str]:
        values = []

        for i in range(total_rows):
            if constraints.allowed_values:
                value = str(random.choice(constraints.allowed_values))
            else:
                if constraints.regular_expr:
                    value = rstr.xeger(constraints.regular_expr)
                else:
                    value = "".join(random.choices(string.ascii_letters, k=constraints.length))

                if constraints.is_unique:
                    value += str(i)

            if constraints.uppercase:
                value = value.upper()
            elif constraints.lowercase:
                value = value.lower()

            values.append(value)

        return values
