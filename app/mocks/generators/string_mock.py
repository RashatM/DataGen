import random
import string
from typing import List
import rstr

from app.dto.constraints import StringConstraints
from app.enums import CharacterSet, CaseMode
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
                    if constraints.character_set == CharacterSet.DIGITS:
                        char_pool = string.digits
                    elif constraints.character_set == CharacterSet.ALPHANUMERIC:
                        char_pool = string.ascii_letters + string.digits
                    else:
                        char_pool = string.ascii_letters

                    value = "".join(random.choices(char_pool, k=constraints.length))

                if constraints.case_mode == CaseMode.LOWER:
                    value = value.lower()
                elif constraints.case_mode == CaseMode.UPPER:
                    value = value.upper()

                if constraints.is_unique:
                    value += str(i)

            values.append(value)

        return values
