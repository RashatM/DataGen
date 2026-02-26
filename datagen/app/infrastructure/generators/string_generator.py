import random
import string
from typing import List
import rstr

from app.core.application.ports.mock_generator_port import IMockDataGenerator
from app.core.domain.constraints import StringConstraints
from app.core.domain.enums import CaseMode, CharacterSet
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.logger import logger


class StringGeneratorMock(IMockDataGenerator[StringConstraints]):
    @staticmethod
    def apply_case(value: str, mode: CaseMode) -> str:
        if mode == CaseMode.LOWER:
            return value.lower()
        if mode == CaseMode.UPPER:
            return value.upper()
        return value

    @staticmethod
    def build_char_pool(constraints: StringConstraints) -> str:
        if constraints.case_mode == CaseMode.LOWER:
            letters = string.ascii_lowercase
        elif constraints.case_mode == CaseMode.UPPER:
            letters = string.ascii_uppercase
        else:
            letters = string.ascii_letters

        if constraints.character_set == CharacterSet.ALPHANUMERIC:
            return letters + string.digits

        return letters

    @staticmethod
    def encode_index(index: int, pool: str, length: int) -> str:
        base = len(pool)
        chars = [""] * length

        for pos in range(length - 1, -1, -1):
            index, remainder = divmod(index, base)
            chars[pos] = pool[remainder]

        return "".join(chars)

    @staticmethod
    def validate(constraints: StringConstraints) -> None:
        if constraints.length <= 0:
            raise InvalidConstraintsError("Length must be greater than 0")

        if constraints.regular_expr and constraints.is_unique:
            raise InvalidConstraintsError("Unique regex generation is not deterministic and is not supported")

    @staticmethod
    def generate_constant_values(total_rows: int, constraints: StringConstraints) -> List[str]:
        values = list(dict.fromkeys(map(str, constraints.allowed_values)))

        if constraints.is_unique:
            if total_rows > len(values):
                raise UnsatisfiableConstraintsError("Not enough unique allowed values")
            return random.sample(values, total_rows)

        return random.choices(values, k=total_rows)

    def generate_regex_values(self, total_rows: int, constraints: StringConstraints) -> List[str]:
        if total_rows > 10000:
            logger.warning(
                "Generating a large number of regex-based strings may be slow and consume significant memory."
            )

        result = []
        batch_size = 1000
        apply_case = self.apply_case
        regex = constraints.regular_expr
        case_mode = constraints.case_mode

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = []
            for _ in range(start, end):
                value = rstr.xeger(regex)
                batch.append(apply_case(value, case_mode))
            result.extend(batch)

        return result

    @staticmethod
    def generate_digit_values(total_rows: int, constraints: StringConstraints) -> List[str]:
        length = constraints.length
        min_value = 0 if length == 1 else 10 ** (length - 1)
        max_value = 10 ** length - 1
        space = max_value - min_value + 1

        if constraints.is_unique:
            if total_rows > space:
                raise UnsatisfiableConstraintsError("Not enough unique digit combinations")
            sampled_numbers = random.sample(range(min_value, max_value + 1), total_rows)
            return [str(n) for n in sampled_numbers]

        return [str(random.randint(min_value, max_value)) for _ in range(total_rows)]


    def generate_letter_values(self, total_rows: int, constraints: StringConstraints) -> List[str]:
        pool = self.build_char_pool(constraints)
        length = constraints.length
        max_unique_values = len(pool) ** length

        if constraints.is_unique:
            if total_rows > max_unique_values:
                raise UnsatisfiableConstraintsError("Not enough unique combinations")
            sampled_indexes = random.sample(range(max_unique_values), total_rows)
            return [self.encode_index(i, pool, length) for i in sampled_indexes]

        return ["".join(random.choices(pool, k=length)) for _ in range(total_rows)]

    def generate_values(self, total_rows: int, constraints: StringConstraints) -> List[str]:
        self.validate(constraints)

        if constraints.allowed_values:
            return self.generate_constant_values(total_rows, constraints)

        if constraints.regular_expr:
            return self.generate_regex_values(total_rows, constraints)

        if constraints.character_set == CharacterSet.DIGITS:
            return self.generate_digit_values(total_rows, constraints)

        return self.generate_letter_values(total_rows, constraints)
