import random
import re
import string
from rstr import Rstr

from app.core.application.ports.generator_port import DataGeneratorPort
from app.core.domain.constraints import OutputConstraints, StringConstraints
from app.core.domain.enums import CaseMode, CharacterSet
from app.core.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError
from app.shared.logger import generation_logger

logger = generation_logger


class StringDataGenerator(DataGeneratorPort[StringConstraints]):
    FIXED_LENGTH_CHAR_CLASS_REGEX = re.compile(r"^\^(?P<char_class>\[[^]]+])\{(?P<length>\d+)}\$$")
    FIXED_LENGTH_CHAR_CLASS_POOLS = {
        "[0-9]": string.digits,
        "[A-Z]": string.ascii_uppercase,
        "[a-z]": string.ascii_lowercase,
        "[A-Za-z]": string.ascii_letters,
        "[A-Z0-9]": string.ascii_uppercase + string.digits,
        "[a-z0-9]": string.ascii_lowercase + string.digits,
        "[A-Za-z0-9]": string.ascii_letters + string.digits,
    }

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.regex_generator = Rstr(rng)

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
    def validate(constraints: StringConstraints, output_constraints: OutputConstraints) -> None:
        if constraints.length <= 0:
            raise InvalidConstraintsError("Length must be greater than 0")

        if constraints.regular_expr and output_constraints.is_unique:
            raise InvalidConstraintsError("Unique regex generation is not deterministic and is not supported")

    @staticmethod
    def generate_fixed_length_pool_values(
        total_rows: int,
        pool: str,
        length: int,
        output_constraints: OutputConstraints,
        rng: random.Random,
    ) -> list[str]:
        max_unique_values = len(pool) ** length

        if output_constraints.is_unique:
            if total_rows > max_unique_values:
                raise UnsatisfiableConstraintsError("Not enough unique combinations")
            sampled_indexes = rng.sample(range(max_unique_values), total_rows)
            return [StringDataGenerator.encode_index(i, pool, length) for i in sampled_indexes]

        return ["".join(rng.choices(pool, k=length)) for _ in range(total_rows)]

    @classmethod
    def parse_fixed_length_char_class_regex(cls, regex: str | None) -> tuple[str, int] | None:
        if not regex:
            return None

        match = cls.FIXED_LENGTH_CHAR_CLASS_REGEX.fullmatch(regex)
        if not match:
            return None

        pool = cls.FIXED_LENGTH_CHAR_CLASS_POOLS.get(match.group("char_class"))
        if pool is None:
            return None

        return pool, int(match.group("length"))

    def generate_constant_values(
        self,
        total_rows: int,
        constraints: StringConstraints,
        output_constraints: OutputConstraints,
    ) -> list[str]:
        allowed_values = constraints.allowed_values
        if allowed_values is None:
            raise InvalidConstraintsError("allowed_values must be defined for constant string generation")

        values = list(dict.fromkeys(map(str, allowed_values)))

        if output_constraints.is_unique:
            if total_rows > len(values):
                raise UnsatisfiableConstraintsError("Not enough unique allowed values")
            return self.rng.sample(values, total_rows)

        return self.rng.choices(values, k=total_rows)

    def generate_regex_values(
        self,
        total_rows: int,
        constraints: StringConstraints,
        output_constraints: OutputConstraints,
    ) -> list[str]:
        regex = constraints.regular_expr
        if regex is None:
            raise InvalidConstraintsError("regular_expr must be defined for regex generation")

        parsed_regex = self.parse_fixed_length_char_class_regex(regex)
        if parsed_regex:
            pool, length = parsed_regex
            return self.generate_fixed_length_pool_values(
                total_rows=total_rows,
                pool=pool,
                length=length,
                output_constraints=output_constraints,
                rng=self.rng,
            )

        if total_rows > 10000:
            logger.warning(f"Regex generation may be slow: rows={total_rows}")

        result = []
        batch_size = 1000
        apply_case = self.apply_case
        case_mode = constraints.case_mode

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = []
            for _ in range(start, end):
                value = self.regex_generator.xeger(regex)
                batch.append(apply_case(value, case_mode))
            result.extend(batch)

        return result

    @staticmethod
    def generate_digit_values(
        total_rows: int,
        constraints: StringConstraints,
        output_constraints: OutputConstraints,
        rng: random.Random,
    ) -> list[str]:
        length = constraints.length
        min_value = 0 if length == 1 else 10 ** (length - 1)
        max_value = 10 ** length - 1
        space = max_value - min_value + 1

        if output_constraints.is_unique:
            if total_rows > space:
                raise UnsatisfiableConstraintsError("Not enough unique digit combinations")
            sampled_numbers = rng.sample(range(min_value, max_value + 1), total_rows)
            return [str(n) for n in sampled_numbers]

        return [str(rng.randint(min_value, max_value)) for _ in range(total_rows)]

    def generate_letter_values(
        self,
        total_rows: int,
        constraints: StringConstraints,
        output_constraints: OutputConstraints,
    ) -> list[str]:
        pool = self.build_char_pool(constraints)
        return self.generate_fixed_length_pool_values(
            total_rows=total_rows,
            pool=pool,
            length=constraints.length,
            output_constraints=output_constraints,
            rng=self.rng,
        )

    def generate_values(
        self,
        total_rows: int,
        constraints: StringConstraints,
        output_constraints: OutputConstraints,
    ) -> list[str]:
        self.validate(constraints, output_constraints)

        if constraints.allowed_values:
            return self.generate_constant_values(total_rows, constraints, output_constraints)

        if constraints.regular_expr:
            return self.generate_regex_values(total_rows, constraints, output_constraints)

        if constraints.character_set == CharacterSet.DIGITS:
            return self.generate_digit_values(total_rows, constraints, output_constraints, self.rng)

        return self.generate_letter_values(total_rows, constraints, output_constraints)
