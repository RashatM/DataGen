from typing import Any

from app.domain.constraints import StringConstraints
from app.domain.conversion_rules import ConversionNotAllowedError
from app.domain.enums import DataType
from app.domain.validation_errors import InvalidConstraintsError, ValueConversionError
from app.infrastructure.converters.value.source_value_converter import SourceValueConverter


class StringSourceValueConverter(SourceValueConverter[StringConstraints]):
    """Конвертирует STRING source values в STRING или INT с fail-fast ошибкой на плохих значениях."""
    @property
    def source_type(self) -> DataType:
        return DataType.STRING

    def convert(
        self,
        values: list[Any],
        constraints: StringConstraints,
        target_type: DataType,
        column_name: str,
    ) -> list[Any]:
        if not isinstance(constraints, StringConstraints):
            raise InvalidConstraintsError(f"Invalid string constraints for column {column_name}")

        if target_type == DataType.STRING:
            return [str(value) for value in values]
        if target_type == DataType.INT:
            converted_values: list[int] = []
            for value in values:
                value_as_string = str(value)
                try:
                    converted_values.append(int(value_as_string))
                except ValueError as exc:
                    raise ValueConversionError(
                        f"Column {column_name}: cannot convert '{value_as_string}' from STRING to INT"
                    ) from exc
            return converted_values

        raise ConversionNotAllowedError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
