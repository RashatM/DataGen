from typing import Any

from app.core.domain.constraints import FloatConstraints
from app.core.domain.conversion_rules import ConversionNotAllowedError
from app.core.domain.enums import DataType
from app.core.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.converters.value.source_value_converter import SourceValueConverter


class FloatSourceValueConverter(SourceValueConverter[FloatConstraints]):
    """Конвертирует FLOAT source values в STRING или INT, когда это разрешено доменными правилами."""
    @property
    def source_type(self) -> DataType:
        return DataType.FLOAT

    def convert(
        self,
        values: list[Any],
        constraints: FloatConstraints,
        target_type: DataType,
        column_name: str,
    ) -> list[Any]:
        if not isinstance(constraints, FloatConstraints):
            raise InvalidConstraintsError(f"Invalid float constraints for column {column_name}")

        if target_type == DataType.STRING:
            return [str(value) for value in values]
        if target_type == DataType.INT:
            return [int(value) for value in values]

        raise ConversionNotAllowedError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
