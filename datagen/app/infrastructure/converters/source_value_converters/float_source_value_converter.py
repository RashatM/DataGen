from typing import Any, List

from app.core.application.ports.value_converter_port import ISourceValueConverter
from app.core.domain.constraints import FloatConstraints
from app.core.domain.conversion_rules import ConversionNotAllowedError
from app.core.domain.enums import DataType
from app.core.domain.validation_errors import InvalidConstraintsError


class FloatSourceValueConverter(ISourceValueConverter[FloatConstraints]):
    @property
    def source_type(self) -> DataType:
        return DataType.FLOAT

    def convert(
        self,
        values: List[Any],
        constraints: FloatConstraints,
        target_type: DataType,
        column_name: str,
    ) -> List[Any]:
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
