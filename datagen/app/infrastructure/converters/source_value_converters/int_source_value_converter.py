from typing import Any, List

from app.core.application.ports.value_converter_port import ISourceValueConverter
from app.core.domain.constraints import IntConstraints
from app.core.domain.conversion_rules import ConversionNotAllowedError
from app.core.domain.enums import DataType
from app.core.domain.validation_errors import InvalidConstraintsError


class IntSourceValueConverter(ISourceValueConverter[IntConstraints]):
    @property
    def source_type(self) -> DataType:
        return DataType.INT

    def convert(
        self,
        values: List[Any],
        constraints: IntConstraints,
        target_type: DataType,
        column_name: str,
    ) -> List[Any]:
        if not isinstance(constraints, IntConstraints):
            raise InvalidConstraintsError(f"Invalid int constraints for column {column_name}")

        if target_type == DataType.FLOAT:
            return [float(value) for value in values]
        if target_type == DataType.STRING:
            return [str(value) for value in values]
        if target_type == DataType.BOOLEAN:
            return [bool(value) for value in values]

        raise ConversionNotAllowedError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
