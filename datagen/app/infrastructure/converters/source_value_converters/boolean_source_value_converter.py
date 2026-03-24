from typing import Any

from app.core.domain.constraints import BooleanConstraints
from app.core.domain.conversion_rules import ConversionNotAllowedError
from app.core.domain.enums import DataType
from app.core.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.converters.source_type_value_converter import SourceTypeValueConverter


class BooleanSourceValueConverter(SourceTypeValueConverter[BooleanConstraints]):
    @property
    def source_type(self) -> DataType:
        return DataType.BOOLEAN

    def convert(
        self,
        values: list[Any],
        constraints: BooleanConstraints,
        target_type: DataType,
        column_name: str,
    ) -> list[Any]:
        if not isinstance(constraints, BooleanConstraints):
            raise InvalidConstraintsError(f"Invalid boolean constraints for column {column_name}")

        if target_type == DataType.STRING:
            return [str(value) for value in values]
        if target_type == DataType.INT:
            return [int(value) for value in values]

        raise ConversionNotAllowedError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
