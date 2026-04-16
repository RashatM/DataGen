from typing import Any

from app.domain.constraints import IntConstraints
from app.domain.conversion_rules import ConversionNotAllowedError
from app.domain.enums import DataType
from app.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.converters.value.source_value_converter import SourceValueConverter


class IntSourceValueConverter(SourceValueConverter[IntConstraints]):
    """Конвертирует INT source values в FLOAT, STRING или BOOLEAN по поддерживаемым правилам."""
    @property
    def source_type(self) -> DataType:
        return DataType.INT

    def convert(
        self,
        values: list[Any],
        constraints: IntConstraints,
        target_type: DataType,
        column_name: str,
    ) -> list[Any]:
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
