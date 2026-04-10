from typing import Any

from app.core.domain.constraints import IntConstraints
from app.core.domain.conversion_rules import ConversionNotAllowedError
from app.core.domain.enums import DataType
from app.core.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.converters.source_type_value_converter import SourceTypeValueConverter


class IntSourceValueConverter(SourceTypeValueConverter[IntConstraints]):
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
