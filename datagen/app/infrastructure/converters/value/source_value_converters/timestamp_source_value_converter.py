from typing import Any

from app.domain.constraints import TimestampConstraints
from app.domain.conversion_rules import ConversionNotAllowedError
from app.domain.enums import DataType
from app.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.converters.value.source_value_converter import SourceValueConverter


class TimestampSourceValueConverter(SourceValueConverter[TimestampConstraints]):
    """Конвертирует TIMESTAMP source values в STRING, INT или DATE с учётом заданного формата."""
    @property
    def source_type(self) -> DataType:
        return DataType.TIMESTAMP

    def convert(
        self,
        values: list[Any],
        constraints: TimestampConstraints,
        target_type: DataType,
        column_name: str,
    ) -> list[Any]:
        if not isinstance(constraints, TimestampConstraints):
            raise InvalidConstraintsError(f"Invalid timestamp constraints for column {column_name}")

        if target_type == DataType.STRING:
            return [value.strftime(constraints.timestamp_format) for value in values]

        if target_type == DataType.INT:
            result = []
            for value in values:
                rendered = value.strftime(constraints.timestamp_format)
                if not rendered.isdigit():
                    raise InvalidConstraintsError(
                        f"Column {column_name}: timestamp_format '{constraints.timestamp_format}' "
                        f"must produce only digits for TIMESTAMP -> INT conversion"
                    )
                result.append(int(rendered))
            return result

        if target_type == DataType.DATE:
            return [value.date() for value in values]

        raise ConversionNotAllowedError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
