from typing import Any, List

from app.core.application.ports.value_converter_port import ISourceValueConverter
from app.core.domain.constraints import TimestampConstraints
from app.core.domain.conversion_rules import ConversionNotAllowedError
from app.core.domain.enums import DataType
from app.core.domain.validation_errors import InvalidConstraintsError


class TimestampSourceValueConverter(ISourceValueConverter[TimestampConstraints]):
    @property
    def source_type(self) -> DataType:
        return DataType.TIMESTAMP

    def convert(
        self,
        values: List[Any],
        constraints: TimestampConstraints,
        target_type: DataType,
        column_name: str,
    ) -> List[Any]:
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
