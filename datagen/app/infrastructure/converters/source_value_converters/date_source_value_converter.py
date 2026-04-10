from datetime import datetime
from typing import Any

from app.core.domain.constraints import DateConstraints
from app.core.domain.conversion_rules import ConversionNotAllowedError
from app.core.domain.enums import DataType
from app.core.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.converters.source_type_value_converter import SourceTypeValueConverter


class DateSourceValueConverter(SourceTypeValueConverter[DateConstraints]):
    """Конвертирует DATE source values в STRING, INT или TIMESTAMP по правилам формата и точности."""
    @property
    def source_type(self) -> DataType:
        return DataType.DATE

    def convert(
        self,
        values: list[Any],
        constraints: DateConstraints,
        target_type: DataType,
        column_name: str,
    ) -> list[Any]:
        if not isinstance(constraints, DateConstraints):
            raise InvalidConstraintsError(f"Invalid date constraints for column {column_name}")

        if target_type == DataType.STRING:
            return [value.strftime(constraints.date_format) for value in values]

        if target_type == DataType.INT:
            result = []
            for value in values:
                rendered = value.strftime(constraints.date_format)
                if not rendered.isdigit():
                    raise InvalidConstraintsError(
                        f"Column {column_name}: date_format '{constraints.date_format}' "
                        f"must produce only digits for DATE -> INT conversion"
                    )
                result.append(int(rendered))
            return result

        if target_type == DataType.TIMESTAMP:
            return [datetime.combine(value, datetime.min.time()) for value in values]

        raise ConversionNotAllowedError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
