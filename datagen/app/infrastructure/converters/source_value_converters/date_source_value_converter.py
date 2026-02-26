from datetime import datetime
from typing import Any, List

from app.core.application.ports.value_converter_port import ISourceValueConverter
from app.core.domain.constraints import DateConstraints
from app.core.domain.enums import DataType


class DateSourceValueConverter(ISourceValueConverter[DateConstraints]):
    @property
    def source_type(self) -> DataType:
        return DataType.DATE

    def convert(
        self,
        values: List[Any],
        constraints: DateConstraints,
        target_type: DataType,
        column_name: str,
    ) -> List[Any]:
        if not isinstance(constraints, DateConstraints):
            raise ValueError(f"Invalid date constraints for column {column_name}")

        if target_type == DataType.STRING:
            return [value.strftime(constraints.date_format) for value in values]

        if target_type == DataType.INT:
            result = []
            for value in values:
                rendered = value.strftime(constraints.date_format)
                if not rendered.isdigit():
                    raise ValueError(
                        f"Column {column_name}: date_format '{constraints.date_format}' "
                        f"must produce only digits for DATE -> INT conversion"
                    )
                result.append(int(rendered))
            return result

        if target_type == DataType.TIMESTAMP:
            return [datetime.combine(value, datetime.min.time()) for value in values]

        raise ValueError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
