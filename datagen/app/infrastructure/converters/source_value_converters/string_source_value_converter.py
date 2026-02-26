from typing import Any, List

from app.core.application.ports.value_converter_port import ISourceValueConverter
from app.core.domain.constraints import StringConstraints
from app.core.domain.enums import DataType


class StringSourceValueConverter(ISourceValueConverter[StringConstraints]):
    @property
    def source_type(self) -> DataType:
        return DataType.STRING

    def convert(
        self,
        values: List[Any],
        constraints: StringConstraints,
        target_type: DataType,
        column_name: str,
    ) -> List[Any]:
        if not isinstance(constraints, StringConstraints):
            raise ValueError(f"Invalid string constraints for column {column_name}")

        if target_type == DataType.STRING:
            return [str(value) for value in values]
        if target_type == DataType.INT:
            converted_values: List[int] = []
            for value in values:
                value_as_string = str(value)
                try:
                    converted_values.append(int(value_as_string))
                except ValueError as exc:
                    raise ValueError(
                        f"Column {column_name}: cannot convert '{value_as_string}' from STRING to INT"
                    ) from exc
            return converted_values

        raise ValueError(
            f"Unsupported conversion for column {column_name}: "
            f"{self.source_type.value} -> {target_type.value}"
        )
