from typing import Any, Dict

from app.core.domain.enums import DataType
from app.infrastructure.converters.column_value_converter import ColumnValueConverter
from app.infrastructure.converters.source_type_value_converter import SourceTypeValueConverter
from app.infrastructure.errors import ConverterRegistrationError


class ValueConverterFactory:
    def __init__(self):
        self.source_converters: Dict[DataType, SourceTypeValueConverter[Any]] = {}

    def register(self, source_type: DataType, source_converter: SourceTypeValueConverter[Any]) -> None:
        if source_converter.source_type != source_type:
            raise ConverterRegistrationError(
                f"Converter source type mismatch: expected {source_type.value}, "
                f"got {source_converter.source_type.value}"
            )
        self.source_converters[source_type] = source_converter

    def create(self) -> ColumnValueConverter:
        return ColumnValueConverter(converter_by_source_type=self.source_converters)
