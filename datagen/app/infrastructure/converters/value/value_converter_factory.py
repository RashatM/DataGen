from typing import Any

from app.domain.enums import DataType
from app.infrastructure.converters.value.column_value_converter import ColumnValueConverter
from app.infrastructure.converters.value.source_value_converter import SourceValueConverter
from app.infrastructure.errors import ConverterRegistrationError


class ValueConverterFactory:
    """Собирает registry конвертеров и создаёт готовый ColumnValueConverter для application-слоя."""
    def __init__(self):
        self.source_converters: dict[DataType, SourceValueConverter[Any]] = {}

    def register(self, source_type: DataType, source_converter: SourceValueConverter[Any]) -> None:
        if source_converter.source_type != source_type:
            raise ConverterRegistrationError(
                f"Converter source type mismatch: expected {source_type.value}, "
                f"got {source_converter.source_type.value}"
            )
        self.source_converters[source_type] = source_converter

    def create(self) -> ColumnValueConverter:
        return ColumnValueConverter(converter_by_source_type=self.source_converters)
