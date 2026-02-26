from typing import Any, Dict

from app.core.application.ports.value_converter_port import ISourceValueConverter
from app.core.domain.enums import DataType
from app.infrastructure.errors import ConverterRegistrationError
from app.infrastructure.converters.value_type_converter import ValueTypeConverter


class ValueConverterFactory:
    def __init__(self):
        self.source_converters: Dict[DataType, ISourceValueConverter[Any]] = {}

    def register(self, source_type: DataType, source_converter: ISourceValueConverter[Any]) -> None:
        if source_converter.source_type != source_type:
            raise ConverterRegistrationError(
                f"Converter source type mismatch: expected {source_type.value}, "
                f"got {source_converter.source_type.value}"
            )
        self.source_converters[source_type] = source_converter

    def create(self) -> ValueTypeConverter:
        return ValueTypeConverter(converter_by_source_type=self.source_converters)
