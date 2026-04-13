from typing import Any

from app.core.application.ports.value_converter_port import ValueConverterPort
from app.core.domain.entities import TableColumnSpec
from app.core.domain.enums import DataType
from app.infrastructure.converters.value.source_value_converter import SourceValueConverter
from app.infrastructure.errors import SourceValueConverterNotRegisteredError


class ColumnValueConverter(ValueConverterPort):
    """Выбирает source-type converter и выполняет приведение исходных значений к output type колонки."""
    def __init__(self, converter_by_source_type: dict[DataType, SourceValueConverter[Any]]):
        self.converter_by_source_type = converter_by_source_type

    def convert(self, values: list[Any], table_column: TableColumnSpec) -> list[Any]:
        source_type = table_column.generator_data_type
        target_type = table_column.output_data_type

        if source_type == target_type:
            return values

        converter = self.converter_by_source_type.get(source_type)
        if not converter:
            raise SourceValueConverterNotRegisteredError(
                f"No converter for source data type: {source_type.value}"
            )

        return converter.convert(
            values=values,
            constraints=table_column.generator_constraints,
            target_type=target_type,
            column_name=table_column.name,
        )
