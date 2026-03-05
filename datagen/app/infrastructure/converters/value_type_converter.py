from typing import Any, Dict, List

from app.core.application.ports.value_converter_port import ISourceValueConverter, IValueConverter
from app.core.domain.entities import TableColumnSpec
from app.core.domain.enums import DataType
from app.infrastructure.errors import SourceValueConverterNotRegisteredError


class ValueTypeConverter(IValueConverter):
    def __init__(self, converter_by_source_type: Dict[DataType, ISourceValueConverter[Any]]):
        self.converter_by_source_type = converter_by_source_type

    def convert(self, values: List[Any], table_column: TableColumnSpec) -> List[Any]:
        source_type = table_column.gen_data_type
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
            constraints=table_column.constraints,
            target_type=target_type,
            column_name=table_column.name,
        )
