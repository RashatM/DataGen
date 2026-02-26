from typing import Any, Dict, List

from app.core.application.ports.value_converter_port import ISourceValueConverter, IValueConverter
from app.core.domain.entities import MockDataColumn
from app.core.domain.enums import DataType


class ValueTypeConverter(IValueConverter):
    def __init__(self, converter_by_source_type: Dict[DataType, ISourceValueConverter[Any]]):
        self.converter_by_source_type = converter_by_source_type

    def convert(self, values: List[Any], column: MockDataColumn) -> List[Any]:
        source_type = column.gen_data_type
        target_type = column.output_data_type

        if source_type == target_type:
            return values

        converter = self.converter_by_source_type.get(source_type)
        if not converter:
            raise ValueError(f"No converter for source data type: {source_type.value}")

        return converter.convert(
            values=values,
            constraints=column.constraints,
            target_type=target_type,
            column_name=column.name,
        )
