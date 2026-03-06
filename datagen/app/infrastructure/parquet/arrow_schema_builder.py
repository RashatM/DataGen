from typing import Dict
import pyarrow as pa
from app.core.domain.entities import TableSpec
from app.core.domain.enums import DataType


class ArrowSchemaBuilder:
    ARROW_MAPPING: Dict[DataType, pa.DataType] = {
        DataType.INT: pa.int64(),
        DataType.STRING: pa.string(),
        DataType.FLOAT: pa.float64(),
        DataType.BOOLEAN: pa.bool_(),
        DataType.DATE: pa.date32(),
        DataType.TIMESTAMP: pa.timestamp("us", tz="UTC"),
    }

    def build_schema(self, table: TableSpec) -> pa.Schema:
        fields = []
        for column in table.columns:
            arrow_type = self.ARROW_MAPPING[column.output_data_type]
            fields.append(
                pa.field(
                    column.name,
                    arrow_type,
                    nullable=column.constraints.null_ratio > 0,
                )
            )
        return pa.schema(fields)