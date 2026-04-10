import pyarrow as pa
from app.core.domain.entities import TableSpec
from app.core.domain.enums import DataType


class ArrowSchemaBuilder:
    """Строит Arrow schema для parquet-публикации из доменной спецификации таблицы."""
    ARROW_MAPPING: dict[DataType, pa.DataType] = {
        DataType.INT: pa.int64(),
        DataType.STRING: pa.string(),
        DataType.FLOAT: pa.float64(),
        DataType.BOOLEAN: pa.bool_(),
        DataType.DATE: pa.date32(),
        DataType.TIMESTAMP: pa.timestamp("us", tz="UTC"),
    }

    def build_schema(self, table: TableSpec) -> pa.Schema:
        """Преобразует output-типы и nullable-контракт доменных колонок в Arrow schema."""
        fields = []
        for column in table.columns:
            arrow_type = self.ARROW_MAPPING[column.output_data_type]
            fields.append(
                pa.field(
                    column.name,
                    arrow_type,
                    nullable=column.output_constraints.null_ratio > 0,
                )
            )
        return pa.schema(fields)
