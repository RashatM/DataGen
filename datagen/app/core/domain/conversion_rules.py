from typing import Dict, Set

from app.core.domain.enums import DataType


class ConversionNotAllowedError(ValueError):
    pass


ALLOWED_OUTPUT_TYPES: Dict[DataType, Set[DataType]] = {
    DataType.DATE: {DataType.DATE, DataType.STRING, DataType.INT, DataType.TIMESTAMP},
    DataType.TIMESTAMP: {DataType.TIMESTAMP, DataType.STRING, DataType.INT, DataType.DATE},
    DataType.INT: {DataType.INT, DataType.FLOAT, DataType.STRING, DataType.BOOLEAN},
    DataType.FLOAT: {DataType.FLOAT, DataType.STRING, DataType.INT},
    DataType.BOOLEAN: {DataType.BOOLEAN, DataType.STRING, DataType.INT},
    DataType.STRING: {DataType.STRING, DataType.INT},
}


def ensure_conversion_supported(source_type: DataType, target_type: DataType) -> None:
    allowed_target_types = ALLOWED_OUTPUT_TYPES.get(source_type)
    if allowed_target_types is None or target_type not in allowed_target_types:
        raise ConversionNotAllowedError(
            f"Conversion not allowed: {source_type.value} -> {target_type.value}"
        )
