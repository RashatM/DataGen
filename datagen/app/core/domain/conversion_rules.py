from app.core.domain.enums import DataType


class ConversionNotAllowedError(ValueError):
    """Поднимается, когда система вообще не поддерживает requested source -> target conversion."""
    pass


class LossyUniqueOutputConversionError(ValueError):
    """Поднимается, когда conversion допустим, но ломает гарантию уникальности итогового столбца."""
    pass


ALLOWED_OUTPUT_TYPES: dict[DataType, set[DataType]] = {
    DataType.DATE: {DataType.DATE, DataType.STRING, DataType.INT, DataType.TIMESTAMP},
    DataType.TIMESTAMP: {DataType.TIMESTAMP, DataType.STRING, DataType.INT, DataType.DATE},
    DataType.INT: {DataType.INT, DataType.FLOAT, DataType.STRING, DataType.BOOLEAN},
    DataType.FLOAT: {DataType.FLOAT, DataType.STRING, DataType.INT},
    DataType.BOOLEAN: {DataType.BOOLEAN, DataType.STRING, DataType.INT},
    DataType.STRING: {DataType.STRING, DataType.INT},
}

LOSSY_UNIQUE_OUTPUT_CONVERSIONS = {
    (DataType.INT, DataType.BOOLEAN),
    (DataType.FLOAT, DataType.INT),
    (DataType.TIMESTAMP, DataType.DATE),
}


def ensure_conversion_supported(source_type: DataType, target_type: DataType) -> None:
    allowed_target_types = ALLOWED_OUTPUT_TYPES.get(source_type)
    if allowed_target_types is None or target_type not in allowed_target_types:
        raise ConversionNotAllowedError(
            f"Conversion not allowed: {source_type.value} -> {target_type.value}"
        )


def ensure_final_uniqueness_supported(
    source_type: DataType,
    target_type: DataType,
    requires_unique_output: bool,
) -> None:
    if not requires_unique_output:
        return

    if (source_type, target_type) in LOSSY_UNIQUE_OUTPUT_CONVERSIONS:
        raise LossyUniqueOutputConversionError(
            f"Final uniqueness is not supported for conversion: {source_type.value} -> {target_type.value}"
        )
