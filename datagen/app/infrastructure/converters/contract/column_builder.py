from collections.abc import Mapping
from datetime import date, datetime
from typing import Any, cast

from app.domain.constraints import (
    BooleanConstraints,
    DateConstraints,
    FloatConstraints,
    IntConstraints,
    OutputConstraints,
    StringConstraints,
    TimestampConstraints,
)
from app.domain.conversion_rules import ensure_conversion_supported, ensure_final_uniqueness_supported
from app.domain.entities import ColumnGenerationSpec, TableColumnSpec, TableForeignKeySpec
from app.domain.enums import CaseMode, CharacterSet, DataType, RelationType
from app.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.converters.contract.fields import (
    get_constraints_data,
    optional_string,
    parse_date_literal,
    parse_timestamp_literal,
    require_integer,
    require_non_empty_string,
    require_number,
)
from app.infrastructure.errors import SchemaValidationError

LEGACY_MAPPING = {
    "DATE_STRING": DataType.DATE,
    "TIMESTAMP_STRING": DataType.TIMESTAMP,
}

FOREIGN_KEY_ALLOWED_CONSTRAINT_FIELDS = {"null_ratio"}


def normalize_is_primary_key(column_name: str, raw_is_primary_key: Any) -> bool:
    if raw_is_primary_key is None:
        return False
    if isinstance(raw_is_primary_key, bool):
        return raw_is_primary_key
    raise SchemaValidationError(f"Column {column_name} has invalid is_primary_key: {raw_is_primary_key!r}")


def normalize_allowed_values(column_name: str, raw_allowed_values: Any) -> tuple[Any, ...] | None:
    if raw_allowed_values is None:
        return None
    if isinstance(raw_allowed_values, (str, bytes)) or not isinstance(raw_allowed_values, list):
        raise SchemaValidationError(f"Column {column_name} allowed_values must be a list")
    return tuple(raw_allowed_values)


def normalize_null_ratio(column_name: str, raw_null_ratio: Any) -> float:
    if isinstance(raw_null_ratio, bool) or not isinstance(raw_null_ratio, (int, float)):
        raise SchemaValidationError(f"Column {column_name} has invalid null_ratio: {raw_null_ratio!r}")

    null_ratio = float(raw_null_ratio)
    if 0 <= null_ratio <= 1:
        return null_ratio

    raise SchemaValidationError(
        f"Column {column_name} has null_ratio outside [0, 1]: {raw_null_ratio}"
    )


def normalize_is_unique(column_name: str, raw_is_unique: Any) -> bool:
    if isinstance(raw_is_unique, bool):
        return raw_is_unique

    raise SchemaValidationError(f"Column {column_name} has invalid is_unique: {raw_is_unique!r}")


def validate_foreign_key_constraints(column_name: str, constraints_data: Mapping[str, Any]) -> None:
    unsupported_constraint_fields = sorted(set(constraints_data.keys()) - FOREIGN_KEY_ALLOWED_CONSTRAINT_FIELDS)
    if unsupported_constraint_fields:
        unsupported_fields = ", ".join(unsupported_constraint_fields)
        raise SchemaValidationError(
            f"Foreign key column {column_name} supports only null_ratio constraint, got: {unsupported_fields}"
        )


def build_output_constraints(
    column_name: str,
    constraints_data: dict[str, Any],
    is_primary_key: bool,
) -> OutputConstraints:
    raw_null_ratio = constraints_data.get("null_ratio")
    raw_is_unique = constraints_data.get("is_unique", constraints_data.get("unique"))

    null_ratio = 0.0 if raw_null_ratio is None else normalize_null_ratio(column_name, raw_null_ratio)
    is_unique = False if raw_is_unique is None else normalize_is_unique(column_name, raw_is_unique)

    if is_primary_key:
        if raw_is_unique is False:
            raise SchemaValidationError(
                f"Primary key column {column_name} cannot declare is_unique=false"
            )
        if null_ratio != 0:
            raise SchemaValidationError(f"Primary key column {column_name} must have null_ratio=0")

        return OutputConstraints(
            null_ratio=0.0,
            is_unique=True,
        )

    try:
        return OutputConstraints(
            null_ratio=null_ratio,
            is_unique=is_unique,
        )
    except InvalidConstraintsError as exc:
        raise SchemaValidationError(f"Column {column_name}: {exc}") from exc


def resolve_data_types(
    column_name: str,
    column_data: Mapping[str, Any],
    constraints_data: Mapping[str, Any],
) -> tuple[DataType, DataType]:
    generator_raw = column_data.get("generator_data_type", column_data.get("gen_data_type"))
    output_raw = column_data.get("output_data_type")

    if generator_raw is None:
        raise SchemaValidationError(f"Column {column_name} has no generator data type")

    generator_normalized = require_non_empty_string(
        generator_raw,
        f"Column {column_name} generator_data_type",
    ).upper()
    if generator_normalized in LEGACY_MAPPING:
        generator_data_type = LEGACY_MAPPING[generator_normalized]
    else:
        try:
            generator_data_type = DataType(generator_normalized)
        except ValueError as exc:
            raise SchemaValidationError(f"Unsupported generator data type: {generator_normalized}") from exc

    if output_raw is not None:
        try:
            output_data_type = DataType(
                require_non_empty_string(
                    output_raw,
                    f"Column {column_name} output_data_type",
                ).upper()
            )
        except ValueError as exc:
            raise SchemaValidationError(
                f"Unsupported output data type for column {column_name}: {output_raw}"
            ) from exc
    elif (
        (generator_data_type == DataType.DATE and "date_format" in constraints_data) or
        (generator_data_type == DataType.TIMESTAMP and "timestamp_format" in constraints_data)
    ):
        output_data_type = DataType.STRING
    else:
        output_data_type = generator_data_type

    ensure_conversion_supported(generator_data_type, output_data_type)
    return generator_data_type, output_data_type


def build_int_constraints(
    column_name: str,
    constraints_data: dict[str, Any],
    allowed_values: tuple[Any, ...] | None,
) -> IntConstraints:
    digits_count = constraints_data.get("digits_count")
    min_value = constraints_data.get("min_value")
    max_value = constraints_data.get("max_value")

    if digits_count is not None:
        conflicting = [
            field_name
            for field_name, field_value in (
                ("min_value", min_value),
                ("max_value", max_value),
                ("allowed_values", allowed_values),
            )
            if field_value is not None
        ]
        if conflicting:
            conflict_list = ", ".join(conflicting)
            raise SchemaValidationError(
                f"Column {column_name}: digits_count cannot be used together with {conflict_list}"
            )

        if isinstance(digits_count, bool) or not isinstance(digits_count, int):
            raise SchemaValidationError(f"Column {column_name}: digits_count must be integer")
        if digits_count <= 0:
            raise SchemaValidationError(f"Column {column_name}: digits_count must be greater than 0")

        if digits_count == 1:
            min_value, max_value = 0, 9
        else:
            min_value, max_value = 10 ** (digits_count - 1), 10 ** digits_count - 1
    else:
        min_value = 0 if min_value is None else require_integer(min_value, f"Column {column_name} min_value")
        max_value = 1000 if max_value is None else require_integer(max_value, f"Column {column_name} max_value")

    try:
        constraints = IntConstraints(
            allowed_values=allowed_values,
            min_value=min_value,
            max_value=max_value,
        )
    except InvalidConstraintsError as exc:
        raise SchemaValidationError(f"Column {column_name}: {exc}") from exc

    return constraints


def build_string_constraints(
    column_name: str,
    constraints_data: dict[str, Any],
    allowed_values: tuple[Any, ...] | None,
) -> StringConstraints:
    case_mode_raw = constraints_data.get("case_mode")
    if case_mode_raw is None:
        if constraints_data.get("uppercase"):
            case_mode_raw = "upper"
        elif constraints_data.get("lowercase"):
            case_mode_raw = "lower"
        else:
            case_mode_raw = "mixed"
    else:
        case_mode_raw = require_non_empty_string(case_mode_raw, f"Column {column_name} case_mode")

    character_set_raw = constraints_data.get("character_set")
    if character_set_raw is None:
        if constraints_data.get("digits_only"):
            character_set_raw = "digits"
        elif constraints_data.get("chars_only"):
            character_set_raw = "letters"
        else:
            character_set_raw = "letters"
    else:
        character_set_raw = require_non_empty_string(
            character_set_raw,
            f"Column {column_name} character_set",
        )

    try:
        return StringConstraints(
            allowed_values=allowed_values,
            length=require_integer(constraints_data.get("length", 10), f"Column {column_name} length"),
            character_set=CharacterSet(character_set_raw.lower()),
            case_mode=CaseMode(case_mode_raw.lower()),
            regular_expr=optional_string(
                constraints_data.get("regular_expr"),
                f"Column {column_name} regular_expr",
            ),
        )
    except (InvalidConstraintsError, ValueError) as exc:
        raise SchemaValidationError(f"Column {column_name}: {exc}") from exc


def build_generator_constraints(
    column_name: str,
    generator_data_type: DataType,
    constraints_data: dict[str, Any],
    allowed_values: tuple[Any, ...] | None,
) -> Any:
    if generator_data_type == DataType.STRING:
        return build_string_constraints(
            column_name=column_name,
            constraints_data=constraints_data,
            allowed_values=allowed_values,
        )

    if generator_data_type == DataType.INT:
        return build_int_constraints(
            column_name=column_name,
            constraints_data=constraints_data,
            allowed_values=allowed_values,
        )

    if generator_data_type == DataType.FLOAT:
        try:
            return FloatConstraints(
                allowed_values=allowed_values,
                min_value=require_number(constraints_data.get("min_value", 0), f"Column {column_name} min_value"),
                max_value=require_number(constraints_data.get("max_value", 1000), f"Column {column_name} max_value"),
                precision=require_integer(constraints_data.get("precision", 2), f"Column {column_name} precision"),
            )
        except InvalidConstraintsError as exc:
            raise SchemaValidationError(f"Column {column_name}: {exc}") from exc

    if generator_data_type == DataType.DATE:
        min_date = constraints_data.get("min_value")
        max_date = constraints_data.get("max_value")
        normalized_date_values = (
            tuple(parse_date_literal(column_name, "allowed_values", value) for value in allowed_values)
            if allowed_values else None
        )
        try:
            return DateConstraints(
                allowed_values=normalized_date_values,
                min_date=parse_date_literal(column_name, "min_value", min_date) if min_date is not None
                else date(date.today().year, 1, 1),
                max_date=parse_date_literal(column_name, "max_value", max_date) if max_date is not None
                else date(date.today().year, 12, 31),
                date_format=require_non_empty_string(
                    constraints_data.get("date_format", "%Y-%m-%d"),
                    f"Column {column_name} date_format",
                ),
            )
        except InvalidConstraintsError as exc:
            raise SchemaValidationError(f"Column {column_name}: {exc}") from exc

    if generator_data_type == DataType.TIMESTAMP:
        min_timestamp = constraints_data.get("min_timestamp")
        max_timestamp = constraints_data.get("max_timestamp")
        normalized_timestamp_values = (
            tuple(parse_timestamp_literal(column_name, "allowed_values", value) for value in allowed_values)
            if allowed_values else None
        )
        current_year = datetime.now().year
        try:
            return TimestampConstraints(
                allowed_values=normalized_timestamp_values,
                min_timestamp=parse_timestamp_literal(column_name, "min_timestamp", min_timestamp)
                if min_timestamp is not None else datetime(current_year, 1, 1, 0, 0, 0),
                max_timestamp=parse_timestamp_literal(column_name, "max_timestamp", max_timestamp)
                if max_timestamp is not None else datetime(current_year, 12, 31, 23, 59, 59),
                timestamp_format=require_non_empty_string(
                    constraints_data.get("timestamp_format", "%Y-%m-%d %H:%M:%S"),
                    f"Column {column_name} timestamp_format",
                ),
            )
        except InvalidConstraintsError as exc:
            raise SchemaValidationError(f"Column {column_name}: {exc}") from exc

    if generator_data_type == DataType.BOOLEAN:
        return BooleanConstraints(allowed_values=allowed_values)

    raise SchemaValidationError(f"Unsupported generator data type: {generator_data_type.value}")


def build_foreign_key_spec(
    column_name: str,
    parent_table_name: str,
    parent_column_name: str,
    foreign_key_data: dict[str, Any],
) -> TableForeignKeySpec:
    try:
        return TableForeignKeySpec(
            table_name=parent_table_name,
            column_name=parent_column_name,
            relation_type=RelationType(
                require_non_empty_string(
                    foreign_key_data.get("relation_type"),
                    f"Foreign key column {column_name} relation_type",
                ).upper()
            ),
        )
    except ValueError as exc:
        raise SchemaValidationError(
            f"Unsupported relation_type for column {column_name}: {foreign_key_data.get('relation_type')}"
        ) from exc


def build_generated_column_spec(
    column_data: Mapping[str, Any],
) -> TableColumnSpec[Any]:
    column_name = cast(str, column_data["name"])
    constraints_data = get_constraints_data(column_name, column_data)
    generator_data_type, output_data_type = resolve_data_types(
        column_name=column_name,
        column_data=column_data,
        constraints_data=constraints_data,
    )
    is_primary_key = normalize_is_primary_key(column_name, column_data.get("is_primary_key"))
    normalized_allowed_values = normalize_allowed_values(column_name, constraints_data.get("allowed_values"))
    output_constraints = build_output_constraints(
        column_name=column_name,
        constraints_data=constraints_data,
        is_primary_key=is_primary_key,
    )
    try:
        ensure_final_uniqueness_supported(
            source_type=generator_data_type,
            target_type=output_data_type,
            requires_unique_output=is_primary_key or output_constraints.is_unique,
        )
        generator_constraints = build_generator_constraints(
            column_name=column_name,
            generator_data_type=generator_data_type,
            constraints_data=constraints_data,
            allowed_values=normalized_allowed_values,
        )
        return TableColumnSpec(
            name=column_name,
            output_data_type=output_data_type,
            output_constraints=output_constraints,
            is_primary_key=is_primary_key,
            generation=ColumnGenerationSpec(
                source_data_type=generator_data_type,
                constraints=generator_constraints,
            ),
        )
    except SchemaValidationError:
        raise
    except ValueError as exc:
        raise SchemaValidationError(f"Column {column_name}: {exc}") from exc
