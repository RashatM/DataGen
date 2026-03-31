from datetime import date, datetime
from typing import Any

from dateutil.parser import parse

from app.core.domain.constraints import (
    BooleanConstraints,
    DateConstraints,
    FloatConstraints,
    IntConstraints,
    OutputConstraints,
    StringConstraints,
    TimestampConstraints,
)
from app.core.domain.conversion_rules import ensure_conversion_supported, ensure_final_uniqueness_supported
from app.core.domain.entities import (
    TableColumnSpec,
    TableSpec,
    TableForeignKeySpec, GenerationRun
)
from app.core.domain.enums import CaseMode, CharacterSet, DataType, RelationType
from app.core.domain.validation_errors import InvalidConstraintsError
from app.infrastructure.errors import SchemaValidationError

LEGACY_MAPPING = {
    "DATE_STRING": DataType.DATE,
    "TIMESTAMP_STRING": DataType.TIMESTAMP,
}

FOREIGN_KEY_ALLOWED_CONSTRAINT_FIELDS = {"null_ratio"}


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

    return OutputConstraints(
        null_ratio=null_ratio,
        is_unique=is_unique,
    )


def resolve_data_types(column_data: dict, constraints_data: dict) -> tuple[DataType, DataType]:
    generator_raw = column_data.get("generator_data_type", column_data.get("gen_data_type"))
    output_raw = column_data.get("output_data_type")

    if not generator_raw:
        raise SchemaValidationError(f"Column {column_data.get('name')} has no generator data type")

    generator_normalized = str(generator_raw).upper()
    if generator_normalized in LEGACY_MAPPING:
        generator_data_type = LEGACY_MAPPING[generator_normalized]
    else:
        try:
            generator_data_type = DataType(generator_normalized)
        except ValueError as exc:
            raise SchemaValidationError(f"Unsupported generator data type: {generator_normalized}") from exc

    if output_raw:
        try:
            output_data_type = DataType(str(output_raw).upper())
        except ValueError as exc:
            raise SchemaValidationError(
                f"Unsupported output data type for column {column_data.get('name')}: {output_raw}"
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
        min_value = 0 if min_value is None else min_value
        max_value = 1000 if max_value is None else max_value

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
    output_data_type: DataType,
) -> StringConstraints:
    case_mode_raw = constraints_data.get("case_mode")
    if not case_mode_raw:
        if constraints_data.get("uppercase"):
            case_mode_raw = "upper"
        elif constraints_data.get("lowercase"):
            case_mode_raw = "lower"
        else:
            case_mode_raw = "mixed"

    character_set_raw = constraints_data.get("character_set")
    if not character_set_raw:
        if constraints_data.get("digits_only"):
            character_set_raw = "digits"
        elif constraints_data.get("chars_only"):
            character_set_raw = "letters"
        else:
            character_set_raw = "letters"

    try:
        return StringConstraints(
            allowed_values=allowed_values,
            length=constraints_data.get("length", 10),
            character_set=CharacterSet(character_set_raw.lower()),
            case_mode=CaseMode(case_mode_raw.lower()),
            regular_expr=constraints_data.get("regular_expr"),
        )
    except InvalidConstraintsError as exc:
        raise SchemaValidationError(f"Column {column_name}: {exc}") from exc


def convert_to_table_spec(table_data: dict) -> TableSpec:
    schema_name = table_data["schema_name"]
    table_name = table_data["table_name"]
    total_rows = table_data["total_rows"]
    table_columns = []

    for column_data in table_data["columns"]:
        column_name = column_data["name"]
        constraints_data = column_data.get("constraints", {})
        generator_data_type, output_data_type = resolve_data_types(column_data, constraints_data)

        is_primary_key = column_data.get("is_primary_key", False)
        foreign_key_data = column_data.get("foreign_key")
        foreign_key = None
        if foreign_key_data:
            try:
                foreign_key = TableForeignKeySpec(
                    schema_name=foreign_key_data["schema_name"],
                    table_name=foreign_key_data["table_name"],
                    column_name=foreign_key_data["column_name"],
                    relation_type=RelationType(str(foreign_key_data["relation_type"]).upper()),
                )
            except ValueError as exc:
                raise SchemaValidationError(
                    f"Unsupported relation_type for column {column_name}: {foreign_key_data.get('relation_type')}"
                ) from exc

        allowed_values = constraints_data.get("allowed_values")
        normalized_allowed_values = tuple(allowed_values) if allowed_values else None
        output_constraints = build_output_constraints(
            column_name=column_name,
            constraints_data=constraints_data,
            is_primary_key=is_primary_key,
        )
        ensure_final_uniqueness_supported(
            source_type=generator_data_type,
            target_type=output_data_type,
            requires_unique_output=is_primary_key or output_constraints.is_unique,
        )

        if generator_data_type == DataType.STRING:
            constraints = build_string_constraints(
                column_name=column_name,
                constraints_data=constraints_data,
                allowed_values=normalized_allowed_values,
                output_data_type=output_data_type,
            )

        elif generator_data_type == DataType.INT:
            constraints = build_int_constraints(
                column_name=column_name,
                constraints_data=constraints_data,
                allowed_values=normalized_allowed_values,
            )

        elif generator_data_type == DataType.FLOAT:
            constraints = FloatConstraints(
                allowed_values=normalized_allowed_values,
                min_value=constraints_data.get("min_value", 0),
                max_value=constraints_data.get("max_value", 1000),
                precision=constraints_data.get("precision", 2),
            )

        elif generator_data_type == DataType.DATE:
            min_date = constraints_data.get("min_value")
            max_date = constraints_data.get("max_value")
            normalized_date_values = tuple(parse(v).date() for v in allowed_values) if allowed_values else None
            constraints = DateConstraints(
                allowed_values=normalized_date_values,
                min_date=parse(min_date).date() if min_date else date(date.today().year, 1, 1),
                max_date=parse(max_date).date() if max_date else date(date.today().year, 12, 31),
                date_format=constraints_data.get("date_format", "%Y-%m-%d"),
            )

        elif generator_data_type == DataType.TIMESTAMP:
            min_timestamp = constraints_data.get("min_timestamp")
            max_timestamp = constraints_data.get("max_timestamp")
            normalized_timestamp_values = tuple(parse(v) for v in allowed_values) if allowed_values else None
            current_year = datetime.now().year
            constraints = TimestampConstraints(
                allowed_values=normalized_timestamp_values,
                min_timestamp=parse(min_timestamp) if min_timestamp else datetime(current_year, 1, 1, 0, 0, 0),
                max_timestamp=parse(max_timestamp) if max_timestamp else datetime(current_year, 12, 31, 23, 59, 59),
                timestamp_format=constraints_data.get("timestamp_format", "%Y-%m-%d %H:%M:%S"),
            )

        elif generator_data_type == DataType.BOOLEAN:
            constraints = BooleanConstraints(allowed_values=normalized_allowed_values)

        else:
            raise SchemaValidationError(f"Unsupported generator data type: {generator_data_type.value}")

        if foreign_key_data:
            unsupported_constraint_fields = sorted(
                set(constraints_data.keys()) - FOREIGN_KEY_ALLOWED_CONSTRAINT_FIELDS
            )
            if unsupported_constraint_fields:
                unsupported_fields = ", ".join(unsupported_constraint_fields)
                raise SchemaValidationError(
                    f"Foreign key column {column_name} supports only null_ratio constraint, "
                    f"got: {unsupported_fields}"
                )

        table_columns.append(
            TableColumnSpec(
                name=column_name,
                generator_data_type=generator_data_type,
                generator_constraints=constraints,
                output_constraints=output_constraints,
                output_data_type=output_data_type,
                is_primary_key=is_primary_key,
                foreign_key=foreign_key,
            )
        )

    return TableSpec(
        schema_name=schema_name,
        table_name=table_name,
        columns=table_columns,
        total_rows=total_rows,
    )


def convert_to_generation_run(run_id: str, raw_tables: list[dict[str, Any]]) -> GenerationRun:
    tables = [convert_to_table_spec(table_data) for table_data in raw_tables]
    return GenerationRun(run_id=run_id, tables=tables)
