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
from app.infrastructure.errors import SchemaValidationError

LEGACY_MAPPING = {
    "DATE_STRING": DataType.DATE,
    "TIMESTAMP_STRING": DataType.TIMESTAMP,
}

FOREIGN_KEY_ALLOWED_CONSTRAINT_FIELDS = {"null_ratio"}


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

        null_ratio = constraints_data.get("null_ratio", 0)
        if not isinstance(null_ratio, (int, float)):
            raise SchemaValidationError(f"Column {column_name} has invalid null_ratio: {null_ratio!r}")
        if not 0 <= null_ratio <= 100:
            raise SchemaValidationError(f"Column {column_name} has null_ratio outside [0, 100]: {null_ratio}")
        null_ratio = int(null_ratio)
        is_unique = (
            constraints_data.get("is_unique", constraints_data.get("unique", False))
            if not is_primary_key
            else is_primary_key
        )
        allowed_values = constraints_data.get("allowed_values")
        normalized_allowed_values = tuple(allowed_values) if allowed_values else None
        output_constraints = OutputConstraints(
            null_ratio=null_ratio,
            is_unique=is_unique,
        )
        ensure_final_uniqueness_supported(
            source_type=generator_data_type,
            target_type=output_data_type,
            requires_unique_output=is_primary_key or output_constraints.is_unique,
        )

        if generator_data_type == DataType.STRING:
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

            constraints = StringConstraints(
                allowed_values=normalized_allowed_values,
                length=constraints_data.get("length", 10),
                character_set=CharacterSet(character_set_raw.lower()),
                case_mode=CaseMode(case_mode_raw.lower()),
                regular_expr=constraints_data.get("regular_expr"),
            )

        elif generator_data_type == DataType.INT:
            constraints = IntConstraints(
                allowed_values=normalized_allowed_values,
                min_value=constraints_data.get("min_value", 0),
                max_value=constraints_data.get("max_value", 1000),
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
            constraints = TimestampConstraints(
                allowed_values=normalized_timestamp_values,
                min_timestamp=parse(min_timestamp) if min_timestamp else datetime(datetime.now().year, 1, 1, 0, 0, 0),
                max_timestamp=parse(max_timestamp) if max_timestamp else datetime(datetime.now().year, 12, 31, 0, 0, 0),
                timestamp_format=constraints_data.get("timestamp_format", "%Y-%m-%d %H:%M:%S"),
            )

        elif generator_data_type == DataType.BOOLEAN:
            constraints = BooleanConstraints(allowed_values=normalized_allowed_values)

        else:
            raise SchemaValidationError(f"Unsupported generator data type: {generator_data_type.value}")

        if is_primary_key and null_ratio != 0:
            raise SchemaValidationError(f"Primary key column {column_name} must have null_ratio=0")

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
