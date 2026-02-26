from datetime import date, datetime
from typing import Dict, Tuple, List

from dateutil.parser import parse

from app.core.domain.constraints import (
    BooleanConstraints,
    DateConstraints,
    FloatConstraints,
    IntConstraints,
    StringConstraints,
    TimestampConstraints,
)
from app.core.domain.conversion_rules import ensure_conversion_supported
from app.core.domain.entities import (
    MockDataColumn,
    MockDataEntity,
    MockDataForeignKey
)
from app.core.domain.enums import CaseMode, CharacterSet, DataType, RelationType

LEGACY_MAPPING = {
    "DATE_STRING": DataType.DATE,
    "TIMESTAMP_STRING": DataType.TIMESTAMP,
}

def resolve_data_types(column_data: Dict, constraints_data: Dict) -> Tuple[DataType, DataType]:
    generator_raw = column_data.get("gen_data_type")
    output_raw = column_data.get("output_data_type")

    if not generator_raw:
        raise ValueError(f"Column {column_data.get('name')} has no generator data type")

    generator_normalized = str(generator_raw).upper()
    generator_data_type = LEGACY_MAPPING.get(generator_normalized, DataType(generator_normalized))

    if output_raw:
        output_data_type = DataType(str(output_raw).upper())
    elif (
            (generator_data_type == DataType.DATE and "date_format" in constraints_data) or
            (generator_data_type == DataType.TIMESTAMP and "timestamp_format" in constraints_data)
    ):
        output_data_type = DataType.STRING
    else:
        output_data_type = generator_data_type

    ensure_conversion_supported(generator_data_type, output_data_type)
    return generator_data_type, output_data_type


def convert_to_mock_data_entity(entity_data: Dict) -> MockDataEntity:
    schema_name = entity_data["schema_name"]
    table_name = entity_data["table_name"]
    total_rows = entity_data["total_rows"]
    entity_columns = []

    for column_data in entity_data["columns"]:
        column_name = column_data["name"]
        constraints_data = column_data.get("constraints", {})
        generator_data_type, output_data_type = resolve_data_types(column_data, constraints_data)

        is_primary_key = column_data.get("is_primary_key", False)
        foreign_key_data = column_data.get("foreign_key")
        foreign_key = (
            MockDataForeignKey(
                schema_name=foreign_key_data["schema_name"],
                table_name=foreign_key_data["table_name"],
                column_name=foreign_key_data["column_name"],
                relation_type=RelationType(foreign_key_data["relation_type"].upper()),
            )
            if foreign_key_data
            else None
        )

        null_ratio = constraints_data.get("null_ratio", 0)
        is_unique = (
            constraints_data.get("is_unique", constraints_data.get("unique", False))
            if not is_primary_key
            else is_primary_key
        )
        allowed_values = constraints_data.get("allowed_values")

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
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                length=constraints_data.get("length", 10),
                character_set=CharacterSet(character_set_raw.lower()),
                case_mode=CaseMode(case_mode_raw.lower()),
                regular_expr=constraints_data.get("regular_expr"),
            )
            entity_columns.append(
                MockDataColumn(
                    name=column_name,
                    gen_data_type=generator_data_type,
                    output_data_type=output_data_type,
                    is_primary_key=is_primary_key,
                    foreign_key=foreign_key,
                    constraints=constraints,
                )
            )
            continue

        if generator_data_type == DataType.INT:
            constraints = IntConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                min_value=constraints_data.get("min_value", 0),
                max_value=constraints_data.get("max_value", 1000),
            )
            entity_columns.append(
                MockDataColumn(
                    name=column_name,
                    gen_data_type=generator_data_type,
                    output_data_type=output_data_type,
                    is_primary_key=is_primary_key,
                    foreign_key=foreign_key,
                    constraints=constraints,
                )
            )
            continue

        if generator_data_type == DataType.FLOAT:
            constraints = FloatConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                min_value=constraints_data.get("min_value", 0),
                max_value=constraints_data.get("max_value", 1000),
                precision=constraints_data.get("precision", 2),
            )
            entity_columns.append(
                MockDataColumn(
                    name=column_name,
                    gen_data_type=generator_data_type,
                    output_data_type=output_data_type,
                    is_primary_key=is_primary_key,
                    foreign_key=foreign_key,
                    constraints=constraints,
                )
            )
            continue

        if generator_data_type == DataType.DATE:
            min_date = constraints_data.get("min_value")
            max_date = constraints_data.get("max_value")
            normalized_allowed_values = [parse(v).date() for v in allowed_values] if allowed_values else None
            constraints = DateConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=normalized_allowed_values,
                min_date=parse(min_date).date() if min_date else date(date.today().year, 1, 1),
                max_date=parse(max_date).date() if max_date else date(date.today().year, 12, 31),
                date_format=constraints_data.get("date_format", "%Y-%m-%d"),
            )
            entity_columns.append(
                MockDataColumn(
                    name=column_name,
                    gen_data_type=generator_data_type,
                    output_data_type=output_data_type,
                    is_primary_key=is_primary_key,
                    foreign_key=foreign_key,
                    constraints=constraints,
                )
            )
            continue

        if generator_data_type == DataType.TIMESTAMP:
            min_timestamp = constraints_data.get("min_timestamp")
            max_timestamp = constraints_data.get("max_timestamp")
            normalized_allowed_values = [parse(v) for v in allowed_values] if allowed_values else None
            constraints = TimestampConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=normalized_allowed_values,
                min_timestamp=parse(min_timestamp) if min_timestamp else datetime(datetime.now().year, 1, 1, 0, 0, 0),
                max_timestamp=parse(max_timestamp) if max_timestamp else datetime(datetime.now().year, 12, 31, 0, 0, 0),
                timestamp_format=constraints_data.get("timestamp_format", "%Y-%m-%d %H:%M:%S"),
            )
            entity_columns.append(
                MockDataColumn(
                    name=column_name,
                    gen_data_type=generator_data_type,
                    output_data_type=output_data_type,
                    is_primary_key=is_primary_key,
                    foreign_key=foreign_key,
                    constraints=constraints,
                )
            )
            continue

        if generator_data_type == DataType.BOOLEAN:
            constraints = BooleanConstraints(null_ratio=null_ratio, allowed_values=allowed_values)
            entity_columns.append(
                MockDataColumn(
                    name=column_name,
                    gen_data_type=generator_data_type,
                    output_data_type=output_data_type,
                    is_primary_key=is_primary_key,
                    foreign_key=foreign_key,
                    constraints=constraints,
                )
            )
            continue

        raise ValueError(f"Unsupported generator data type: {generator_data_type.value}")

    return MockDataEntity(
        schema_name=schema_name,
        table_name=table_name,
        columns=entity_columns,
        total_rows=total_rows,
    )
