import json
import re
from typing import Any

import pandas as pd

from app.core.domain.conversion_rules import ALLOWED_OUTPUT_TYPES
from app.core.domain.enums import CaseMode, CharacterSet, DataType, RelationType

SHEET_TABLES = "tables"
SHEET_QUERIES = "queries"
EXCEL_SUFFIXES = {".xlsx", ".xlsm"}
TABLES_HEADER_FIELDS = {"schema_name", "table_name", "total_rows"}
WORKBOOK_TABLES_HEADER_FIELDS = {
    "table_name",
    "total_rows",
    "hive_target_table",
    "iceberg_target_table",
    "write_mode",
    "hive_partition_columns",
    "iceberg_partition_columns",
}
WORKBOOK_QUERIES_HEADER_FIELDS = {
    "hive_sql",
    "iceberg_sql",
    "hive_exclude_columns",
    "iceberg_exclude_columns",
}
DATA_SHEET_REQUIRED_FIELDS = {"column_name", "generator_data_type"}
WORKBOOK_DATA_SHEET_REQUIRED_FIELDS = {"column_name"}
ROW_DATA_FIELDS = (
    "column_name",
    "generator_data_type",
    "output_data_type",
    "is_primary_key",
    "constraints",
    "foreign_key",
)
WORKBOOK_ROW_DATA_FIELDS = (
    "column_name",
    "generator_data_type",
    "output_data_type",
    "is_primary_key",
    "constraints",
    "foreign_key",
    "engine_scope",
    "derive",
)
WORKBOOK_QUERY_ROW_FIELDS = (
    "hive_sql",
    "iceberg_sql",
    "hive_exclude_columns",
    "iceberg_exclude_columns",
)
WRITE_MODES = {
    "OVERWRITE_TABLE",
    "OVERWRITE_PARTITIONS",
    "APPEND",
    "APPEND_DISTINCT_PARTITIONS",
}
ENGINE_SCOPES = {"both", "hive_only", "iceberg_only"}
DERIVE_RULES = {"YYYYMMDD", "YYYY", "MM"}
GENERATOR_TYPE_ALIASES = {
    "STRING": DataType.STRING.value,
    "INT": DataType.INT.value,
    "FLOAT": DataType.FLOAT.value,
    "DATE": DataType.DATE.value,
    "TIMESTAMP": DataType.TIMESTAMP.value,
    "BOOLEAN": DataType.BOOLEAN.value,
    "DATE_STRING": DataType.DATE.value,
    "TIMESTAMP_STRING": DataType.TIMESTAMP.value,
}
ALLOWED_OUTPUT_TYPES_BY_SOURCE = {
    source_type.value: {target_type.value for target_type in target_types}
    for source_type, target_types in ALLOWED_OUTPUT_TYPES.items()
}
HEADER_ALIASES = {
    "column_name": "column_name",
    "name": "column_name",
    "gen_data_type": "generator_data_type",
    "generator_data_type": "generator_data_type",
    "data_type": "generator_data_type",
    "output_data_type": "output_data_type",
    "is_primary_key": "is_primary_key",
    "constraints": "constraints",
    "foreign_key": "foreign_key",
    "engine_scope": "engine_scope",
    "derive": "derive",
    "hive_target_table": "hive_target_table",
    "iceberg_target_table": "iceberg_target_table",
    "write_mode": "write_mode",
    "hive_partition_columns": "hive_partition_columns",
    "iceberg_partition_columns": "iceberg_partition_columns",
    "hive_sql": "hive_sql",
    "iceberg_sql": "iceberg_sql",
    "hive_exclude_columns": "hive_exclude_columns",
    "iceberg_exclude_columns": "iceberg_exclude_columns",
}
LEGACY_CONSTRAINT_ALIASES = {
    "unique": "is_unique",
    "min_date": "min_value",
    "max_date": "max_value",
}
SUPPORTED_CONSTRAINTS = {
    DataType.STRING.value: {
        "null_ratio",
        "is_unique",
        "allowed_values",
        "length",
        "regular_expr",
        "character_set",
        "case_mode",
    },
    DataType.INT.value: {
        "null_ratio",
        "is_unique",
        "allowed_values",
        "min_value",
        "max_value",
        "digits_count",
    },
    DataType.FLOAT.value: {
        "null_ratio",
        "is_unique",
        "allowed_values",
        "min_value",
        "max_value",
        "precision",
    },
    DataType.DATE.value: {
        "null_ratio",
        "is_unique",
        "allowed_values",
        "min_value",
        "max_value",
        "date_format",
    },
    DataType.TIMESTAMP.value: {
        "null_ratio",
        "is_unique",
        "allowed_values",
        "min_timestamp",
        "max_timestamp",
        "timestamp_format",
    },
    DataType.BOOLEAN.value: {
        "null_ratio",
        "is_unique",
        "allowed_values",
    },
}
INT_PATTERN = re.compile(r"^[+-]?\d+$")
FLOAT_PATTERN = re.compile(r"^[+-]?(?:\d+\.\d+|\d+\.\d*|\.\d+)$")


def format_issue(sheet_name: str, message: str, row: int | None = None) -> str:
    if row is None:
        return f"sheet={sheet_name} | message={message}"
    return f"sheet={sheet_name} | row={row} | message={message}"


def prefix_issue(workbook_name: str, issue: str) -> str:
    return f"{workbook_name}: {issue}"


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        if bool(pd.isna(value)):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def is_blank(value: Any) -> bool:
    return normalize_text(value) == ""


def row_has_data(row: pd.Series) -> bool:
    return any(normalize_text(row.get(field_name)) for field_name in ROW_DATA_FIELDS)


def row_has_data_by_fields(row: pd.Series, field_names: tuple[str, ...]) -> bool:
    return any(normalize_text(row.get(field_name)) for field_name in field_names)


def normalize_sheet(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.dropna(how="all").copy()
    normalized_df.columns = [
        HEADER_ALIASES.get(normalize_text(column_name).lower(), normalize_text(column_name).lower())
        for column_name in normalized_df.columns
    ]
    return normalized_df


def find_tables_header(df: pd.DataFrame) -> tuple[int, dict[str, int]]:
    for row_index in range(min(len(df.index), 20)):
        current_headers: dict[str, int] = {}
        for column_index in range(min(len(df.columns), 8)):
            header = normalize_text(df.iat[row_index, column_index]).lower()
            if header:
                current_headers[header] = column_index
        if TABLES_HEADER_FIELDS <= set(current_headers):
            return row_index, current_headers
    raise ValueError("could not find table metadata header on 'tables' sheet")


def find_required_header(
    df: pd.DataFrame,
    required_headers: set[str],
    *,
    max_rows: int = 20,
    max_columns: int = 20,
) -> tuple[int, dict[str, int]]:
    for row_index in range(min(len(df.index), max_rows)):
        current_headers: dict[str, int] = {}
        for column_index in range(min(len(df.columns), max_columns)):
            header = normalize_text(df.iat[row_index, column_index]).lower()
            if header:
                current_headers[header] = column_index
        if required_headers <= set(current_headers):
            return row_index, current_headers
    required_text = ", ".join(sorted(required_headers))
    raise ValueError(f"could not find required header: {required_text}")


def parse_optional_bool(value: Any, field_name: str) -> bool | None:
    text = normalize_text(value)
    if not text:
        return None

    lowered = text.lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"invalid boolean for {field_name}: {value!r}")


def normalize_generator_type(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        raise ValueError("generator_data_type must not be empty")

    normalized = GENERATOR_TYPE_ALIASES.get(text.upper())
    if not normalized:
        raise ValueError(f"unsupported generator_data_type: {value!r}")
    return normalized


def normalize_output_type(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    normalized = GENERATOR_TYPE_ALIASES.get(text.upper())
    if not normalized:
        raise ValueError(f"unsupported output_data_type: {value!r}")
    return normalized


def normalize_write_mode(value: Any) -> str:
    text = normalize_text(value).upper()
    if not text:
        raise ValueError("write_mode must not be empty")
    if text not in WRITE_MODES:
        supported = ", ".join(sorted(WRITE_MODES))
        raise ValueError(f"unsupported write_mode: {value!r}. Supported: {supported}")
    return text


def normalize_engine_scope(value: Any) -> str:
    text = normalize_text(value).lower()
    if not text:
        return "both"
    if text not in ENGINE_SCOPES:
        supported = ", ".join(sorted(ENGINE_SCOPES))
        raise ValueError(f"unsupported engine_scope: {value!r}. Supported: {supported}")
    return text


def parse_csv_list(raw_value: Any) -> list[str]:
    text = normalize_text(raw_value)
    if not text:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for chunk in text.split(","):
        item = normalize_text(chunk)
        if not item:
            continue
        if item not in seen:
            seen.add(item)
            items.append(item)
    return items


def split_semicolon_pairs(raw_text: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quote_char: str | None = None
    square_depth = 0
    curly_depth = 0

    for char in raw_text:
        if quote_char:
            current.append(char)
            if char == quote_char:
                quote_char = None
            continue

        if char in {'"', "'"}:
            quote_char = char
            current.append(char)
            continue

        if char == "[":
            square_depth += 1
        elif char == "]":
            square_depth = max(0, square_depth - 1)
        elif char == "{":
            curly_depth += 1
        elif char == "}":
            curly_depth = max(0, curly_depth - 1)

        if char == ";" and square_depth == 0 and curly_depth == 0:
            token = "".join(current).strip()
            if token:
                parts.append(token)
            current = []
            continue

        current.append(char)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_scalar(raw_value: str) -> Any:
    value = raw_value.strip()
    if value == "":
        return ""
    if value.startswith("[") or value.startswith("{"):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON value: {value!r}") from exc

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    if INT_PATTERN.fullmatch(value):
        return int(value)
    if FLOAT_PATTERN.fullmatch(value):
        return float(value)
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value


def parse_key_value_text(raw_text: Any, field_name: str) -> dict[str, Any]:
    text = normalize_text(raw_text)
    if not text:
        return {}

    result: dict[str, Any] = {}
    for chunk in split_semicolon_pairs(text):
        if "=" not in chunk:
            raise ValueError(f"{field_name} contains malformed fragment: {chunk!r}")

        key, value = chunk.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            raise ValueError(f"{field_name} contains empty key")
        result[normalized_key] = parse_scalar(value)
    return result


def normalize_constraints(raw_constraints: dict[str, Any], generator_type: str) -> dict[str, Any]:
    constraints: dict[str, Any] = {}
    legacy_flags: dict[str, Any] = {}

    for raw_key, raw_value in raw_constraints.items():
        key = LEGACY_CONSTRAINT_ALIASES.get(raw_key.strip(), raw_key.strip())

        if key in {"lowercase", "uppercase", "digits_only", "chars_only"}:
            legacy_flags[key] = raw_value
            continue

        if generator_type == DataType.TIMESTAMP.value and key == "date_format":
            key = "timestamp_format"

        if key not in SUPPORTED_CONSTRAINTS[generator_type]:
            supported = ", ".join(sorted(SUPPORTED_CONSTRAINTS[generator_type]))
            raise ValueError(f"unsupported constraint {raw_key!r} for {generator_type}. Supported: {supported}")

        if key == "null_ratio":
            if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
                raise ValueError("null_ratio must be a number in [0, 1]")
            if not 0 <= float(raw_value) <= 1:
                raise ValueError("null_ratio must be in [0, 1]")
            constraints[key] = raw_value
            continue

        if key == "is_unique":
            if not isinstance(raw_value, bool):
                raise ValueError("is_unique must be true/false")
            constraints[key] = raw_value
            continue

        if key == "allowed_values":
            if not isinstance(raw_value, list):
                raise ValueError("allowed_values must be a JSON array")
            constraints[key] = raw_value
            continue

        if key in {"length", "precision", "digits_count"}:
            if isinstance(raw_value, bool):
                raise ValueError(f"{key} must be integer")
            constraints[key] = int(raw_value)
            continue

        if key in {"regular_expr", "date_format", "timestamp_format", "min_timestamp", "max_timestamp"}:
            text = normalize_text(raw_value)
            if not text:
                raise ValueError(f"{key} must not be empty")
            constraints[key] = text
            continue

        constraints[key] = raw_value

    if "greater_than" in raw_constraints or "less_than" in raw_constraints:
        raise ValueError("greater_than/less_than are no longer supported in current generator contract")

    if generator_type == DataType.STRING.value:
        apply_legacy_string_flags(constraints, legacy_flags)

    return constraints


def apply_legacy_string_flags(constraints: dict[str, Any], legacy_flags: dict[str, Any]) -> None:
    lowercase = legacy_flags.get("lowercase")
    uppercase = legacy_flags.get("uppercase")
    digits_only = legacy_flags.get("digits_only")
    chars_only = legacy_flags.get("chars_only")

    if lowercase:
        constraints.setdefault("case_mode", CaseMode.LOWER.value)
    if uppercase:
        constraints.setdefault("case_mode", CaseMode.UPPER.value)
    if lowercase and uppercase:
        raise ValueError("lowercase=true and uppercase=true cannot be used together")

    if digits_only:
        constraints.setdefault("character_set", CharacterSet.DIGITS.value)
    if chars_only:
        constraints.setdefault("character_set", CharacterSet.LETTERS.value)
    if digits_only and chars_only:
        raise ValueError("digits_only=true and chars_only=true cannot be used together")


def normalize_foreign_key(raw_foreign_key: dict[str, Any]) -> dict[str, Any] | None:
    if not raw_foreign_key:
        return None

    table_name = normalize_text(raw_foreign_key.get("table_name"))
    column_name = normalize_text(raw_foreign_key.get("column_name"))
    relation_type = normalize_text(raw_foreign_key.get("relation_type")).upper()

    if not table_name or not column_name or not relation_type:
        raise ValueError("foreign_key must contain table_name, column_name and relation_type")

    allowed_relation_types = {item.value for item in RelationType}
    if relation_type not in allowed_relation_types:
        raise ValueError(f"unsupported relation_type: {relation_type!r}")

    return {
        "table_name": table_name,
        "column_name": column_name,
        "relation_type": relation_type,
    }


def normalize_foreign_key_reference(raw_foreign_key: dict[str, Any]) -> dict[str, str] | None:
    if not raw_foreign_key:
        return None

    table_name = normalize_text(raw_foreign_key.get("table_name"))
    column_name = normalize_text(raw_foreign_key.get("column_name"))
    relation_type = normalize_text(raw_foreign_key.get("relation_type")).upper()

    if not table_name or not column_name or not relation_type:
        raise ValueError("foreign_key must contain table_name, column_name and relation_type")

    allowed_relation_types = {item.value for item in RelationType}
    if relation_type not in allowed_relation_types:
        raise ValueError(f"unsupported relation_type: {relation_type!r}")

    return {
        "table_name": table_name,
        "column_name": column_name,
        "relation_type": relation_type,
    }


def normalize_derive_spec(raw_derive: dict[str, Any]) -> dict[str, str] | None:
    if not raw_derive:
        return None

    source_column = normalize_text(raw_derive.get("source_column"))
    rule = normalize_text(raw_derive.get("rule")).upper()

    if not source_column or not rule:
        raise ValueError("derive must contain source_column and rule")

    if rule not in DERIVE_RULES:
        supported = ", ".join(sorted(DERIVE_RULES))
        raise ValueError(f"unsupported derive rule: {rule!r}. Supported: {supported}")

    return {
        "source_column": source_column,
        "rule": rule,
    }
