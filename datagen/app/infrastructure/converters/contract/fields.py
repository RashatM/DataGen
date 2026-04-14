"""Low-level readers for JSON-like raw contract fields."""

from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from dateutil.parser import parse

from app.infrastructure.errors import SchemaValidationError


def require_non_empty_string(value: Any, context: str) -> str:
    if isinstance(value, str) and value.strip():
        return value
    raise SchemaValidationError(f"{context} must be a non-empty string")


def require_integer(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SchemaValidationError(f"{context} must be an integer")
    return value


def require_number(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SchemaValidationError(f"{context} must be a number")
    return float(value)


def require_mapping(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SchemaValidationError(f"{context} must be an object")
    return value


def optional_mapping(value: Any, context: str) -> dict[str, Any] | None:
    if value is None:
        return None
    return require_mapping(value, context)


def require_list_of_mappings(value: Any, context: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise SchemaValidationError(f"{context} must be a list")

    if not all(isinstance(item, dict) for item in value):
        raise SchemaValidationError(f"{context} must contain only objects")

    return value


def optional_string(value: Any, context: str) -> str | None:
    if value is None:
        return None
    return require_non_empty_string(value, context)


def parse_date_literal(column_name: str, field_name: str, raw_value: Any) -> date:
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, str):
        return parse(raw_value).date()
    raise SchemaValidationError(f"Column {column_name} has invalid {field_name}: {raw_value!r}")


def parse_timestamp_literal(column_name: str, field_name: str, raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, date):
        return datetime.combine(raw_value, datetime.min.time())
    if isinstance(raw_value, str):
        return parse(raw_value)
    raise SchemaValidationError(f"Column {column_name} has invalid {field_name}: {raw_value!r}")


def get_constraints_data(column_name: str, column_data: Mapping[str, Any]) -> dict[str, Any]:
    return optional_mapping(column_data.get("constraints"), f"Column {column_name} constraints") or {}
