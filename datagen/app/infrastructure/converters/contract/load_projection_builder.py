from collections.abc import Mapping, Sequence
from typing import Any, cast

from app.application.constants import EngineScope
from app.infrastructure.converters.contract.fields import require_non_empty_string
from app.infrastructure.errors import SchemaValidationError


def parse_engine_scope(
    table_name: str,
    column_name: str,
    column_data: Mapping[str, Any],
) -> EngineScope:
    try:
        raw_engine_scope = column_data.get("engine_scope") or EngineScope.ALL.value
        engine_scope_value = require_non_empty_string(
            raw_engine_scope,
            f"Column {table_name}.{column_name} engine_scope",
        ).upper()
        return EngineScope(engine_scope_value)
    except ValueError as exc:
        raise SchemaValidationError(
            f"Unsupported engine_scope for column {table_name}.{column_name}: {column_data.get('engine_scope')}"
        ) from exc


def build_engine_load_columns(
    table_name: str,
    raw_columns: Sequence[Mapping[str, Any]],
    engine_name: str,
    included_scopes: set[EngineScope],
) -> tuple[str, ...]:
    load_columns: list[str] = []
    for column_data in raw_columns:
        column_name = cast(str, column_data["name"])
        engine_scope = parse_engine_scope(
            table_name=table_name,
            column_name=column_name,
            column_data=column_data,
        )
        if engine_scope in included_scopes:
            load_columns.append(column_name)

    return validate_engine_load_columns(
        table_name=table_name,
        engine_name=engine_name,
        load_columns=tuple(load_columns),
    )


def build_hive_load_columns(table_name: str, raw_columns: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    return build_engine_load_columns(
        table_name=table_name,
        raw_columns=raw_columns,
        engine_name="hive",
        included_scopes={EngineScope.ALL, EngineScope.HIVE_ONLY},
    )


def build_iceberg_load_columns(table_name: str, raw_columns: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    return build_engine_load_columns(
        table_name=table_name,
        raw_columns=raw_columns,
        engine_name="iceberg",
        included_scopes={EngineScope.ALL, EngineScope.ICEBERG_ONLY},
    )


def validate_engine_load_columns(
    table_name: str,
    engine_name: str,
    load_columns: tuple[str, ...],
) -> tuple[str, ...]:
    if not load_columns:
        raise SchemaValidationError(
            f"Table {table_name} has no load columns for engine={engine_name}"
        )

    return load_columns
