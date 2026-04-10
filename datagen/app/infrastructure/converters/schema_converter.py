from datetime import date, datetime
from collections.abc import Mapping, Sequence
from typing import Any

from dateutil.parser import parse

from app.core.application.constants import EngineScope, WriteMode
from app.core.application.dto.pipeline import (
    ComparisonQuerySpec,
    PipelineExecutionSpec,
    TableExecutionSpec,
    TableLoadSpec,
)
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
from app.core.domain.derivation import DerivationPolicy
from app.core.domain.entities import (
    ColumnGenerationSpec,
    GenerationRun,
    TableColumnSpec,
    TableDerivationSpec,
    TableForeignKeySpec,
    TableSpec,
)
from app.core.domain.enums import CaseMode, CharacterSet, DataType, DerivationRule, RelationType
from app.core.domain.validation_errors import InvalidConstraintsError, InvalidDerivationError
from app.infrastructure.errors import SchemaValidationError

LEGACY_MAPPING = {
    "DATE_STRING": DataType.DATE,
    "TIMESTAMP_STRING": DataType.TIMESTAMP,
}

FOREIGN_KEY_ALLOWED_CONSTRAINT_FIELDS = {"null_ratio"}
DERIVATION_POLICY = DerivationPolicy()


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


def require_string_list(value: Any, context: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise SchemaValidationError(f"{context} must be a list")

    string_values: list[str] = []
    for item in value:
        string_values.append(require_non_empty_string(item, context))
    return string_values


def optional_string(value: Any, context: str) -> str | None:
    if value is None:
        return None
    return require_non_empty_string(value, context)


def normalize_is_primary_key(column_name: str, raw_is_primary_key: Any) -> bool:
    if raw_is_primary_key is None:
        return False
    if isinstance(raw_is_primary_key, bool):
        return raw_is_primary_key
    raise SchemaValidationError(f"Column {column_name} has invalid is_primary_key: {raw_is_primary_key!r}")


def get_constraints_data(column_name: str, column_data: Mapping[str, Any]) -> dict[str, Any]:
    return optional_mapping(column_data.get("constraints"), f"Column {column_name} constraints") or {}


def normalize_allowed_values(column_name: str, raw_allowed_values: Any) -> tuple[Any, ...] | None:
    if raw_allowed_values is None:
        return None
    if isinstance(raw_allowed_values, (str, bytes)) or not isinstance(raw_allowed_values, list):
        raise SchemaValidationError(f"Column {column_name} allowed_values must be a list")
    return tuple(raw_allowed_values)


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

    try:
        return OutputConstraints(
            null_ratio=null_ratio,
            is_unique=is_unique,
        )
    except InvalidConstraintsError as exc:
        raise SchemaValidationError(f"Column {column_name}: {exc}") from exc


def resolve_data_types(
    column_data: Mapping[str, Any],
    constraints_data: Mapping[str, Any],
) -> tuple[DataType, DataType]:
    column_name = require_non_empty_string(column_data.get("name"), "Column name")
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


def build_foreign_key_spec(column_name: str, foreign_key_data: dict[str, Any]) -> TableForeignKeySpec:
    try:
        return TableForeignKeySpec(
            table_name=require_non_empty_string(
                foreign_key_data.get("table_name"),
                f"Foreign key column {column_name} table_name",
            ),
            column_name=require_non_empty_string(
                foreign_key_data.get("column_name"),
                f"Foreign key column {column_name} column_name",
            ),
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
    column_name = require_non_empty_string(column_data.get("name"), "Column name")
    constraints_data = get_constraints_data(column_name, column_data)
    generator_data_type, output_data_type = resolve_data_types(column_data, constraints_data)
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


class WorkbookTableCompiler:
    """Компилирует raw workbook-таблицы в доменные TableSpec по единому пути с поддержкой FK и derive."""

    def __init__(self, raw_tables: Sequence[Mapping[str, Any]]) -> None:
        self.raw_tables = list(raw_tables)
        self.raw_tables_by_name: dict[str, dict[str, Any]] = {}
        self.raw_columns_by_table: dict[str, dict[str, dict[str, Any]]] = {}
        self.raw_columns_sequence_by_table: dict[str, list[dict[str, Any]]] = {}
        self.resolved_columns: dict[tuple[str, str], TableColumnSpec[Any]] = {}
        self.resolving_stack: set[tuple[str, str]] = set()

        for table_data in self.raw_tables:
            table_name = require_non_empty_string(table_data.get("table_name"), "Table table_name")
            if table_name in self.raw_tables_by_name:
                raise SchemaValidationError(f"Duplicate table_name in workbook contract: {table_name}")

            raw_columns = require_list_of_mappings(table_data.get("columns"), f"Table {table_name} columns")
            raw_columns_by_name: dict[str, dict[str, Any]] = {}
            for column_data in raw_columns:
                column_name = require_non_empty_string(column_data.get("name"), f"Table {table_name} column name")
                if column_name in raw_columns_by_name:
                    raise SchemaValidationError(f"Duplicate column in workbook contract: {table_name}.{column_name}")
                raw_columns_by_name[column_name] = column_data

            self.raw_tables_by_name[table_name] = dict(table_data)
            self.raw_columns_by_table[table_name] = raw_columns_by_name
            self.raw_columns_sequence_by_table[table_name] = list(raw_columns)

    def get_raw_columns(self, table_name: str) -> list[dict[str, Any]]:
        raw_columns = self.raw_columns_sequence_by_table.get(table_name)
        if raw_columns is None:
            raise SchemaValidationError(f"Unknown table referenced in workbook contract: {table_name}")
        return raw_columns

    def resolve_column(self, table_name: str, column_name: str) -> TableColumnSpec[Any]:
        cache_key = (table_name, column_name)
        cached_column = self.resolved_columns.get(cache_key)
        if cached_column is not None:
            return cached_column

        if cache_key in self.resolving_stack:
            raise SchemaValidationError(
                f"Circular column dependency detected while resolving {table_name}.{column_name}"
            )

        table_columns = self.raw_columns_by_table.get(table_name)
        if table_columns is None:
            raise SchemaValidationError(f"Unknown table referenced in workbook contract: {table_name}")

        column_data = table_columns.get(column_name)
        if column_data is None:
            raise SchemaValidationError(f"Unknown column referenced in workbook contract: {table_name}.{column_name}")

        self.resolving_stack.add(cache_key)
        try:
            constraints_data = get_constraints_data(column_name, column_data)
            is_primary_key = normalize_is_primary_key(column_name, column_data.get("is_primary_key"))
            raw_foreign_key = optional_mapping(
                column_data.get("foreign_key"),
                f"Column {table_name}.{column_name} foreign_key",
            )
            raw_derive = optional_mapping(
                column_data.get("derive"),
                f"Column {table_name}.{column_name} derive",
            )

            if raw_foreign_key:
                unsupported_constraint_fields = sorted(
                    set(constraints_data.keys()) - FOREIGN_KEY_ALLOWED_CONSTRAINT_FIELDS
                )
                if unsupported_constraint_fields:
                    unsupported_fields = ", ".join(unsupported_constraint_fields)
                    raise SchemaValidationError(
                        f"Foreign key column {column_name} supports only null_ratio constraint, got: {unsupported_fields}"
                    )

                parent_table_name = require_non_empty_string(
                    raw_foreign_key.get("table_name"),
                    f"Column {table_name}.{column_name} foreign_key table_name",
                )
                parent_column_name = require_non_empty_string(
                    raw_foreign_key.get("column_name"),
                    f"Column {table_name}.{column_name} foreign_key column_name",
                )
                parent_column = self.resolve_column(parent_table_name, parent_column_name)
                resolved_column = TableColumnSpec(
                    name=column_name,
                    output_data_type=parent_column.output_data_type,
                    output_constraints=build_output_constraints(
                        column_name=column_name,
                        constraints_data=constraints_data,
                        is_primary_key=is_primary_key,
                    ),
                    is_primary_key=is_primary_key,
                    foreign_key=build_foreign_key_spec(column_name, raw_foreign_key),
                )
            elif raw_derive:
                if constraints_data:
                    raise SchemaValidationError(f"Derived column {column_name} cannot define constraints")

                output_raw = column_data.get("output_data_type")
                if not output_raw:
                    raise SchemaValidationError(f"Derived column {column_name} must declare output_data_type")

                source_column_name = require_non_empty_string(
                    raw_derive.get("source_column"),
                    f"Derived column {table_name}.{column_name} source_column",
                )
                source_column = self.resolve_column(table_name, source_column_name)
                derivation = TableDerivationSpec(
                    source_column=source_column_name,
                    rule=DerivationRule(
                        require_non_empty_string(
                            raw_derive.get("rule"),
                            f"Derived column {table_name}.{column_name} rule",
                        )
                    ),
                )
                output_data_type = DataType(
                    require_non_empty_string(
                        output_raw,
                        f"Derived column {table_name}.{column_name} output_data_type",
                    ).upper()
                )
                try:
                    DERIVATION_POLICY.validate_derived_column_spec(
                        column_name=column_name,
                        source_column=source_column,
                        derivation=derivation,
                        output_data_type=output_data_type,
                    )
                except InvalidDerivationError as exc:
                    raise SchemaValidationError(str(exc)) from exc

                resolved_column = TableColumnSpec(
                    name=column_name,
                    output_data_type=output_data_type,
                    output_constraints=DERIVATION_POLICY.derive_output_constraints_from_source(source_column),
                    derivation=derivation,
                )
            else:
                resolved_column = build_generated_column_spec(column_data=column_data)

            self.resolved_columns[cache_key] = resolved_column
            return resolved_column
        except SchemaValidationError:
            raise
        except ValueError as exc:
            raise SchemaValidationError(str(exc)) from exc
        finally:
            self.resolving_stack.remove(cache_key)

    def build_table_spec(self, table_name: str) -> TableSpec:
        raw_table = self.raw_tables_by_name.get(table_name)
        if raw_table is None:
            raise SchemaValidationError(f"Unknown table referenced in workbook contract: {table_name}")

        raw_columns = self.get_raw_columns(table_name)
        try:
            return TableSpec(
                table_name=table_name,
                columns=[
                    self.resolve_column(
                        table_name=table_name,
                        column_name=require_non_empty_string(column_data.get("name"), f"Table {table_name} column name"),
                    )
                    for column_data in raw_columns
                ],
                total_rows=require_integer(raw_table.get("total_rows"), f"Table {table_name} total_rows"),
            )
        except SchemaValidationError:
            raise
        except ValueError as exc:
            raise SchemaValidationError(str(exc)) from exc


def convert_to_table_spec(table_data: Mapping[str, Any]) -> TableSpec:
    compiler = WorkbookTableCompiler([table_data])
    table_name = require_non_empty_string(table_data.get("table_name"), "Table table_name")
    return compiler.build_table_spec(table_name)


def parse_engine_scope(
    table_name: str,
    column_name: str,
    column_data: Mapping[str, Any],
) -> EngineScope:
    try:
        raw_engine_scope = column_data.get("engine_scope") or EngineScope.BOTH.value
        return EngineScope(
            require_non_empty_string(
                raw_engine_scope,
                f"Column {table_name}.{column_name} engine_scope",
            )
        )
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
    load_columns = tuple(
        column_name
        for column_data in raw_columns
        for column_name in [require_non_empty_string(column_data.get("name"), f"Table {table_name} column name")]
        if parse_engine_scope(
            table_name=table_name,
            column_name=column_name,
            column_data=column_data,
        ) in included_scopes
    )
    return validate_engine_load_columns(
        table_name=table_name,
        engine_name=engine_name,
        load_columns=load_columns,
    )


def build_hive_load_columns(table_name: str, raw_columns: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    return build_engine_load_columns(
        table_name=table_name,
        raw_columns=raw_columns,
        engine_name="hive",
        included_scopes={EngineScope.BOTH, EngineScope.HIVE_ONLY},
    )


def build_iceberg_load_columns(table_name: str, raw_columns: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    return build_engine_load_columns(
        table_name=table_name,
        raw_columns=raw_columns,
        engine_name="iceberg",
        included_scopes={EngineScope.BOTH, EngineScope.ICEBERG_ONLY},
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


def convert_to_generation_run(run_id: str, raw_tables: Sequence[Mapping[str, Any]]) -> GenerationRun:
    compiler = WorkbookTableCompiler(raw_tables)
    tables = [
        compiler.build_table_spec(
            require_non_empty_string(table_data.get("table_name"), "Table table_name")
        )
        for table_data in raw_tables
    ]
    return GenerationRun(run_id=run_id, tables=tables)


def convert_to_pipeline_execution_spec(raw_workbook_spec: Mapping[str, Any]) -> PipelineExecutionSpec:
    raw_queries = require_mapping(raw_workbook_spec.get("queries"), "Workbook queries")
    comparison = ComparisonQuerySpec(
        hive_sql=require_non_empty_string(raw_queries.get("hive_sql"), "Workbook queries hive_sql"),
        iceberg_sql=require_non_empty_string(raw_queries.get("iceberg_sql"), "Workbook queries iceberg_sql"),
        hive_exclude_columns=tuple(require_string_list(
            raw_queries.get("hive_exclude_columns"),
            "Workbook queries hive_exclude_columns",
        )),
        iceberg_exclude_columns=tuple(require_string_list(
            raw_queries.get("iceberg_exclude_columns"),
            "Workbook queries iceberg_exclude_columns",
        )),
    )

    raw_tables = require_list_of_mappings(raw_workbook_spec.get("tables"), "Workbook tables")
    compiler = WorkbookTableCompiler(raw_tables)

    execution_tables: list[TableExecutionSpec] = []
    for table_data in raw_tables:
        table_name = require_non_empty_string(table_data.get("table_name"), "Table table_name")
        table_columns_data = compiler.get_raw_columns(table_name)
        table_spec = compiler.build_table_spec(table_name)
        try:
            write_mode = WriteMode(require_non_empty_string(table_data.get("write_mode"), f"Table {table_name} write_mode"))
        except ValueError as exc:
            raise SchemaValidationError(
                f"Unsupported write_mode for table {table_name}: {table_data.get('write_mode')}"
            ) from exc
        execution_tables.append(
            TableExecutionSpec(
                table=table_spec,
                load_spec=TableLoadSpec(
                    hive_target_table=require_non_empty_string(
                        table_data.get("hive_target_table"),
                        f"Table {table_name} hive_target_table",
                    ),
                    iceberg_target_table=require_non_empty_string(
                        table_data.get("iceberg_target_table"),
                        f"Table {table_name} iceberg_target_table",
                    ),
                    write_mode=write_mode,
                    hive_columns=build_hive_load_columns(
                        table_name=table_name,
                        raw_columns=table_columns_data,
                    ),
                    iceberg_columns=build_iceberg_load_columns(
                        table_name=table_name,
                        raw_columns=table_columns_data,
                    ),
                ),
            )
        )

    return PipelineExecutionSpec(
        tables=tuple(execution_tables),
        comparison=comparison,
    )
