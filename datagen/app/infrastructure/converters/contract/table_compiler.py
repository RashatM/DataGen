from collections.abc import Mapping, Sequence
from typing import Any, cast

from app.domain.derivation import DerivationPolicy
from app.domain.entities import (
    GenerationRun,
    TableColumnSpec,
    TableDerivationSpec,
    TableSpec,
)
from app.domain.enums import DataType, DerivationRule
from app.domain.validation_errors import InvalidDerivationError
from app.infrastructure.converters.contract.column_builder import (
    build_foreign_key_spec,
    build_generated_column_spec,
    build_output_constraints,
    normalize_is_primary_key,
    validate_foreign_key_constraints,
)
from app.infrastructure.converters.contract.fields import (
    get_constraints_data,
    optional_mapping,
    require_integer,
    require_list_of_mappings,
    require_non_empty_string,
)
from app.infrastructure.errors import SchemaValidationError

DERIVATION_POLICY = DerivationPolicy()


class ContractTableCompiler:
    """Компилирует raw contract tables в доменные TableSpec по единому пути с поддержкой FK и derive."""

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
                raise SchemaValidationError(f"Duplicate table_name in contract: {table_name}")

            raw_columns = require_list_of_mappings(table_data.get("columns"), f"Table {table_name} columns")
            raw_columns_by_name: dict[str, dict[str, Any]] = {}
            for column_data in raw_columns:
                column_name = require_non_empty_string(column_data.get("name"), f"Table {table_name} column name")
                if column_name in raw_columns_by_name:
                    raise SchemaValidationError(f"Duplicate column in contract: {table_name}.{column_name}")
                raw_columns_by_name[column_name] = column_data

            self.raw_tables_by_name[table_name] = dict(table_data)
            self.raw_columns_by_table[table_name] = raw_columns_by_name
            self.raw_columns_sequence_by_table[table_name] = list(raw_columns)

    def get_raw_columns(self, table_name: str) -> list[dict[str, Any]]:
        raw_columns = self.raw_columns_sequence_by_table.get(table_name)
        if raw_columns is None:
            raise SchemaValidationError(f"Unknown table referenced in contract: {table_name}")
        return raw_columns

    def resolve_column(self, table_name: str, column_name: str) -> TableColumnSpec[Any]:
        """Собирает колонку один раз, рекурсивно подтягивая parent/source колонки для FK и derive."""
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
            raise SchemaValidationError(f"Unknown table referenced in contract: {table_name}")

        column_data = table_columns.get(column_name)
        if column_data is None:
            raise SchemaValidationError(f"Unknown column referenced in contract: {table_name}.{column_name}")

        self.resolving_stack.add(cache_key)
        try:
            resolved_column = self.build_column(
                table_name=table_name,
                column_name=column_name,
                column_data=column_data,
            )
            self.resolved_columns[cache_key] = resolved_column
            return resolved_column
        except SchemaValidationError:
            raise
        except ValueError as exc:
            raise SchemaValidationError(str(exc)) from exc
        finally:
            self.resolving_stack.remove(cache_key)

    def build_column(
        self,
        table_name: str,
        column_name: str,
        column_data: Mapping[str, Any],
    ) -> TableColumnSpec[Any]:
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

        if raw_foreign_key and raw_derive:
            raise SchemaValidationError(
                f"Column {table_name}.{column_name} cannot define both foreign_key and derive"
            )
        if raw_foreign_key:
            return self.build_foreign_key_column(
                table_name=table_name,
                column_name=column_name,
                constraints_data=constraints_data,
                is_primary_key=is_primary_key,
                raw_foreign_key=raw_foreign_key,
            )
        if raw_derive:
            return self.build_derived_column(
                table_name=table_name,
                column_name=column_name,
                column_data=column_data,
                constraints_data=constraints_data,
                is_primary_key=is_primary_key,
                raw_derive=raw_derive,
            )
        return build_generated_column_spec(column_data=column_data)

    def build_foreign_key_column(
        self,
        table_name: str,
        column_name: str,
        constraints_data: dict[str, Any],
        is_primary_key: bool,
        raw_foreign_key: dict[str, Any],
    ) -> TableColumnSpec[Any]:
        validate_foreign_key_constraints(
            column_name=column_name,
            constraints_data=constraints_data,
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
        return TableColumnSpec(
            name=column_name,
            output_data_type=parent_column.output_data_type,
            output_constraints=build_output_constraints(
                column_name=column_name,
                constraints_data=constraints_data,
                is_primary_key=is_primary_key,
            ),
            is_primary_key=is_primary_key,
            foreign_key=build_foreign_key_spec(
                column_name=column_name,
                parent_table_name=parent_table_name,
                parent_column_name=parent_column_name,
                foreign_key_data=raw_foreign_key,
            ),
        )

    def build_derived_column(
        self,
        table_name: str,
        column_name: str,
        column_data: Mapping[str, Any],
        constraints_data: dict[str, Any],
        is_primary_key: bool,
        raw_derive: dict[str, Any],
    ) -> TableColumnSpec[Any]:
        if constraints_data:
            raise SchemaValidationError(f"Derived column {column_name} cannot define constraints")
        if is_primary_key:
            raise SchemaValidationError(f"Derived column {column_name} cannot be primary key")

        output_raw = require_non_empty_string(
            column_data.get("output_data_type"),
            f"Derived column {table_name}.{column_name} output_data_type",
        )

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
            output_raw.upper()
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

        return TableColumnSpec(
            name=column_name,
            output_data_type=output_data_type,
            output_constraints=DERIVATION_POLICY.derive_output_constraints_from_source(source_column),
            derivation=derivation,
        )

    def build_table_spec(self, table_name: str) -> TableSpec:
        raw_table = self.raw_tables_by_name.get(table_name)
        if raw_table is None:
            raise SchemaValidationError(f"Unknown table referenced in contract: {table_name}")

        raw_columns = self.get_raw_columns(table_name)
        try:
            return TableSpec(
                table_name=table_name,
                columns=[
                    self.resolve_column(
                        table_name=table_name,
                        column_name=cast(str, column_data["name"]),
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
    compiler = ContractTableCompiler([table_data])
    table_name = cast(str, table_data["table_name"])
    return compiler.build_table_spec(table_name)


def convert_to_generation_run(run_id: str, raw_tables: Sequence[Mapping[str, Any]]) -> GenerationRun:
    compiler = ContractTableCompiler(raw_tables)
    tables = [
        compiler.build_table_spec(cast(str, table_data["table_name"]))
        for table_data in raw_tables
    ]
    return GenerationRun(run_id=run_id, tables=tables)
