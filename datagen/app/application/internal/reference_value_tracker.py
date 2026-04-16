from dataclasses import dataclass, field

from app.application.errors import GenerationInvariantError
from app.domain.entities import TableColumnSpec, TableSpec
from app.domain.value_types import ColumnValues, GeneratedColumnsByName, NonNullColumnValues


@dataclass(slots=True)
class ParentTableBuildState:
    """Временное состояние parent table на этапе подготовки reference-трекера."""

    required_parent_columns: set[str] = field(default_factory=set)
    dependent_tables: set[str] = field(default_factory=set)


@dataclass(slots=True)
class ParentTableReferenceState:
    """Runtime-состояние parent table, значения которой используются reference-колонками."""

    required_columns: frozenset[str]
    remaining_dependent_table_count: int
    cached_values_by_column: dict[str, NonNullColumnValues] = field(default_factory=dict)


class ReferenceValueTracker:
    """Управляет кэшем non-null parent values для reference-колонок во время генерации."""

    def __init__(self, state_by_parent_table: dict[str, ParentTableReferenceState]) -> None:
        self.state_by_parent_table = state_by_parent_table

    @classmethod
    def from_ordered_tables(cls, ordered_tables: list[TableSpec]) -> "ReferenceValueTracker":
        build_state_by_parent_table: dict[str, ParentTableBuildState] = {}

        for table in ordered_tables:
            for table_column in table.columns:
                reference_spec = table_column.reference
                if not reference_spec:
                    continue

                parent_build_state = build_state_by_parent_table.setdefault(
                    reference_spec.table_name,
                    ParentTableBuildState(),
                )
                parent_build_state.required_parent_columns.add(reference_spec.column_name)
                parent_build_state.dependent_tables.add(table.table_name)

        state_by_parent_table = {
            parent_table_name: ParentTableReferenceState(
                required_columns=frozenset(parent_build_state.required_parent_columns),
                remaining_dependent_table_count=len(parent_build_state.dependent_tables),
            )
            for parent_table_name, parent_build_state in build_state_by_parent_table.items()
        }
        return cls(state_by_parent_table)

    def get_parent_values(self, table_column: TableColumnSpec) -> NonNullColumnValues:
        reference_spec = table_column.reference
        if reference_spec is None:
            raise GenerationInvariantError(f"Column {table_column.name} is not a reference")

        parent_state = self.state_by_parent_table.get(reference_spec.table_name)
        if not parent_state:
            raise GenerationInvariantError(f"Missing reference tracker state for table {reference_spec.table_name}")

        cached_parent_values = parent_state.cached_values_by_column.get(reference_spec.column_name)
        if cached_parent_values is not None:
            return cached_parent_values

        raise GenerationInvariantError(
            f"Missing cached reference values for {reference_spec.table_name}.{reference_spec.column_name}"
        )

    @staticmethod
    def filter_non_null_values(values: ColumnValues) -> NonNullColumnValues:
        return [value for value in values if value is not None]

    def cache_parent_values(
        self,
        table: TableSpec,
        generated_columns_by_name: GeneratedColumnsByName,
    ) -> None:
        parent_state = self.state_by_parent_table.get(table.table_name)
        if not parent_state:
            return

        cached_values_by_column: dict[str, NonNullColumnValues] = {}
        for column_name in parent_state.required_columns:
            column_values = generated_columns_by_name[column_name]
            cached_values_by_column[column_name] = self.filter_non_null_values(column_values)

        parent_state.cached_values_by_column = cached_values_by_column

    def release_parent_cache_for_child(self, table: TableSpec) -> None:
        parent_tables_referenced_by_child: set[str] = set()
        for table_column in table.columns:
            reference_spec = table_column.reference
            if reference_spec is None:
                continue
            parent_tables_referenced_by_child.add(reference_spec.table_name)

        for parent_table_name in parent_tables_referenced_by_child:
            parent_state = self.state_by_parent_table.get(parent_table_name)
            if not parent_state:
                raise GenerationInvariantError(f"Missing reference tracker state for table {parent_table_name}")

            if parent_state.remaining_dependent_table_count > 1:
                parent_state.remaining_dependent_table_count -= 1
                continue

            del self.state_by_parent_table[parent_table_name]
