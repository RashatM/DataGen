from dataclasses import dataclass
from typing import Generic

from app.domain.constraints import OutputConstraints
from app.domain.enums import DataType, DerivationRule, ReferenceCardinality
from app.domain.typevars import TConstraints
from app.domain.validation_errors import (
    DuplicateColumnSpecInTableError,
    DuplicateTableSpecInRunError,
    InvalidDerivationError,
    InvalidEntityError,
)
from app.domain.value_types import GeneratedColumnsByName


@dataclass
class TableReferenceSpec:
    """Описывает источник значений reference-колонки и кардинальность их использования."""
    table_name: str
    column_name: str
    cardinality: ReferenceCardinality

    def __post_init__(self) -> None:
        if not self.table_name.strip():
            raise InvalidEntityError("Reference table_name must not be empty")
        if not self.column_name.strip():
            raise InvalidEntityError("Reference column_name must not be empty")


@dataclass(frozen=True)
class ColumnGenerationSpec(Generic[TConstraints]):
    """Хранит исходный тип генератора и его ограничения для обычной генерируемой колонки."""
    source_data_type: DataType
    constraints: TConstraints


@dataclass(frozen=True)
class TableDerivationSpec:
    """Описывает правило вычисления производной колонки из уже сгенерированной исходной."""
    source_column: str
    rule: DerivationRule

    def __post_init__(self) -> None:
        if not self.source_column.strip():
            raise InvalidEntityError("Derived column source_column must not be empty")


@dataclass
class TableColumnSpec(Generic[TConstraints]):
    """Единая спецификация колонки таблицы независимо от того, генерируется она, выводится или берётся из reference."""
    name: str
    output_data_type: DataType
    output_constraints: OutputConstraints
    generation: ColumnGenerationSpec[TConstraints] | None = None
    reference: TableReferenceSpec | None = None
    derivation: TableDerivationSpec | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise InvalidEntityError("Column name must not be empty")
        active_modes = sum(
            candidate is not None
            for candidate in (self.generation, self.reference, self.derivation)
        )
        if active_modes != 1:
            raise InvalidEntityError(
                f"Column {self.name} must declare exactly one mode: generation, reference or derivation"
            )
        if self.is_derived and self.output_constraints.is_unique:
            raise InvalidDerivationError(f"Derived column {self.name} cannot be unique")

    @property
    def is_generated(self) -> bool:
        return self.generation is not None

    @property
    def is_reference(self) -> bool:
        return self.reference is not None

    @property
    def is_derived(self) -> bool:
        return self.derivation is not None

    @property
    def generator_data_type(self) -> DataType:
        if self.generation is None:
            raise AttributeError(f"Column {self.name} does not have generator_data_type")
        return self.generation.source_data_type

    @property
    def generator_constraints(self) -> TConstraints:
        if self.generation is None:
            raise AttributeError(f"Column {self.name} does not have generator_constraints")
        return self.generation.constraints


@dataclass
class TableSpec:
    """Спецификация таблицы в доменной модели с уникальной идентичностью по table_name."""
    table_name: str
    columns: list[TableColumnSpec]
    total_rows: int

    def __post_init__(self) -> None:
        if not self.table_name.strip():
            raise InvalidEntityError("table_name must not be empty")
        if self.total_rows <= 0:
            raise InvalidEntityError(f"Table {self.table_name} must have total_rows greater than 0")
        if not self.columns:
            raise InvalidEntityError(f"Table {self.table_name} must contain at least one column")

        seen: set[str] = set()
        duplicates: set[str] = set()
        for column in self.columns:
            column_name = column.name
            if column_name in seen:
                duplicates.add(column_name)
            else:
                seen.add(column_name)

        if duplicates:
            duplicate_text = ", ".join(sorted(duplicates))
            raise DuplicateColumnSpecInTableError(
                f"Duplicate columns are not allowed in table {self.table_name}: {duplicate_text}"
            )

    def __hash__(self) -> int:
        return hash(self.table_name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TableSpec) and self.table_name == other.table_name


@dataclass
class GeneratedTableData:
    """Готовый результат генерации одной таблицы: доменная схема плюс значения по колонкам."""
    table: TableSpec
    generated_data: GeneratedColumnsByName



@dataclass
class GenerationRun:
    """Корневой объект генерации, объединяющий run_id и набор таблиц для одного запуска."""
    run_id: str
    tables: list[TableSpec]

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise InvalidEntityError("run_id must not be empty")
        if not self.tables:
            raise InvalidEntityError("GenerationRun must contain at least one table")

        seen = set()
        duplicates = set()

        for table in self.tables:
            table_name = table.table_name
            if table_name in seen:
                duplicates.add(table_name)
            else:
                seen.add(table_name)

        if duplicates:
            ordered_duplicates = sorted(duplicates)
            duplicate_text = ", ".join(ordered_duplicates)
            raise DuplicateTableSpecInRunError(
                f"Duplicate table specs are not allowed in GenerationRun: {duplicate_text}"
            )
