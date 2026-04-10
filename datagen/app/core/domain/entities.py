from dataclasses import dataclass
from typing import Generic

from app.core.domain.constraints import OutputConstraints
from app.core.domain.enums import DataType, DerivationRule, RelationType
from app.core.domain.typevars import TConstraints
from app.core.domain.value_types import GeneratedColumnsByName


@dataclass
class TableForeignKeySpec:
    """Описывает ссылку колонки на родительскую таблицу и тип кардинальности связи."""
    table_name: str
    column_name: str
    relation_type: RelationType


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


@dataclass
class TableColumnSpec(Generic[TConstraints]):
    """Единая спецификация колонки таблицы независимо от того, генерируется она, выводится или берётся из FK."""
    name: str
    output_data_type: DataType
    output_constraints: OutputConstraints
    is_primary_key: bool = False
    generation: ColumnGenerationSpec[TConstraints] | None = None
    foreign_key: TableForeignKeySpec | None = None
    derivation: TableDerivationSpec | None = None

    def __post_init__(self) -> None:
        active_modes = sum(
            candidate is not None
            for candidate in (self.generation, self.foreign_key, self.derivation)
        )
        if active_modes != 1:
            raise ValueError(
                f"Column {self.name} must declare exactly one mode: generation, foreign_key or derivation"
            )

    @property
    def is_generated(self) -> bool:
        return self.generation is not None

    @property
    def is_foreign_key(self) -> bool:
        return self.foreign_key is not None

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
            raise ValueError("table_name must not be empty")

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


class DuplicateTableSpecInRunError(ValueError):
    """Поднимается, когда один run пытается содержать две таблицы с одинаковым table_name."""
    pass


class DuplicateColumnSpecInTableError(ValueError):
    """Поднимается, когда спецификация таблицы содержит повторяющиеся имена колонок."""
    pass


@dataclass
class GenerationRun:
    """Корневой объект генерации, объединяющий run_id и набор таблиц для одного запуска."""
    run_id: str
    tables: list[TableSpec]

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")

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
