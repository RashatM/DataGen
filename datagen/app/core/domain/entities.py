from dataclasses import dataclass, field
from typing import Any, Generic

from app.core.domain.constraints import OutputConstraints
from app.core.domain.enums import DataType, RelationType
from app.core.domain.typevars import TConstraints


@dataclass
class TableForeignKeySpec:
    schema_name: str
    table_name: str
    column_name: str
    relation_type: RelationType

    full_table_name: str = field(init=False)

    def __post_init__(self) -> None:
        self.full_table_name = f"{self.schema_name}.{self.table_name}"


@dataclass
class TableColumnSpec(Generic[TConstraints]):
    name: str
    generator_data_type: DataType
    is_primary_key: bool
    generator_constraints: TConstraints
    output_constraints: OutputConstraints
    foreign_key: TableForeignKeySpec | None
    output_data_type: DataType | None = None

    def __post_init__(self) -> None:
        if self.output_data_type is None:
            self.output_data_type = self.generator_data_type


@dataclass
class TableSpec:
    schema_name: str
    table_name: str
    columns: list[TableColumnSpec]
    total_rows: int
    full_table_name: str = field(init=False)

    def __hash__(self) -> int:
        return hash(self.full_table_name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TableSpec) and self.full_table_name == other.full_table_name

    def __post_init__(self) -> None:
        self.full_table_name = f"{self.schema_name}.{self.table_name}"


@dataclass
class GeneratedTableData:
    table: TableSpec
    generated_data: dict[str, list[Any]]


class DuplicateTableSpecInRunError(ValueError):
    pass


@dataclass
class GenerationRun:
    run_id: str
    tables: list[TableSpec]

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")

        seen = set()
        duplicates = set()

        for table in self.tables:
            full_table_name = table.full_table_name
            if full_table_name in seen:
                duplicates.add(full_table_name)
            else:
                seen.add(full_table_name)

        if duplicates:
            ordered_duplicates = sorted(duplicates)
            duplicate_text = ", ".join(ordered_duplicates)
            raise DuplicateTableSpecInRunError(
                f"Duplicate table specs are not allowed in GenerationRun: {duplicate_text}"
            )
