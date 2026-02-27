from dataclasses import dataclass, field
from typing import Any, Dict, Generic, List, Optional

from app.core.domain.enums import DataType, RelationType
from app.core.domain.typevars import TConstraints


@dataclass
class MockDataForeignKey:
    schema_name: str
    table_name: str
    column_name: str
    relation_type: RelationType

    full_table_name: str = field(init=False)

    def __post_init__(self) -> None:
        self.full_table_name = f"{self.schema_name}.{self.table_name}"



@dataclass
class MockDataColumn(Generic[TConstraints]):
    name: str
    gen_data_type: DataType
    is_primary_key: bool
    constraints: TConstraints
    foreign_key: Optional[MockDataForeignKey]
    output_data_type: Optional[DataType] = None

    @property
    def data_type(self) -> DataType:
        return self.gen_data_type

    def __post_init__(self):
        if not self.output_data_type:
            self.output_data_type = self.gen_data_type


@dataclass
class MockDataEntity:
    schema_name: str
    table_name: str
    columns: List[MockDataColumn]
    total_rows: int
    full_table_name: str = field(init=False)

    def __hash__(self) -> int:
        return hash(self.full_table_name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, MockDataEntity) and self.full_table_name == other.full_table_name

    def __post_init__(self) -> None:
        self.full_table_name = f"{self.schema_name}.{self.table_name}"


@dataclass
class MockDataEntityResult:
    entity: MockDataEntity
    generated_data: Dict[str, List[Any]]
