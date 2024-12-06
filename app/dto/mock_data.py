from dataclasses import dataclass
from typing import List, Dict, Any

from app.dto.constraints import Constraints
from app.enums import RelationType, DataType, SourceType


@dataclass
class MockDataForeignKey:
    table_name: str
    column_name: str
    relation_type: RelationType


@dataclass
class MockDataColumn:
    name: str
    data_type: DataType
    is_primary_key: bool
    constraints: Constraints
    foreign_key: MockDataForeignKey


@dataclass
class MockDataEntity:
    schema_name: str
    table_name: str
    columns: List[MockDataColumn]
    total_rows: int

    def __hash__(self):
        return hash(self.table_name)

    def __eq__(self, other):
        return isinstance(other, MockDataEntity) and self.table_name == other.table_name

    def __post_init__(self):
        self.full_table_name = f"{self.schema_name}.{self.table_name}"


@dataclass
class MockDataSchema:
    source_type: SourceType
    schema_name: str
    entities: List[MockDataEntity]



@dataclass
class MockDataEntityResult:
    entity: MockDataEntity
    generated_data: Dict[str, List[Any]]


