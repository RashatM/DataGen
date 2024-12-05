from dataclasses import dataclass
from typing import List, Dict, Any

from app.dto.constraints import Constraints
from app.enums import RelationType, DataType


@dataclass
class MockForeignKey:
    table_name: str
    column_name: str
    relation_type: RelationType


@dataclass
class MockColumn:
    name: str
    data_type: DataType
    is_primary_key: bool
    constraints: Constraints
    foreign_key: MockForeignKey


@dataclass
class MockEntity:
    table_name: str
    columns: List[MockColumn]
    total_rows: int

    def __hash__(self):
        return hash(self.table_name)

    def __eq__(self, other):
        return isinstance(other, MockEntity) and self.table_name == other.table_name


@dataclass
class MockSchema:
    id: int
    entities: List[MockEntity]



@dataclass
class MockEntityResult:
    table_name: str
    entity: MockEntity
    generated_data: Dict[str, List[Any]]


