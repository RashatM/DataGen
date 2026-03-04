from dataclasses import asdict, dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class TablePublication:
    contract_version: str
    storage_type: str
    schema_name: str
    table_name: str
    run_id: str
    storage: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
