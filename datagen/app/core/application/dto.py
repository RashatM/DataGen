from dataclasses import asdict, dataclass
from typing import Any, Dict


@dataclass(slots=True)
class TablePublication:
    storage_type: str
    schema_name: str
    table_name: str
    run_id: str
    storage: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
