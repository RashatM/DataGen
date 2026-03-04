from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class MockEntityArtifacts:
    data_uri: str
    ddl_uris: Dict[str, str]
    pointer_uri: str
