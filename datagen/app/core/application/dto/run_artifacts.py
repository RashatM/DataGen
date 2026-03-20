from dataclasses import dataclass
from typing import List

from app.core.application.dto.publication import EnginePair, TablePublication


@dataclass(slots=True)
class ArtifactPublicationResult:
    table_publications: List[TablePublication]
    comparison_query_uris: EnginePair[str]
