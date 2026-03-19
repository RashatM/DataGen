from dataclasses import dataclass
from typing import Dict, List

from app.core.application.dto.publication import TablePublication


@dataclass(slots=True)
class ArtifactPublicationResult:
    table_publications: List[TablePublication]
    comparison_query_uris: Dict[str, str]
