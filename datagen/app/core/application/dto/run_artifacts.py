from dataclasses import dataclass

from app.core.application.dto.publication import EnginePair, TablePublication


@dataclass(slots=True)
class ArtifactPublicationResult:
    table_publications: list[TablePublication]
    comparison_query_uris: EnginePair[str]
