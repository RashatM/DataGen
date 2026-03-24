from abc import ABC, abstractmethod

from app.core.application.dto.publication import EngineLoadPayload
from app.core.domain.entities import TableSpec


class TableLoadPayloadBuilderPort(ABC):
    @abstractmethod
    def build_load_payload(self, table: TableSpec) -> EngineLoadPayload:
        pass
