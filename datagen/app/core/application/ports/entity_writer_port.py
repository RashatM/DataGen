from abc import ABC, abstractmethod

from app.core.domain.entities import MockDataEntityResult


class IEntityWriter(ABC):
    @abstractmethod
    def persist_entity_result(self, entity_result: MockDataEntityResult) -> None:
        pass
