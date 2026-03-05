from abc import ABC, abstractmethod

from app.core.domain.entities import GeneratedTableData


class IEntityWriter(ABC):
    @abstractmethod
    def persist_generated_table_data(self, table_data: GeneratedTableData) -> None:
        pass
