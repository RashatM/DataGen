from abc import ABC, abstractmethod
from typing import List, Any

from app.dto.constraints import Constraints


class IMockDataGenerator(ABC):

    @abstractmethod
    def generate_values(self, total_rows: int, constraints: Constraints) -> List[Any]:
        pass
