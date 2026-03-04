from abc import ABC, abstractmethod
from typing import Any, Dict


class IObjectStorage(ABC):
    """Интерфейс объекта для записи данных в объектное хранилище."""

    @abstractmethod
    def put_text(self, key: str, content: str) -> str:
        """Сохраняет текстовый контент, возвращает URI."""
        pass

    @abstractmethod
    def put_json(self, key: str, payload: Dict[str, Any]) -> str:
        """Сохраняет JSON, возвращает URI."""
        pass

    @abstractmethod
    def put_bytes(self, key: str, body: bytes) -> str:
        """Сохраняет бинарные данные, возвращает URI."""
        pass

    @abstractmethod
    def build_uri(self, key: str) -> str:
        """Строит URI для ключа без записи объекта."""
        pass

    @abstractmethod
    def get_json(self, key: str) -> Dict[str, Any]:
        """Читает JSON по ключу."""
        pass

    @abstractmethod
    def get_bytes(self, key: str) -> bytes:
        """Читает бинарные данные по ключу."""
        pass
