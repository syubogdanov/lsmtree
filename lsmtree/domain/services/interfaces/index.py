from abc import abstractmethod
from typing import Protocol, Self

from lsmtree.domain.entities.key import Key
from lsmtree.utils.typing import UnsignedInt


class OffsetIndex(Protocol):
    """Интерфейс индекса смещений."""

    @abstractmethod
    def get(self: Self, key: Key) -> UnsignedInt | None:
        """Получить значение по ключу."""

    @abstractmethod
    def set(self: Self, key: Key, offset: UnsignedInt) -> bool:
        """Задать смещение, если оно еще не задано."""
