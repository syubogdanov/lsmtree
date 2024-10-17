from abc import abstractmethod
from collections.abc import Iterator
from typing import Protocol, Self

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value
from lsmtree.utils.typing import UnsignedInt


class MemTable(Protocol):
    """Интерфейс структуры данных `MemTable`."""

    @abstractmethod
    def put(self: Self, key: Key, value: Value | None) -> None:
        """Записать значение по ключу."""

    @abstractmethod
    def get(self: Self, key: Key) -> Value | None:
        """Получить значение по ключу."""

    @abstractmethod
    def clear(self: Self) -> None:
        """Очистить таблицу."""

    @property
    @abstractmethod
    def size(self: Self) -> UnsignedInt:
        """Получить размер хранимых пар в байтах."""

    @abstractmethod
    def __contains__(self: Self, key: Key) -> bool:
        """Проверить наличие ключа."""

    @abstractmethod
    def __iter__(self: Self) -> Iterator[tuple[Key, Value | None]]:
        """Получить итератор по упорядоченным парам."""

    @abstractmethod
    def __len__(self: Self) -> int:
        """Получить количество хранимых пар."""
