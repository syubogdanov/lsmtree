from abc import abstractmethod
from collections.abc import Iterator
from typing import Protocol, Self

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value


class SortedStringTable(Protocol):
    """Интерфейс структуры данных `SSTable`."""

    @abstractmethod
    def get(self: Self, key: Key) -> Value | None:
        """Получить значение по ключу."""

    @abstractmethod
    def __contains__(self: Self, key: Key) -> bool:
        """Проверить наличие ключа."""

    @abstractmethod
    def __iter__(self: Self) -> Iterator[tuple[Key, Value | None]]:
        """Получить итератор по упорядоченным парам."""
