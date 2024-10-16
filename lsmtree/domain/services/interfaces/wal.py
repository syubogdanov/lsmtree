from abc import abstractmethod
from collections.abc import Iterator
from typing import Protocol, Self

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value


class WriteAheadLog(Protocol):
    """Интерфейс журнала предзаписи."""

    @abstractmethod
    def write(self: Self, key: Key, value: Value | None) -> None:
        """Записать значение по ключу."""

    @abstractmethod
    def clear(self: Self) -> None:
        """Очистить журнал предзаписи."""

    @abstractmethod
    def __iter__(self: Self) -> Iterator[tuple[Key, Value | None]]:
        """Получить итератор по журналу."""
