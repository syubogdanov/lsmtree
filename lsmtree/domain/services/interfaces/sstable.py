from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.entities.level import Level
from lsmtree.utils.typing import SortedIterable, SortedIterator


@dataclass
class SortedStringTable(Protocol):
    """Интерфейс структуры данных `SSTable`."""

    level: Level

    @abstractmethod
    def get(self: Self, key: Bytes32) -> Bytes32 | None:
        """Получить значение по ключу."""

    @abstractmethod
    def from_iterable(self: Self, iterable: SortedIterable[tuple[Bytes32, Bytes32 | None]]) -> None:
        """Построить песочную `SSTable`."""

    @abstractmethod
    def over_sandbox(self: Self) -> SortedIterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по таблице из песочницы."""

    @abstractmethod
    def __contains__(self: Self, key: Bytes32) -> bool:
        """Проверить наличие ключа."""

    @abstractmethod
    def __iter__(self: Self) -> SortedIterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по таблице."""
