from abc import abstractmethod
from typing import Protocol, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.services.interfaces.wal import WriteAheadLog
from lsmtree.utils.typing import NonNegativeInt, SortedIterator


class MemTable(Protocol):
    """Интерфейс структуры данных `MemTable`."""

    wal: WriteAheadLog

    @abstractmethod
    def put(self: Self, key: Bytes32, value: Bytes32 | None) -> None:
        """Записать значение по ключу."""

    @abstractmethod
    def get(self: Self, key: Bytes32) -> Bytes32 | None:
        """Получить значение по ключу."""

    @abstractmethod
    def clear(self: Self) -> None:
        """Очистить таблицу."""

    @property
    @abstractmethod
    def size(self: Self) -> NonNegativeInt:
        """Получить размер таблицы в байтах."""

    @abstractmethod
    def __contains__(self: Self, key: Bytes32) -> bool:
        """Проверить наличие ключа."""

    @abstractmethod
    def __iter__(self: Self) -> SortedIterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по упорядоченным парам."""
