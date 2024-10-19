from abc import abstractmethod
from dataclasses import dataclass
from typing import Protocol, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32


@dataclass
class SortedStringTable(Protocol):
    """Интерфейс структуры данных `SSTable`."""

    @abstractmethod
    def get(self: Self, key: Bytes32) -> Bytes32 | None:
        """Получить значение по ключу."""

    @abstractmethod
    def __contains__(self: Self, key: Bytes32) -> bool:
        """Проверить наличие ключа."""
