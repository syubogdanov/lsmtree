from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32


@dataclass
class WriteAheadLog(Protocol):
    """Интерфейс журнала предзаписи."""

    path: Path

    @abstractmethod
    def write(self: Self, key: Bytes32, value: Bytes32 | None) -> None:
        """Записать значение по ключу."""

    @abstractmethod
    def clear(self: Self) -> None:
        """Очистить журнал предзаписи."""

    @abstractmethod
    def __iter__(self: Self) -> Iterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по журналу."""
