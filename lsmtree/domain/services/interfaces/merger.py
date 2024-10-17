from abc import abstractmethod
from dataclasses import dataclass
from io import BufferedWriter
from typing import Protocol, Self

from lsmtree.domain.services.interfaces.sstable import SortedStringTable


@dataclass
class Merger(Protocol):
    """Интерфейс оператора слияния `SSTable`."""

    buffer: BufferedWriter

    @abstractmethod
    def merge(self: Self, new: SortedStringTable, old: SortedStringTable) -> None:
        """Выполнить слияние двух `SSTable`."""
