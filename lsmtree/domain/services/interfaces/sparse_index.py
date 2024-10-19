from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.utils.typing import NonNegativeInt, SortedIterable


@dataclass
class SparseIndex(Protocol):
    """Разреженный индекс."""

    path: Path

    @abstractmethod
    def get(self: Self, key: Bytes32) -> Uint1024:
        """Получить ближайшее к ключу смещение."""

    @abstractmethod
    def from_iterable(
        self: Self,
        iterable: SortedIterable[tuple[Bytes32, Uint1024]],
        distance: NonNegativeInt,
    ) -> None:
        """Построить индекс."""
