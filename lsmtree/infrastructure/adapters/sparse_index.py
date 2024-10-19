from dataclasses import dataclass
from pathlib import Path
from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.services.interfaces.sparse_index import SparseIndex as Interface
from lsmtree.infrastructure.adapters.readers.keyoffset import Reader
from lsmtree.infrastructure.adapters.writers.keyoffset import Writer
from lsmtree.utils.itertools import distanced
from lsmtree.utils.typing import NonNegativeInt, SortedIterable


@dataclass
class SparseIndex(Interface):
    """Разреженный индекс."""

    path: Path

    def get(self: Self, key: Bytes32) -> Uint1024:
        """Получить ближайшее к ключу смещение."""
        offset = Uint1024(0)

        with self.path.open(mode="rb") as buffer:
            reader = Reader(buffer)

            while reader.has_next():
                next_key, next_offset = reader.read()

                if key < next_key:
                    break

                offset = next_offset

        return offset

    def from_iterable(
        self: Self,
        iterable: SortedIterable[tuple[Bytes32, Uint1024]],
        distance: NonNegativeInt,
    ) -> None:
        """Построить индекс."""
        with self.path.open(mode="wb") as buffer:
            writer = Writer(buffer)

            for key, offset in distanced(iterable, distance):
                pair = (key, offset)
                writer.write(pair)
