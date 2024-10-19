from dataclasses import dataclass
from io import BufferedWriter
from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.services.interfaces.writer import Writer as Interface
from lsmtree.infrastructure.adapters.writers.bytes32 import Writer as Bytes32Writer
from lsmtree.infrastructure.adapters.writers.uint1024 import Writer as Uint1024Writer


@dataclass
class Writer(Interface[tuple[Bytes32, Uint1024]]):
    """Оператор записи."""

    buffer: BufferedWriter

    def write(self: Self, data: tuple[Bytes32, Uint1024]) -> None:
        """Записать значение."""
        key, offset = data

        Bytes32Writer(self.buffer).write(key)
        Uint1024Writer(self.buffer).write(offset)
