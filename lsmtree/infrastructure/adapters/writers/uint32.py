from dataclasses import dataclass
from io import BufferedWriter
from typing import Self

from lsmtree.domain.dtypes.uint32 import Uint32
from lsmtree.domain.services.interfaces.writer import Writer as Interface


@dataclass
class Writer(Interface[int]):
    """Оператор записи."""

    buffer: BufferedWriter

    def write(self: Self, data: int) -> None:
        """Записать значение."""
        self.buffer.write(data.to_bytes(Uint32.bytes))
