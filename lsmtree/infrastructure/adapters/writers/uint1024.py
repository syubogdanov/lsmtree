from dataclasses import dataclass
from io import BufferedWriter
from typing import Self

from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.services.interfaces.writer import Writer as Interface


@dataclass
class Writer(Interface[int]):
    """Оператор записи."""

    buffer: BufferedWriter

    def write(self: Self, data: int) -> None:
        """Записать значение."""
        self.buffer.write(data.to_bytes(Uint1024.bytes))
