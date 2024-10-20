from dataclasses import dataclass
from io import BufferedWriter
from typing import Self

from lsmtree.domain.services.interfaces.writer import Writer as Interface
from lsmtree.infrastructure.adapters.writers.uint32 import Writer as Uint32Writer


@dataclass
class Writer(Interface[bytes]):
    """Оператор записи."""

    buffer: BufferedWriter

    def write(self: Self, data: bytes) -> None:
        """Записать значение."""
        Uint32Writer(self.buffer).write(len(data))
        if len(data) > 0:
            self.buffer.write(data)
