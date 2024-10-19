from dataclasses import dataclass
from io import BufferedWriter
from typing import Self

from lsmtree.domain.services.interfaces.writer import Writer as Interface


@dataclass
class Writer(Interface[bool]):
    """Оператор записи."""

    buffer: BufferedWriter

    def write(self: Self, data: bool) -> None:  # noqa: FBT001
        """Записать значение."""
        self.buffer.write(int(data).to_bytes(1))
