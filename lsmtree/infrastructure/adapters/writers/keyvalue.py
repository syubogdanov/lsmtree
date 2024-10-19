from dataclasses import dataclass
from io import BufferedWriter
from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.services.interfaces.writer import Writer as Interface
from lsmtree.infrastructure.adapters.writers.boolean import Writer as BooleanWriter
from lsmtree.infrastructure.adapters.writers.bytes32 import Writer as Bytes32Writer


@dataclass
class Writer(Interface[tuple[Bytes32, Bytes32 | None]]):
    """Оператор записи."""

    buffer: BufferedWriter

    def write(self: Self, data: tuple[Bytes32, Bytes32 | None]) -> None:
        """Записать значение."""
        key, value = data

        Bytes32Writer(self.buffer).write(key)
        BooleanWriter(self.buffer).write(value is None)

        if value is not None:
            Bytes32Writer(self.buffer).write(value)
