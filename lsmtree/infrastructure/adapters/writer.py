from dataclasses import dataclass
from io import BufferedWriter
from typing import Self

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value
from lsmtree.utils.size import BYTES_UINT32


@dataclass(frozen=True)
class Writer:
    """Оператор записи ключей и значений."""

    buffer: BufferedWriter

    def write(self: Self, key: Key, value: Value | None) -> None:
        """Записать значение по ключу."""
        self.buffer.write(len(key).to_bytes(BYTES_UINT32))
        self.buffer.write(key)

        tombstone_flag = int(value is None)
        self.buffer.write(tombstone_flag.to_bytes(1))

        if value is not None:
            self.buffer.write(len(value).to_bytes(BYTES_UINT32))
            self.buffer.write(value)
