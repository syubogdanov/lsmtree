from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value
from lsmtree.domain.services.interfaces.bloomfilter import BloomFilter
from lsmtree.domain.services.interfaces.sparse_index import SparseIndex
from lsmtree.domain.services.interfaces.sstable import SortedStringTable as Interface
from lsmtree.infrastructure.adapters.kvreader import KeyValueReader


@dataclass(frozen=True)
class SortedStringTable(Interface):
    """Реализация структуры данных `SSTable`."""

    path: Path

    bloomfilter: BloomFilter
    sparse_index: SparseIndex

    def get(self: Self, key: Key) -> Value | None:
        """Получить значение по ключу."""
        if key not in self:
            detail = "The key does not exist"
            raise KeyError(detail)

        with self.path.open(mode="rb") as buffer:
            offset = self.sparse_index.get(key)
            buffer.seek(offset)

            reader = KeyValueReader(buffer)

            candidate: str | None = None
            value: Value | None = None

            while candidate != key:
                candidate, value = reader.read()

        return value

    def __contains__(self: Self, key: Key) -> bool:
        """Проверить наличие ключа."""
        if not self.bloomfilter.test(key):
            return False

        with self.path.open(mode="rb") as buffer:
            offset = self.sparse_index.get(key)
            buffer.seek(offset)

            reader = KeyValueReader(buffer)
            candidate: str | None = None

            while candidate < key and reader.has_next():
                candidate, _ = reader.read()

        if reader.is_broken():
            with self.path.open(mode="ab") as buffer:
                buffer.truncate(reader.offset)
                buffer.flush()

        return key == candidate

    def __iter__(self: Self) -> Iterator[tuple[Key, Value | None]]:
        """Получить итератор по упорядоченным парам."""
        with self.path.open(mode="rb") as buffer:
            reader = KeyValueReader(buffer)

            while reader.has_next():
                key, value = reader.read()
                yield key, value

        if reader.is_broken():
            with self.path.open(mode="ab") as buffer:
                buffer.truncate(reader.offset)
                buffer.flush()
