from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value
from lsmtree.domain.services.interfaces.wal import WriteAheadLog as Interface
from lsmtree.infrastructure.adapters.kvreader import KeyValueReader
from lsmtree.infrastructure.adapters.kvwriter import KeyValueWriter


@dataclass(frozen=True)
class WriteAheadLog(Interface):
    """Реалиация журнала предзаписи."""

    path: Path

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self._descriptor = self.path.open(mode="ab")

    def write(self: Self, key: Key, value: Value | None) -> None:
        """Записать значение по ключу."""
        writer = KeyValueWriter(self._descriptor)
        writer.write(key, value)
        self._descriptor.flush()

    def clear(self: Self) -> None:
        """Очистить журнал предзаписи."""
        self._descriptor.close()
        self._descriptor = self.path.open(mode="wb")

    def __iter__(self: Self) -> Iterator[tuple[Key, Value | None]]:
        """Получить итератор по журналу."""
        with self.path.open(mode="rb") as buffer:
            reader = KeyValueReader(buffer)

            while reader.has_next():
                key, value = reader.read()
                yield key, value

        if reader.is_broken():
            self._descriptor.truncate(reader.offset)
            self._descriptor.flush()
