from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.services.interfaces.wal import WriteAheadLog as Interface
from lsmtree.infrastructure.adapters.readers.keyvalue import Reader
from lsmtree.infrastructure.adapters.writers.keyvalue import Writer


@dataclass
class WriteAheadLog(Interface):
    """Реалиация журнала предзаписи."""

    path: Path

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self._descriptor = self.path.open(mode="ab")

    def write(self: Self, key: Bytes32, value: Bytes32 | None) -> None:
        """Записать значение по ключу."""
        pair = (key, value)
        Writer(self._descriptor).write(pair)
        self._descriptor.flush()

    def clear(self: Self) -> None:
        """Очистить журнал предзаписи."""
        self._descriptor.close()
        self._descriptor = self.path.open(mode="wb")

    def __iter__(self: Self) -> Iterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по журналу."""
        with self.path.open(mode="rb") as buffer:
            reader = Reader(buffer)

            while reader.has_next():
                key, value = reader.read()
                yield key, value

        if reader.is_broken():
            self._descriptor.truncate(reader.offset)
            self._descriptor.flush()
