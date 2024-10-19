from collections.abc import Iterator
from dataclasses import dataclass, field
from sys import getsizeof
from typing import Self

from sortedcontainers import SortedDict

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.services.interfaces.memtable import MemTable as Interface
from lsmtree.domain.services.interfaces.wal import WriteAheadLog
from lsmtree.utils.typing import NonNegativeInt


@dataclass
class MemTable(Interface):
    """Реализация стуктуры данных `MemTable`."""

    wal: WriteAheadLog

    _sorted_dict: SortedDict[Bytes32, Bytes32 | None] = field(default_factory=SortedDict)

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        for key, value in self.wal:
            self._sorted_dict[key] = value

    def put(self: Self, key: Bytes32, value: Bytes32 | None) -> None:
        """Записать значение по ключу."""
        self.wal.write(key, value)
        self._sorted_dict[key] = value

    def get(self: Self, key: Bytes32) -> Bytes32 | None:
        """Получить значение по ключу."""
        return self._sorted_dict[key]

    def clear(self: Self) -> None:
        """Очистить таблицу."""
        self._sorted_dict.clear()
        self.wal.clear()

    @property
    def size(self: Self) -> NonNegativeInt:
        """Получить размер таблицы в байтах."""
        return getsizeof(self)

    def __contains__(self: Self, key: Bytes32) -> bool:
        """Проверить наличие ключа."""
        return key in self._sorted_dict

    def __iter__(self: Self) -> Iterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по упорядоченным парам."""
        return iter(self._sorted_dict.items())
