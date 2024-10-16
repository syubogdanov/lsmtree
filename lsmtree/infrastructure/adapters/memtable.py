from collections.abc import Iterator
from dataclasses import dataclass, field
from sys import getsizeof
from typing import Self

from sortedcontainers import SortedDict

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value
from lsmtree.domain.services.interfaces.memtable import MemTable as Interface
from lsmtree.utils.typing import UnsignedInt


@dataclass(frozen=True)
class MemTable(Interface):
    """Реализация стуктуры данных `MemTable`."""

    _sorted_dict: SortedDict[Key, Value | None] = field(default_factory=SortedDict)

    def put(self: Self, key: Key, value: Value | None) -> bool:
        """Записать значение по ключу."""
        self._sorted_dict[key] = value

    def get(self: Self, key: Key) -> Value | None:
        """Получить значение по ключу."""
        return self._sorted_dict[key]

    def clear(self: Self) -> None:
        """Очистить таблицу."""
        self._sorted_dict.clear()

    @property
    def size(self: Self) -> UnsignedInt:
        """Получить размер хранимых пар в байтах."""
        return sum(getsizeof(key) + getsizeof(value) for key, value in self._sorted_dict.items())

    def __contains__(self: Self, key: Key) -> bool:
        """Проверить наличие ключа."""
        return key in self._sorted_dict

    def __iter__(self: Self) -> Iterator[tuple[Key, Value | None]]:
        """Получить итератор по упорядоченным парам."""
        return iter(self._sorted_dict.items())

    def __len__(self: Self) -> int:
        """Получить количество хранимых пар."""
        return len(self._sorted_dict)
