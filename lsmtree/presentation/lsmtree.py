from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.entities.level import Level
from lsmtree.domain.entities.storage import Storage
from lsmtree.infrastructure.adapters.memtable import MemTable
from lsmtree.infrastructure.adapters.merger import Merger
from lsmtree.infrastructure.adapters.sstable import SortedStringTable
from lsmtree.infrastructure.adapters.wal import WriteAheadLog
from lsmtree.utils.typing import NonNegativeInt


@dataclass
class LSMTree:
    """LSM-дерево."""

    root: Path

    # Размер `MemTable`, после которого она станет `SSTable` [в байтах]
    _memtable_threshold: ClassVar[NonNegativeInt] = 64 * 1024  # 64 KiB

    # Количество `SSTable`, модели которых можно прогрузить в RAM
    # Примечание: в оперативной памяти хранятся лишь фильтры Блума
    _number_of_cached_sstables: ClassVar[NonNegativeInt] = 128

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self._storage = Storage(self.root)
        self._wal = WriteAheadLog(self._storage.wal)
        self._memtable = MemTable(self._wal)

        self._sstables = {
            serial: SortedStringTable(self._storage.get_level(serial))
            for serial in range(1, self._number_of_cached_sstables + 1)
        }

        merger = Merger()

        for level in self._storage:
            merger.merge(level)

        if self._memtable.size > self._memtable_threshold:
            self._flush_memtable()

        for level in self._storage:
            merger.merge(level)

    def __setitem__(self: Self, key: bytes, value: bytes) -> None:
        """Установить значение по ключу."""
        if not (Bytes32.min_len <= len(key) <= Bytes32.max_len):
            detail = f"The key {key!r} is too long..."
            raise ValueError(detail)

        if not (Bytes32.min_len <= len(value) <= Bytes32.max_len):
            detail = f"The value {value!r} is too long..."
            raise ValueError(detail)

        key32 = Bytes32(key)
        value32 = Bytes32(value)

        self._memtable.put(key32, value32)
        if self._memtable.size > self._memtable_threshold:
            self._flush_memtable()

    def __delitem__(self: Self, key: bytes) -> None:
        """Удалить значение по ключу."""
        if not (Bytes32.min_len <= len(key) <= Bytes32.max_len):
            detail = f"The key {key!r} is too long..."
            raise ValueError(detail)

        key32 = Bytes32(key)

        self._memtable.put(key32, None)
        if self._memtable.size > self._memtable_threshold:
            self._flush_memtable()

    def __getitem__(self: Self, key: bytes) -> bytes:
        """Получить значение по ключу."""
        if not (Bytes32.min_len <= len(key) <= Bytes32.max_len):
            detail = f"The key {key!r} is too long..."
            raise ValueError(detail)

        key32 = Bytes32(key)

        if key32 in self._memtable:
            value = self._memtable.get(key32)

            if value is not None:
                return value

            detail = f"The key {key!r} does not exist"
            raise KeyError(detail)

        for level in self._storage:
            sstable = self._get_sstable(level)

            if key32 in sstable:
                value = sstable.get(key32)

                if value is not None:
                    return value

                detail = f"The key {key!r} does not exist"
                raise KeyError(detail)

        detail = f"The key {key!r} does not exist"
        raise KeyError(detail)

    def __contains__(self: Self, key: bytes) -> bool:
        """Проверить, есть ли ключ в дереве."""
        if not (Bytes32.min_len <= len(key) <= Bytes32.max_len):
            detail = f"The key '{key!r}' is too long..."
            raise ValueError(detail)

        key32 = Bytes32(key)

        if key32 in self._memtable:
            value = self._memtable.get(key32)
            return value is not None

        for level in self._storage:
            sstable = self._get_sstable(level)
            if key32 in sstable:
                value = sstable.get(key32)
                return value is not None

        return False

    def _get_sstable(self: Self, level: Level) -> SortedStringTable:
        """Получить `SSTable` для данного уровня."""
        if level.serial < len(self._sstables):
            return self._sstables[level.serial]

        level = self._storage.get_level(level.serial)
        return SortedStringTable(level)

    def _flush_memtable(self: Self) -> None:
        """Перенести данные из RAM на диск."""
        level = self._storage.get_first_level()

        sstable = self._get_sstable(level)
        sstable.from_iterable(iter(self._memtable))

        merger = Merger()
        merger.merge(level)

        self._memtable.clear()
