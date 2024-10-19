from dataclasses import dataclass
from typing import ClassVar, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.entities.level import Level
from lsmtree.domain.services.interfaces.bloomfilter import BloomFilter as BloomFilterInterface
from lsmtree.domain.services.interfaces.sparse_index import SparseIndex as SparseIndexInterface
from lsmtree.domain.services.interfaces.sstable import SortedStringTable as Interface
from lsmtree.infrastructure.adapters.bloomfilter import BloomFilter
from lsmtree.infrastructure.adapters.readers.keyvalue import Reader
from lsmtree.infrastructure.adapters.sparse_index import SparseIndex
from lsmtree.infrastructure.adapters.writers.keyvalue import Writer
from lsmtree.utils.typing import PositiveInt, SortedIterable, SortedIterator


@dataclass
class SortedStringTable(Interface):
    """Интерфейс структуры данных `SSTable`."""

    level: Level

    _bloom_filter: BloomFilterInterface | None = None
    _sparse_index: SparseIndexInterface | None = None

    # Фильтра Блума требует указания числа хэшей
    _number_of_hashes: ClassVar[PositiveInt] = 3

    # Индекс требует расстояние между ключами
    _distance: ClassVar[PositiveInt] = 32

    def get(self: Self, key: Bytes32) -> Bytes32 | None:
        """Получить значение по ключу."""
        self._guarantee_efficiency()

        if self._bloom_filter is None or self._sparse_index is None:
            detail = "Failed to guarantee an efficient access"
            raise RuntimeError(detail)

        if not self._bloom_filter.test(key):
            detail = f"The key {key!r} does not exist"
            raise KeyError(detail)

        with self.level.sstable.open(mode="rb") as buffer:
            offset = self._sparse_index.get(key)
            buffer.seek(offset)

            reader = Reader(buffer)

            candidate: Bytes32 | None = None
            value: Bytes32 | None = None

            while (candidate is None or candidate < key) and reader.has_next():
                candidate, value = reader.read()

        if key != candidate:
            detail = f"The key {key!r} does not exist"
            raise KeyError(detail)

        return value

    def from_iterable(self: Self, iterable: SortedIterable[tuple[Bytes32, Bytes32 | None]]) -> None:
        """Построить песочную `SSTable`."""
        self.level.untrust_sandbox()

        with self.level.sandbox.open(mode="wb") as buffer:
            writer = Writer(buffer)

            for key, value in iterable:
                pair = (key, value)
                writer.write(pair)

        self.level.trust_sandbox()

    def __contains__(self: Self, key: Bytes32) -> bool:
        """Проверить наличие ключа."""
        try:
            self.get(key)
        except KeyError:
            return False
        else:
            return True

    def __iter__(self: Self) -> SortedIterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по таблице."""
        with self.level.sstable.open(mode="rb") as buffer:
            reader = Reader(buffer)

            while reader.has_next():
                key, value = reader.read()
                yield (key, value)

    def _guarantee_efficiency(self: Self) -> None:
        """Прогрузить структуры данных, требующиеся для эффективности."""
        if self._bloom_filter is None:
            self._build_bloom_filter()

        if self._sparse_index is None:
            self._build_sparse_index()

    def _build_bloom_filter(self: Self) -> None:
        """Построить фильтр Блума."""
        if self.level.has_trusted_bloom_filter():
            with self.level.bloom_filter.open(mode="rb") as buffer:
                self._bloom_filter = BloomFilter.load(buffer)
                return

        self._bloom_filter = BloomFilter(self._number_of_hashes)

        for key, _ in self:
            self._bloom_filter.add(key)

        with self.level.bloom_filter.open(mode="wb") as buffer:
            self._bloom_filter.dump(buffer)

        self.level.trust_bloom_filter()

    def _get_offset_mapping_iterator(self: Self) -> SortedIterator[tuple[Bytes32, Uint1024]]:
        """Получить итератор по парам 'ключ-смещение'."""
        with self.level.sstable.open(mode="rb") as buffer:
            reader = Reader(buffer)
            offset = Uint1024(0)

            while reader.has_next():
                key, _ = reader.read()
                yield (key, offset)

                offset = reader.offset

    def _build_sparse_index(self: Self) -> None:
        """Построить разреженный индекс."""
        self._sparse_index = SparseIndex(self.level.sparse_index)

        if self.level.has_trusted_sparse_index():
            return

        iterable = self._get_offset_mapping_iterator()
        self._sparse_index.from_iterable(iterable, self._distance)

        self.level.trust_sparse_index()
