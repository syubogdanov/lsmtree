import pickle

from collections.abc import Hashable, Iterator
from dataclasses import dataclass, field
from io import BufferedReader, BufferedWriter
from typing import Self

from lsmtree.domain.services.interfaces.bloomfilter import BloomFilter as Interface
from lsmtree.utils.bitset import BitSet
from lsmtree.utils.limits import MAX_UINT32
from lsmtree.utils.typing import Uint32


@dataclass
class BloomFilter(Interface):
    """Реализация фильтра Блума."""

    number_of_hashes: Uint32

    _bitset: BitSet = field(default_factory=BitSet)

    def test(self: Self, data: Hashable) -> bool:
        """Проверить, может ли объект быть в множестве."""
        return all(
            self._bitset.test(self._hash_to_bit(self._hash(data, seed)))
            for seed in self._get_seed_iterator()
        )

    def add(self: Self, data: Hashable) -> None:
        """Добавить объект в фильтр Блума."""
        for seed in self._get_seed_iterator():
            hash_ = self._hash(data, seed)
            bit = self._hash_to_bit(hash_)

            if not self._bitset.test(bit):
                self._bitset.flip(bit)

    def dump(self: Self, buffer: BufferedWriter) -> None:
        """Сериализовать фильтр Блума.

        Примечания:
            * Требует более безопасной и эффективной реализации.
        """
        return pickle.dump(self, buffer)

    @staticmethod
    def load(buffer: BufferedReader) -> "BloomFilter":
        """Десериализовать фильтр Блума.

        Примечания:
            * Требует более безопасной и эффективной реализации.
        """
        return pickle.load(buffer)

    def _get_seed_iterator(self: Self) -> Iterator[Uint32]:
        """Получить итератор по сидам."""
        return iter(range(self.number_of_hashes))

    @staticmethod
    def _hash(data: Hashable, seed: Uint32) -> int:
        """Получить хэш объекта."""
        return hash(data) ^ seed

    @staticmethod
    def _hash_to_bit(hash_: int) -> Uint32:
        """Отобразить хэш в бит."""
        return hash_ % (MAX_UINT32 + 1)
