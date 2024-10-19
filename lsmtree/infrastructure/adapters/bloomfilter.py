import hashlib
import pickle

from collections.abc import Iterator
from dataclasses import dataclass, field
from io import BufferedReader, BufferedWriter
from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.dtypes.uint32 import Uint32
from lsmtree.domain.services.interfaces.bloomfilter import BloomFilter as Interface
from lsmtree.infrastructure.adapters.bitset import BitSet
from lsmtree.utils.typing import PositiveInt


@dataclass
class BloomFilter(Interface):
    """Реализация фильтра Блума."""

    number_of_hashes: PositiveInt

    _bitset: BitSet = field(default_factory=BitSet)

    def test(self: Self, data: Bytes32) -> bool:
        """Проверить, может ли объект быть в множестве."""
        return all(
            self._bitset.test(self._hash_to_bit(self._hash(data, seed)))
            for seed in self._get_seed_iterator()
        )

    def add(self: Self, data: Bytes32) -> None:
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

    def _get_seed_iterator(self: Self) -> Iterator[int]:
        """Получить итератор по сидам."""
        return iter(range(self.number_of_hashes))

    @staticmethod
    def _hash(data: Bytes32, seed: int) -> int:
        """Получить хэш объекта."""
        md5 = hashlib.md5(data, usedforsecurity=False)
        return int(md5.hexdigest(), base=16) ^ seed

    @staticmethod
    def _hash_to_bit(hash_: int) -> int:
        """Отобразить хэш в бит."""
        return hash_ % (Uint32.max + 1)
