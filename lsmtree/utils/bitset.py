from dataclasses import dataclass
from typing import Self

from lsmtree.utils.typing import Uint32


@dataclass
class BitSet:
    """Бит-множество."""

    _mask: int = 0

    def test(self: Self, bit: Uint32) -> bool:
        """Проверить, равен ли бит единице."""
        bitmask = (1 << bit)
        return (self._mask & bitmask) != 0

    def flip(self: Self, bit: Uint32) -> None:
        """Инвертировать бит."""
        bitmask = (1 << bit)
        self._mask ^= bitmask

    def __int__(self: Self) -> int:
        """Получить маску бит-множества."""
        return self._mask

    @staticmethod
    def from_int(mask: int) -> "BitSet":
        """Собрать бит-множество по маске."""
        return BitSet(_mask=mask)
