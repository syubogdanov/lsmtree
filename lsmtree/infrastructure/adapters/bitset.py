from dataclasses import dataclass
from typing import Self

from lsmtree.utils.typing import NonNegativeInt


@dataclass
class BitSet:
    """Бит-множество."""

    mask: int = 0

    def test(self: Self, bit: NonNegativeInt) -> bool:
        """Проверить, равен ли бит единице."""
        bitmask = (1 << bit)
        return (self.mask & bitmask) != 0

    def flip(self: Self, bit: NonNegativeInt) -> None:
        """Инвертировать бит."""
        bitmask = (1 << bit)
        self.mask ^= bitmask
