from dataclasses import dataclass
from typing import Self


@dataclass
class BitSet:
    """Бит-множество."""

    mask: int = 0

    def test(self: Self, bit: int) -> bool:
        """Проверить, равен ли бит единице."""
        bitmask = (1 << bit)
        return (self.mask & bitmask) != 0

    def flip(self: Self, bit: int) -> None:
        """Инвертировать бит."""
        bitmask = (1 << bit)
        self.mask ^= bitmask
