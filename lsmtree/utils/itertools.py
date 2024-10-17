from collections.abc import Generator, Iterable
from typing import TypeVar

from lsmtree.utils.typing import UnsignedInt


T = TypeVar("T")


def distanced(iterable: Iterable[T], distance: UnsignedInt) -> Generator[T, None, None]:
    """Итерироваться по объектам, пропуская `distance` объектов."""
    for serial, item in enumerate(iterable):
        if serial % (distance + 1) == 0:
            yield item
