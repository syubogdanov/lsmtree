from collections.abc import Generator, Iterable
from typing import TypeVar

from lsmtree.utils.typing import NonNegativeInt


T = TypeVar("T")


def distanced(iterable: Iterable[T], distance: NonNegativeInt) -> Generator[T, None, None]:
    """Итерирование по объектам, между которыми соблюдено расстояние."""
    for serial, item in enumerate(iterable):
        if serial % (distance + 1) == 0:
            yield item
