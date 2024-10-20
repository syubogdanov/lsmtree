from collections.abc import Generator, Iterable
from itertools import islice
from typing import TypeVar

from lsmtree.utils.typing import NonNegativeInt


T = TypeVar("T")


def distanced(iterable: Iterable[T], distance: NonNegativeInt) -> Generator[T, None, None]:
    """Итерирование по объектам, между которыми соблюдено расстояние."""
    for serial, item in enumerate(iterable):
        if serial % (distance + 1) == 0:
            yield item


def batched(iterable: Iterable[T], n: int = 100) -> Generator[tuple[T, ...], None, None]:
    """Пройтись по итерируемому объекту, используя батчи."""
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch
