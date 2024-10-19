from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, TypeAlias


@dataclass
class IntCompare:
    """Сравнение целых чисел."""

    ge: int | None = None
    gt: int | None = None


NonNegativeInt = Annotated[int, IntCompare(ge=0)]
PositiveInt = Annotated[int, IntCompare(gt=0)]

SortedIterable: TypeAlias = Iterable
SortedIterator: TypeAlias = Iterator

FilePath: TypeAlias = Path
DirectoryPath: TypeAlias = Path
