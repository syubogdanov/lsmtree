from dataclasses import dataclass
from typing import Annotated

from lsmtree.utils.limits import MAX_UINT32, MAX_UINT512


@dataclass
class IntCompare:
    """Сравнение целых чисел."""

    le: int | None = None
    ge: int | None = None
    lt: int | None = None
    gt: int | None = None


@dataclass
class Length:
    """Длина объекта."""

    min: int | None = None
    max: int | None = None


UnsignedInt = Annotated[int, IntCompare(ge=0)]
PositiveInt = Annotated[int, IntCompare(gt=0)]

Uint32 = Annotated[UnsignedInt, IntCompare(le=MAX_UINT32)]
Uint512 = Annotated[UnsignedInt, IntCompare(le=MAX_UINT512)]
