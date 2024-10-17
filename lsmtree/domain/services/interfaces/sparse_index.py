from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Self

from lsmtree.domain.entities.key import Key
from lsmtree.utils.typing import Uint512


@dataclass(frozen=True)
class SparseIndex:
    """Разреженный индекс смещений."""

    @abstractmethod
    def get(self: Self, key: Key) -> Uint512:
        """Получить ближайшее к ключу смещение."""

    @abstractmethod
    def build(self: Self, iterable: Iterable[tuple[Key, Uint512]]) -> None:
        """Сформировать индекс по итерируемому объекту.

        Примечания:
            * Итерируемый объект должен быть отсортирован по ключам.
        """
