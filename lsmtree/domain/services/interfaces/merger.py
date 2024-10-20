from abc import abstractmethod
from typing import Protocol, Self

from lsmtree.domain.entities.level import Level


class Merger(Protocol):
    """Интерфейс оператора слияния."""

    @abstractmethod
    def merge(self: Self, level: Level) -> None:
        """Выполнить операцию слияния."""
