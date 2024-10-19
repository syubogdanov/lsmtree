from abc import abstractmethod
from dataclasses import dataclass
from io import BufferedReader
from typing import Generic, Self, TypeVar

from lsmtree.domain.dtypes.uint1024 import Uint1024


T = TypeVar("T")


@dataclass
class Reader(Generic[T]):
    """Оператор чтения."""

    buffer: BufferedReader

    @abstractmethod
    def read(self: Self) -> T:
        """Считать следующее значение.

        Примечания:
            * Добавляет смещение.
        """

    @abstractmethod
    def has_next(self: Self) -> bool:
        """Проверить, есть ли следующее значение.

        Примечания:
            * Возможно, добавляет смещение.
        """

    @abstractmethod
    def is_broken(self: Self) -> bool:
        """Проверить, сломан ли буффер чтения."""

    @property
    @abstractmethod
    def offset(self: Self) -> Uint1024:
        """Получить смещение относительно начала буффера."""
