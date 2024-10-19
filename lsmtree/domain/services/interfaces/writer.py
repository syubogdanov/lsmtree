from abc import abstractmethod
from dataclasses import dataclass
from io import BufferedWriter
from typing import Generic, Self, TypeVar


T = TypeVar("T")


@dataclass
class Writer(Generic[T]):
    """Оператор записи."""

    buffer: BufferedWriter

    @abstractmethod
    def write(self: Self, data: T) -> None:
        """Записать значение."""
