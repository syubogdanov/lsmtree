from abc import abstractmethod
from io import BufferedReader, BufferedWriter
from typing import Protocol, Self

from lsmtree.domain.dtypes.bytes32 import Bytes32


class BloomFilter(Protocol):
    """Интерфейс фильтра Блума."""

    @abstractmethod
    def test(self: Self, data: Bytes32) -> bool:
        """Проверить, может ли объект быть в множестве."""

    @abstractmethod
    def add(self: Self, data: Bytes32) -> None:
        """Добавить объект в фильтр Блума."""

    @abstractmethod
    def dump(self: Self, buffer: BufferedWriter) -> None:
        """Сериализовать фильтр Блума."""

    @staticmethod
    @abstractmethod
    def load(buffer: BufferedReader) -> "BloomFilter":
        """Десериализовать фильтр Блума."""
