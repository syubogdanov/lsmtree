from abc import abstractmethod
from collections.abc import Hashable
from io import BufferedReader, BufferedWriter
from typing import Protocol, Self


class BloomFilter(Protocol):
    """Интерфейс фильтра Блума."""

    @abstractmethod
    def test(self: Self, data: Hashable) -> bool:
        """Проверить, может ли объект быть в множестве."""

    @abstractmethod
    def add(self: Self, data: Hashable) -> None:
        """Добавить объект в фильтр Блума."""

    @abstractmethod
    def dump(self: Self, buffer: BufferedWriter) -> None:
        """Сериализовать фильтр Блума."""

    @staticmethod
    @abstractmethod
    def load(buffer: BufferedReader) -> "BloomFilter":
        """Десериализовать фильтр Блума."""
