from dataclasses import dataclass
from io import BufferedReader
from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.services.interfaces.reader import Reader as Interface
from lsmtree.infrastructure.adapters.readers.boolean import Reader as BooleanReader
from lsmtree.infrastructure.adapters.readers.bytes32 import Reader as Bytes32Reader


@dataclass
class Reader(Interface[tuple[Bytes32, Bytes32 | None]]):
    """Оператор чтения."""

    buffer: BufferedReader

    _storage: tuple[Bytes32, Bytes32 | None] | None = None

    _is_broken_flag: bool = False

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self._bytes32_reader = Bytes32Reader(self.buffer)
        self._boolean_reader = BooleanReader(self.buffer)

    def read(self: Self) -> tuple[Bytes32, Bytes32 | None]:
        """Считать следующее значение.

        Примечания:
            * Добавляет смещение.
        """
        if self._storage is None:
            self._fetch()

        if self._storage is None:
            detail = "The next 'key-value' pair does not exist"
            raise RuntimeError(detail)

        key, value = self._storage
        self._storage = None

        return (key, value)

    def has_next(self: Self) -> bool:
        """Проверить, есть ли следующее значение.

        Примечания:
            * Возможно, добавляет смещение.
        """
        if self._storage is None:
            self._fetch()

        return self._storage is not None

    def is_broken(self: Self) -> bool:
        """Проверить, сломан ли буффер чтения."""
        return self._is_broken_flag

    @property
    def offset(self: Self) -> Uint1024:
        """Получить смещение относительно начала буффера."""
        return max(self._bytes32_reader.offset, self._boolean_reader.offset)

    def _fetch(self: Self) -> None:
        """Подтянуть данные из буффера."""
        if self.buffer.closed:
            return

        if self._is_broken_flag:
            return

        if not self._bytes32_reader.has_next():
            return

        key = self._bytes32_reader.read()

        if not self._boolean_reader.has_next():
            self._is_broken_flag = True
            return

        tombstone = self._boolean_reader.read()

        if tombstone:
            self._storage = (key, None)
            return

        if not self._bytes32_reader.has_next():
            self._is_broken_flag = True
            return

        value = self._bytes32_reader.read()
        self._storage = (key, value)
