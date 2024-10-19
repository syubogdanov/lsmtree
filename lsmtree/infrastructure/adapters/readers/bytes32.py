from dataclasses import dataclass
from io import BufferedReader
from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.services.interfaces.reader import Reader as Interface
from lsmtree.infrastructure.adapters.readers.uint32 import Reader as Uint32Reader
from lsmtree.utils.typing import NonNegativeInt


@dataclass
class Reader(Interface[Bytes32]):
    """Оператор чтения."""

    buffer: BufferedReader

    _offset_value: NonNegativeInt = 0
    _offset_debt: NonNegativeInt = 0

    _storage: Bytes32 | None = None

    _is_broken_flag: bool = False

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self._uint32_reader = Uint32Reader(self.buffer)

    def read(self: Self) -> Bytes32:
        """Считать следующее значение.

        Примечания:
            * Добавляет смещение.
        """
        if self._storage is None:
            self._fetch()

        if self._storage is None:
            detail = "The next 'Bytes32' does not exist"
            raise RuntimeError(detail)

        bytes32 = self._storage
        self._storage = None

        self._offset_value += self._offset_debt
        self._offset_debt = 0

        return bytes32

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
        return Uint1024(self._offset_value)

    def _fetch(self: Self) -> None:
        """Подтянуть данные из буффера."""
        if self.buffer.closed:
            return

        if self._is_broken_flag:
            return

        if not self._uint32_reader.has_next():
            return

        length = self._uint32_reader.read()

        chunk = self.buffer.read(length)
        self._offset_debt += len(chunk)

        if len(chunk) > 0 and len(chunk) != length:
            self._is_broken_flag = True
            return

        if chunk:
            bytes32 = Bytes32(chunk)
            self._storage = bytes32
