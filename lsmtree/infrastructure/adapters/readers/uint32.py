from dataclasses import dataclass
from io import BufferedReader
from typing import Self

from lsmtree.domain.dtypes.uint32 import Uint32
from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.services.interfaces.reader import Reader as Interface
from lsmtree.utils.typing import NonNegativeInt


@dataclass
class Reader(Interface[Uint32]):
    """Оператор чтения."""

    buffer: BufferedReader

    _offset_value: NonNegativeInt = 0
    _offset_debt: NonNegativeInt = 0

    _storage: Uint32 | None = None

    _is_broken_flag: bool = False

    def read(self: Self) -> Uint32:
        """Считать следующее значение.

        Примечания:
            * Добавляет смещение.
        """
        if self._storage is None:
            self._fetch()

        if self._storage is None:
            detail = "The next 'Uint32' does not exist"
            raise RuntimeError(detail)

        uint32 = self._storage
        self._storage = None

        self._offset_value += self._offset_debt
        self._offset_debt = 0

        return uint32

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

        chunk = self.buffer.read(Uint32.bytes)
        self._offset_debt += len(chunk)

        if len(chunk) > 0 and len(chunk) != Uint32.bytes:
            self._is_broken_flag = True
            return

        if chunk:
            integer = int.from_bytes(chunk)
            self._storage = Uint32(integer)
