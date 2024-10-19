from dataclasses import dataclass
from io import BufferedReader
from typing import Self

from lsmtree.domain.dtypes.uint1024 import Uint1024
from lsmtree.domain.services.interfaces.reader import Reader as Interface
from lsmtree.utils.typing import NonNegativeInt


@dataclass
class Reader(Interface[bool]):
    """Оператор чтения."""

    buffer: BufferedReader

    _offset_value: NonNegativeInt = 0
    _offset_debt: NonNegativeInt = 0

    _storage: bool | None = None

    def read(self: Self) -> bool:
        """Считать следующее значение.

        Примечания:
            * Добавляет смещение.
        """
        if self._storage is None:
            self._fetch()

        if self._storage is None:
            detail = "The next 'bool' does not exist"
            raise RuntimeError(detail)

        boolean = self._storage
        self._storage = None

        self._offset_value += self._offset_debt
        self._offset_debt = 0

        return boolean

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
        return False

    @property
    def offset(self: Self) -> Uint1024:
        """Получить смещение относительно начала буффера."""
        return Uint1024(self._offset_value)

    def _fetch(self: Self) -> None:
        """Подтянуть данные из буффера."""
        if self.buffer.closed:
            return

        chunk = self.buffer.read(1)
        self._offset_debt += len(chunk)

        if chunk:
            boolean = bool(int.from_bytes(chunk))
            self._storage = boolean
