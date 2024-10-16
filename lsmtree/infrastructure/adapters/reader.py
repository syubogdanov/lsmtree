from dataclasses import dataclass
from io import BufferedReader
from typing import Self

from lsmtree.domain.entities.key import Key
from lsmtree.domain.entities.value import Value
from lsmtree.utils.size import BYTES_UINT32
from lsmtree.utils.typing import PositiveInt, Uint32, UnsignedInt


@dataclass
class Reader:
    """Оператор чтения ключей и значений."""

    buffer: BufferedReader

    _offset: UnsignedInt = 0
    _offset_debt: UnsignedInt = 0

    _key: Key | None = None
    _value: Value | None = None

    _is_fetched: bool = False
    _has_errors: bool = False

    def read(self: Self) -> tuple[Key, Value | None]:
        """Считать пару 'ключ-значение'."""
        if not self._is_fetched:
            self._fetch()

        if self._has_errors:
            detail = "The buffer is broken"
            raise RuntimeError(detail)

        if self._key is None:
            detail = "The buffer is empty"
            raise RuntimeError(detail)

        key, value = self._key, self._value

        self._offset += self._offset_debt
        self._offset_debt = 0

        self._key = None
        self._value = None

        self._is_fetched = False

        return (key, value)

    def has_next(self: Self) -> bool:
        """Проверить, есть ли следующее значение."""
        if not self._is_fetched:
            self._fetch()

        return not self._has_errors and self._key is not None

    def is_broken(self: Self) -> bool:
        """Проверить, сломан ли буффер."""
        if not self._is_fetched:
            self._fetch()

        return self._has_errors

    @property
    def offset(self: Self) -> UnsignedInt:
        """Получить смещение относительно начала буффера."""
        return self._offset

    def _fetch(self: Self) -> None:
        """Подтянуть следуюущую пару."""
        self._is_fetched = True

        key, is_empty_or_broken = self._read_key()
        if is_empty_or_broken:
            return

        value, is_empty_or_broken = self._read_value()
        if is_empty_or_broken:
            return

        self._key = key
        self._value = value

    def _read_key(self: Self) -> tuple[Key, bool]:
        """Вычитать ключ."""
        length, is_empty_or_broken = self._read_uint32()
        if is_empty_or_broken:
            return (Key(), True)

        return self._read_chunk(length)

    def _read_value(self: Self) -> tuple[Value | None, bool]:
        """Вычитать значение."""
        is_tombstone, is_empty_or_broken = self._read_bool()
        if is_empty_or_broken:
            return (None, True)

        if is_tombstone:
            return (None, False)

        length, is_empty_or_broken = self._read_uint32()
        if is_empty_or_broken:
            return (None, True)

        return self._read_chunk(length)

    def _read_chunk(self: Self, bytes_: PositiveInt) -> tuple[bytes, bool]:
        """Вычитать чанк."""
        chunk = self.buffer.read(bytes_)
        self._offset_debt += len(chunk)

        is_empty = len(chunk) == 0
        is_broken = not is_empty and len(chunk) != bytes_

        if is_broken:
            self._has_errors = True

        return (chunk, is_empty or is_broken)

    def _read_uint32(self: Self) -> tuple[Uint32, bool]:
        """Вычитать 32-битное число."""
        chunk, is_empty_or_broken = self._read_chunk(BYTES_UINT32)
        if is_empty_or_broken:
            return (Uint32(), True)

        length = Uint32.from_bytes(chunk)
        return (length, False)

    def _read_bool(self: Self) -> tuple[bool, bool]:
        """Вычитать булево значение."""
        chunk, is_empty_or_broken = self._read_chunk(1)
        if is_empty_or_broken:
            return (False, True)

        flg = bool(int.from_bytes(chunk))
        return (flg, False)
