from collections.abc import Iterable
from dataclasses import dataclass
from io import BufferedReader, BufferedWriter
from pathlib import Path
from typing import Self

from lsmtree.domain.entities.key import Key
from lsmtree.utils.itertools import distanced
from lsmtree.utils.size import BYTES_UINT32, BYTES_UINT512
from lsmtree.utils.typing import PositiveInt, Uint32, Uint512, UnsignedInt


@dataclass(frozen=True)
class SparseIndex:
    """Разреженный индекс смещений."""

    path: Path
    distance: UnsignedInt

    def get(self: Self, key: Key) -> Uint512:
        """Получить ближайшее к ключу смещение."""
        offset: Uint512 = 0

        with self.path.open(mode="rb") as buffer:
            reader = SparseIndexReader(buffer)

            while reader.has_next():
                next_key, next_offset = reader.read()

                if key < next_key:
                    break

                offset = next_offset

        if reader.is_broken():
            with self.path.open(mode="ab") as buffer:
                buffer.truncate(reader.offset)
                buffer.flush()

        return offset

    def build(self: Self, iterable: Iterable[tuple[Key, Uint512]]) -> "SparseIndex":
        """Сформировать индекс по итерируемому объекту.

        Примечания:
            * Итерируемый объект должен быть отсортирован по ключам.
        """
        with self.path.open(mode="wb") as buffer:
            writer = SparseIndexWriter(buffer)

            for key, offset in distanced(iterable, self.distance):
                writer.write(key, offset)
                buffer.flush()


@dataclass
class SparseIndexReader:
    """Оператор чтения ключей и значений."""

    buffer: BufferedReader

    _offset: UnsignedInt = 0
    _offset_debt: UnsignedInt = 0

    _key: Key | None = None
    _value: Uint512 | None = None

    _is_fetched: bool = False
    _has_errors: bool = False

    def read(self: Self) -> tuple[Key, Uint512]:
        """Считать пару 'ключ-смещение'."""
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

        if self.buffer.closed:
            return

        key, is_empty_or_broken = self._read_key()
        if is_empty_or_broken:
            return

        value, is_empty_or_broken = self._read_uint512()
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

        value = Uint32.from_bytes(chunk)
        return (value, False)

    def _read_uint512(self: Self) -> tuple[Uint512, bool]:
        """Вычитать 512-битное число."""
        chunk, is_empty_or_broken = self._read_chunk(BYTES_UINT512)
        if is_empty_or_broken:
            return (Uint512(), True)

        value = Uint512.from_bytes(chunk)
        return (value, False)


@dataclass(frozen=True)
class SparseIndexWriter:
    """Оператор записи ключей и смещений."""

    buffer: BufferedWriter

    def write(self: Self, key: Key, offset: Uint512) -> None:
        """Записать ключ и смещение."""
        self.buffer.write(len(key).to_bytes(BYTES_UINT32))
        self.buffer.write(key)
        self.buffer.write(offset.to_bytes(BYTES_UINT512))
