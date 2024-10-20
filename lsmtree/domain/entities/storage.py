from dataclasses import dataclass
from functools import cached_property
from typing import Self

from lsmtree.domain.entities.level import Level
from lsmtree.utils.typing import DirectoryPath, FilePath, PositiveInt, SortedIterator


@dataclass
class Storage:
    """Сущность хранилища."""

    root: DirectoryPath

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self.root.mkdir(parents=True, exist_ok=True)

    @cached_property
    def wal(self: Self) -> FilePath:
        """Путь до журнала предзаписи."""
        return self.root / "wal.db"

    @cached_property
    def levels(self: Self) -> DirectoryPath:
        """Путь до директории уровней."""
        return self.root / "levels"

    def get_level(self: Self, serial: PositiveInt) -> Level:
        """Получить сущность уровня."""
        path = self.levels / str(serial)
        return Level(path)

    def get_first_level(self: Self) -> Level:
        """Получить сущность первого уровня."""
        return self.get_level(serial=1)

    def __iter__(self: Self) -> SortedIterator[Level]:
        """Получить итератор по заполненным уровням."""
        for path in sorted(self.levels.glob("*")):
            level = Level(path)
            if not level.is_empty():
                yield level
