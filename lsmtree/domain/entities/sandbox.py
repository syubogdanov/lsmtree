from dataclasses import dataclass
from typing import Self

from lsmtree.utils.typing import DirectoryPath, FilePath


@dataclass
class Sandbox:
    """Сущность песочницы."""

    path: DirectoryPath

    @property
    def sstable(self: Self) -> FilePath:
        """Путь до песочной `SSTable`."""
        return self.path / "sstable.db"

    @property
    def sstable_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия песочной `SSTable`."""
        return self.path / "sstable-ack.db"

    def trust_sstable(self: Self) -> None:
        """Доверять песочной `SSTable`."""
        self.sstable_trust_label.touch(exist_ok=True)

    def has_trusted_sstable(self: Self) -> bool:
        """Проверить, можно ли доверять песочной `SSTable`."""
        return self.sstable_trust_label.exists()
