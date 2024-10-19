from dataclasses import dataclass
from typing import Self

from lsmtree.utils.typing import DirectoryPath, FilePath


@dataclass
class Level:
    """Сущность уровня."""

    path: DirectoryPath

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self.path.mkdir(parents=True, exist_ok=True)

    @property
    def sstable(self: Self) -> FilePath:
        """Путь до `SSTable`."""
        return self.path / "sstable.db"

    @property
    def bloom_filter(self: Self) -> FilePath:
        """Путь до фильтра Блума."""
        return self.path / "bloom-filter.db"

    @property
    def sparse_index(self: Self) -> FilePath:
        """Путь до разреженного индекса."""
        return self.path / "sparse-index.db"

    @property
    def sandbox(self: Self) -> FilePath:
        """Путь до песочной `SSTable`."""
        return self.path / "sandbox.db"

    @property
    def sandbox_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия песочной `SSTable`."""
        return self.path / "sstable-ack.db"

    def trust_sandbox(self: Self) -> None:
        """Доверять песочной `SSTable`."""
        self.sandbox_trust_label.touch(exist_ok=True)

    def untrust_sandbox(self: Self) -> None:
        """Перестать доверять песочной `SSTable`."""
        self.sandbox_trust_label.unlink(missing_ok=True)
        self.sandbox.unlink(missing_ok=True)

    def has_trusted_sandbox(self: Self) -> bool:
        """Проверить, можно ли доверять песочной `SSTable`."""
        return self.sandbox_trust_label.exists()

    @property
    def bloom_filter_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия фильтру Блума."""
        return self.path / "bloom-filter-ack.db"

    def trust_bloom_filter(self: Self) -> None:
        """Доверять фильтру Блума."""
        self.bloom_filter_trust_label.touch(exist_ok=True)

    def has_trusted_bloom_filter(self: Self) -> bool:
        """Проверить, можно ли доверять фильтру Блума."""
        return self.bloom_filter_trust_label.exists()

    @property
    def sparse_index_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия разреженному индексу."""
        return self.path / "sparse-index-ack.db"

    def trust_sparse_index(self: Self) -> None:
        """Доверять разреженному индексу."""
        self.sparse_index_trust_label.touch(exist_ok=True)

    def has_trusted_sparse_index(self: Self) -> bool:
        """Проверить, можно ли доверять разреженному индексу."""
        return self.sparse_index_trust_label.exists()
