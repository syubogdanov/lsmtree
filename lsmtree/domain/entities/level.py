from dataclasses import dataclass
from typing import Self

from lsmtree.utils.typing import DirectoryPath, FilePath


@dataclass
class Level:
    """Сущность уровня."""

    path: DirectoryPath

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
    def bloom_filter_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия фильтру Блума."""
        return self.path / "bloom-filter-ack.db"

    @property
    def sparse_index_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия разреженному индексу."""
        return self.path / "sparse-index-ack.db"

    def trust_bloom_filter(self: Self) -> None:
        """Доверять фильтру Блума."""
        self.bloom_filter_trust_label.touch(exist_ok=True)

    def trust_sparse_index(self: Self) -> None:
        """Доверять разреженному индексу."""
        self.sparse_index_trust_label.touch(exist_ok=True)

    def has_trusted_bloom_filter(self: Self) -> bool:
        """Проверить, можно ли доверять фильтру Блума."""
        return self.bloom_filter_trust_label.exists()

    def has_trusted_sparse_index(self: Self) -> bool:
        """Проверить, можно ли доверять разреженному индексу."""
        return self.sparse_index_trust_label.exists()
