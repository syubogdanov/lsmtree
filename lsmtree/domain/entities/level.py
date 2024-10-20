from dataclasses import dataclass
from functools import cached_property
from typing import Self

from lsmtree.utils.typing import DirectoryPath, FilePath, PositiveInt


@dataclass
class Level:
    """Сущность уровня."""

    path: DirectoryPath

    def __post_init__(self: Self) -> None:
        """Дополнительная инициализация объекта."""
        self.path.mkdir(parents=True, exist_ok=True)

    @cached_property
    def serial(self: Self) -> PositiveInt:
        """Получить номер уровня."""
        return int(self.path.name)

    @cached_property
    def next(self: Self) -> "Level":
        """Получить следующий уровень."""
        path = self.path.parent / str(self.serial + 1)
        return Level(path)

    @cached_property
    def sstable(self: Self) -> FilePath:
        """Путь до `SSTable`."""
        return self.path / "sstable.db"

    @cached_property
    def bloom_filter(self: Self) -> FilePath:
        """Путь до фильтра Блума."""
        return self.path / "bloom-filter.db"

    @cached_property
    def sparse_index(self: Self) -> FilePath:
        """Путь до разреженного индекса."""
        return self.path / "sparse-index.db"

    @cached_property
    def sandbox(self: Self) -> FilePath:
        """Путь до песочной `SSTable`."""
        return self.path / "sandbox.db"

    @cached_property
    def sandbox_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия песочной `SSTable`."""
        return self.path / "sandbox-ack.db"

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

    @cached_property
    def bloom_filter_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия фильтру Блума."""
        return self.path / "bloom-filter-ack.db"

    def trust_bloom_filter(self: Self) -> None:
        """Доверять фильтру Блума."""
        self.bloom_filter_trust_label.touch(exist_ok=True)

    def untrust_bloom_filter(self: Self) -> None:
        """Перестать доверять фильтру Блума."""
        self.bloom_filter_trust_label.unlink(missing_ok=True)
        self.bloom_filter.unlink(missing_ok=True)

    def has_trusted_bloom_filter(self: Self) -> bool:
        """Проверить, можно ли доверять фильтру Блума."""
        return self.bloom_filter_trust_label.exists()

    @cached_property
    def sparse_index_trust_label(self: Self) -> FilePath:
        """Путь до метки доверия разреженному индексу."""
        return self.path / "sparse-index-ack.db"

    def trust_sparse_index(self: Self) -> None:
        """Доверять разреженному индексу."""
        self.sparse_index_trust_label.touch(exist_ok=True)

    def untrust_sparse_index(self: Self) -> None:
        """Перестать доверять разреженному индексу."""
        self.sparse_index_trust_label.unlink(missing_ok=True)
        self.sparse_index.unlink(missing_ok=True)

    def has_trusted_sparse_index(self: Self) -> bool:
        """Проверить, можно ли доверять разреженному индексу."""
        return self.sparse_index_trust_label.exists()

    def is_empty(self: Self) -> bool:
        """Проверить, существует ли `SSTable` на данном уровне."""
        return not self.sstable.exists()

    @cached_property
    def merge_label(self: Self) -> FilePath:
        """Флаг смержденного уровня."""
        return self.path / "merged-ack.db"

    def is_merged(self: Self) -> bool:
        """Проверить, смерджен ли уровень."""
        return self.merge_label.exists()

    def mark_as_merged(self: Self) -> None:
        """Обозначить уровень, как смердженный."""
        return self.merge_label.touch(exist_ok=True)

    def clear(self: Self) -> None:
        """Очистить уровень от данных, если возможно."""
        if self.is_merged():
            self.untrust_bloom_filter()
            self.untrust_sparse_index()
            self.untrust_sandbox()
            self.sstable.unlink(missing_ok=True)
            self.merge_label.unlink(missing_ok=True)
