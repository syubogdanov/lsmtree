from typing import Self

from lsmtree.domain.dtypes.bytes32 import Bytes32
from lsmtree.domain.entities.level import Level
from lsmtree.domain.services.interfaces.merger import Merger as Interface
from lsmtree.infrastructure.adapters.sstable import SortedStringTable
from lsmtree.utils.typing import SortedIterator


class Merger(Interface):
    """Реализация оператора слияния."""

    def merge(self: Self, level: Level) -> None:
        """Выполнить операцию слияния."""
        if level.is_merged():
            level.clear()

        if not level.has_trusted_sandbox():
            return

        if level.is_empty():
            level.untrust_bloom_filter()
            level.untrust_sparse_index()

            level.sandbox.rename(level.sstable)
            level.untrust_sandbox()

            return

        sstable = SortedStringTable(level.next)
        iterable = self._get_iterator(level)
        sstable.from_iterable(iterable)

        self.merge(level.next)
        level.mark_as_merged()

        level.clear()

    def _get_iterator(self: Self, level: Level) -> SortedIterator[tuple[Bytes32, Bytes32 | None]]:
        """Получить итератор по объединенным `SSTable`."""
        iterator_factory = SortedStringTable(level)

        sandbox_iterator = iterator_factory.over_sandbox()
        sstable_iterator = iter(iterator_factory)

        sandbox_pair: tuple[Bytes32, Bytes32 | None] | None = None
        sstable_pair: tuple[Bytes32, Bytes32 | None] | None = None

        while True:
            if sandbox_pair is None:
                try:
                    sandbox_key, sandbox_value = next(sandbox_iterator)
                    sandbox_pair = sandbox_key, sandbox_value
                except StopIteration:
                    pass

            if sstable_pair is None:
                try:
                    sstable_key, sstable_value = next(sstable_iterator)
                    sstable_pair = sstable_key, sstable_value
                except StopIteration:
                    pass

            if sandbox_pair is None or sstable_pair is None:
                break

            if sandbox_key <= sstable_key:
                yield sandbox_pair
                sandbox_pair = None

            else:
                yield sstable_pair
                sstable_pair = None

        if sandbox_pair is not None:
            yield sandbox_pair
            yield from sandbox_iterator

        if sstable_pair is not None:
            yield sstable_pair
            yield from sstable_iterator
