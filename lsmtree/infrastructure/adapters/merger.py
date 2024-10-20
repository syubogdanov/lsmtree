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
        controller = SortedStringTable(level)

        sandbox_is_done: bool = False
        sstable_is_done: bool = False

        sandbox = controller.over_sandbox()
        sstable = iter(controller)

        pop_from_sandbox: bool = True
        pop_from_sstable: bool = True

        while not sandbox_is_done or not sstable_is_done:
            if pop_from_sandbox:
                try:
                    sandbox_key, sandbox_value = next(sandbox)
                except StopIteration:
                    sandbox_is_done = True
                    break

            if pop_from_sstable:
                try:
                    sstable_key, sstable_value = next(sstable)
                except StopIteration:
                    sstable_is_done = True
                    break

            if sandbox_key < sstable_key:
                pop_from_sandbox = True
                yield (sandbox_key, sandbox_value)

            elif sandbox_key == sstable_key:
                pop_from_sandbox = True
                pop_from_sstable = True
                yield (sandbox_key, sandbox_value)

            else:
                pop_from_sstable = True
                yield (sstable_key, sstable_value)

        if sandbox_is_done:
            yield from sstable

        if sstable_is_done:
            yield from sandbox
