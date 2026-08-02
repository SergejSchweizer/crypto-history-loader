"""Batch-local bounded cache for shared Silver partition intermediates."""

from __future__ import annotations

from collections.abc import Callable


class SilverPartitionCache[T]:
    """Cache one partition-local normalized intermediate per explicit cache key."""

    def __init__(self, *, max_entries: int = 1) -> None:
        """Create a bounded cache that cannot retain complete historical inputs.

        Args:
            max_entries: Maximum partition-local frames retained at once. The default
                of one releases the previous partition before another is loaded.

        Raises:
            ValueError: If the requested cache size is less than one.
        """

        if max_entries < 1:
            raise ValueError("max_entries must be at least one")
        self._max_entries = max_entries
        self._values: dict[str, T] = {}

    def get_or_load(self, key: str, loader: Callable[[], T]) -> T:
        """Return an existing partition intermediate or load it once.

        Args:
            key: Stable source-partition identity.
            loader: Side-effecting loader invoked only for a cache miss.

        Returns:
            The cached or newly loaded partition-local intermediate.
        """

        cached = self._values.get(key)
        if cached is not None:
            return cached
        if len(self._values) >= self._max_entries:
            self._values.clear()
        value = loader()
        self._values[key] = value
        return value

    def clear(self) -> None:
        """Release all batch-local intermediates at a partition boundary."""

        self._values.clear()
