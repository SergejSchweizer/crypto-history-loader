"""Task callback adapters for fetch orchestration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

TTask = TypeVar("TTask")
TRow = TypeVar("TRow")


def bind_task_chunk_callback(
    task: TTask,
    on_task_chunk: Callable[[TTask, list[TRow]], None] | None,
) -> Callable[[list[TRow]], None] | None:
    """Bind a task-level chunk callback to a fetcher history callback.

    Args:
        task: Fetch task DTO associated with every emitted chunk.
        on_task_chunk: Optional callback that accepts the task and chunk rows.

    Returns:
        History callback accepting only rows, or ``None`` when chunk forwarding is disabled.
    """

    if on_task_chunk is None:
        return None

    def _forward_chunk(rows: list[TRow]) -> None:
        on_task_chunk(task, rows)

    return _forward_chunk
