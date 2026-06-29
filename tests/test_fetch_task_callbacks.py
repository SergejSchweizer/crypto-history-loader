"""Tests for fetch task callback adapters."""

from __future__ import annotations

from dataclasses import dataclass

from application.services.fetch_task_callbacks import bind_task_chunk_callback


@dataclass(frozen=True)
class _Task:
    """Minimal task value used by callback adapter tests."""

    symbol: str


def test_bind_task_chunk_callback_returns_none_when_disabled() -> None:
    callback = bind_task_chunk_callback(_Task("BTC"), None)

    assert callback is None


def test_bind_task_chunk_callback_forwards_task_and_rows() -> None:
    task = _Task("BTC")
    seen: list[tuple[_Task, list[int]]] = []

    callback = bind_task_chunk_callback(task, lambda task_value, rows: seen.append((task_value, rows)))

    assert callback is not None
    callback([1, 2, 3])

    assert seen == [(task, [1, 2, 3])]
