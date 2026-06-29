"""Tests for fetch executor helpers."""

from __future__ import annotations

import pytest

import application.services.fetch_executors as fetch_executors
from application.services.fetch_executors import run_with_optional_timeout


def test_run_with_optional_timeout_falls_back_when_process_start_eio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeQueue:
        def close(self) -> None:
            return None

        def join_thread(self) -> None:
            return None

    class _FakeProcess:
        def start(self) -> None:
            raise OSError(5, "Input/output error")

    class _FakeContext:
        def Queue(self, maxsize: int = 1) -> _FakeQueue:  # noqa: N802
            del maxsize
            return _FakeQueue()

        def Process(self, target: object, args: tuple[object, ...]) -> _FakeProcess:  # noqa: N802
            del target, args
            return _FakeProcess()

    monkeypatch.setattr(fetch_executors.mp, "get_context", lambda _: _FakeContext())

    value = run_with_optional_timeout(
        lambda **_: "ok",
        timeout_s=1.0,
        heartbeat_s=0.1,
        heartbeat=lambda _elapsed_s: None,
        use_process_timeout=True,
    )

    assert value == "ok"


@pytest.mark.parametrize(
    ("exc_name", "exc_type"),
    [
        ("TypeError", TypeError),
        ("ValueError", ValueError),
        ("TimeoutError", TimeoutError),
    ],
)
def test_run_with_optional_timeout_maps_worker_error_types(
    monkeypatch: pytest.MonkeyPatch, exc_name: str, exc_type: type[Exception]
) -> None:
    class _FakeQueue:
        def close(self) -> None:
            return None

        def join_thread(self) -> None:
            return None

        def empty(self) -> bool:
            return False

        def get_nowait(self) -> tuple[str, tuple[str, str]]:
            return ("err", (exc_name, "boom"))

    class _FakeProcess:
        exitcode = 0

        def start(self) -> None:
            return None

        def join(self, timeout: float | None = None) -> None:
            del timeout
            return None

        def is_alive(self) -> bool:
            return False

        def terminate(self) -> None:
            return None

    class _FakeContext:
        def Queue(self, maxsize: int = 1) -> _FakeQueue:  # noqa: N802
            del maxsize
            return _FakeQueue()

        def Process(self, target: object, args: tuple[object, ...]) -> _FakeProcess:  # noqa: N802
            del target, args
            return _FakeProcess()

    monkeypatch.setattr(fetch_executors.mp, "get_context", lambda _: _FakeContext())

    with pytest.raises(exc_type, match="boom"):
        run_with_optional_timeout(
            lambda **_: "ok",
            timeout_s=1.0,
            heartbeat_s=0.1,
            heartbeat=lambda _elapsed_s: None,
            use_process_timeout=True,
        )


def test_run_with_optional_timeout_raises_when_worker_exits_without_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeQueue:
        def close(self) -> None:
            return None

        def join_thread(self) -> None:
            return None

        def empty(self) -> bool:
            return True

    class _FakeProcess:
        exitcode = 0

        def start(self) -> None:
            return None

        def join(self, timeout: float | None = None) -> None:
            del timeout
            return None

        def is_alive(self) -> bool:
            return False

        def terminate(self) -> None:
            return None

    class _FakeContext:
        def Queue(self, maxsize: int = 1) -> _FakeQueue:  # noqa: N802
            del maxsize
            return _FakeQueue()

        def Process(self, target: object, args: tuple[object, ...]) -> _FakeProcess:  # noqa: N802
            del target, args
            return _FakeProcess()

    monkeypatch.setattr(fetch_executors.mp, "get_context", lambda _: _FakeContext())

    with pytest.raises(RuntimeError, match="exited without result"):
        run_with_optional_timeout(
            lambda **_: "ok",
            timeout_s=1.0,
            heartbeat_s=0.1,
            heartbeat=lambda _elapsed_s: None,
            use_process_timeout=True,
        )


def test_run_with_optional_timeout_returns_inline_result_without_process_timeout() -> None:
    def _fetch(**kwargs: object) -> str:
        assert kwargs == {"value": "ok"}
        return "done"

    value = run_with_optional_timeout(
        _fetch,
        timeout_s=None,
        heartbeat_s=60.0,
        heartbeat=lambda _elapsed_s: None,
        value="ok",
    )

    assert value == "done"
