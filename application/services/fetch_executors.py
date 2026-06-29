"""Execution helpers for fetch-service task orchestrators."""

from __future__ import annotations

import logging
import multiprocessing as mp
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TypeVar, cast

TResult = TypeVar("TResult")


class ResultQueueProtocol:
    """Protocol subset used by timeout worker process queues."""

    def put(self, item: object) -> object: ...


def elapsed_seconds(started_at: datetime) -> int:
    """Return elapsed integer seconds from ``started_at`` until now (UTC)."""

    return int((datetime.now(UTC) - started_at).total_seconds())


def timeout_worker(
    result_queue: ResultQueueProtocol,
    fn: Callable[..., object],
    kwargs: dict[str, object],
) -> None:
    """Execute one fetch call in a child process and return result state via queue."""

    try:
        result_queue.put(("ok", fn(**kwargs)))
    except Exception as exc:  # noqa: BLE001
        result_queue.put(("err", (exc.__class__.__name__, str(exc))))


def run_with_optional_timeout(
    fn: Callable[..., TResult],
    *,
    timeout_s: float | None,
    heartbeat_s: float,
    heartbeat: Callable[[int], None],
    use_process_timeout: bool = False,
    **kwargs: object,
) -> TResult:
    """Run callable in a worker process with optional hard timeout and heartbeat."""

    def _run_inline_with_heartbeat() -> TResult:
        started = datetime.now(UTC)
        stop_event = threading.Event()

        def _heartbeat_loop() -> None:
            interval = max(0.1, heartbeat_s)
            while not stop_event.wait(interval):
                heartbeat(elapsed_seconds(started))

        watcher = threading.Thread(target=_heartbeat_loop, daemon=True)
        watcher.start()
        try:
            return fn(**kwargs)
        finally:
            stop_event.set()
            watcher.join(timeout=1.0)

    if timeout_s is None or not use_process_timeout:
        return _run_inline_with_heartbeat()

    started = datetime.now(UTC)
    ctx = mp.get_context("fork")
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(target=timeout_worker, args=(result_queue, fn, kwargs))
    try:
        process.start()
    except OSError as exc:
        if exc.errno != 5:
            raise
        logging.getLogger(__name__).warning(
            "Worker process startup failed with EIO; falling back to inline execution without hard timeout"
        )
        result_queue.close()
        result_queue.join_thread()
        return _run_inline_with_heartbeat()
    deadline = time.monotonic() + timeout_s
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                process.terminate()
                process.join(timeout=2)
                raise TimeoutError(f"Fetch task timed out after {timeout_s:.1f}s")
            wait_s = min(max(0.1, heartbeat_s), remaining)

            process.join(timeout=wait_s)
            if process.is_alive():
                heartbeat(elapsed_seconds(started))
                continue

            if result_queue.empty():
                raise RuntimeError(f"Fetch worker exited without result (exitcode={process.exitcode})")

            status, payload = result_queue.get_nowait()
            if status == "ok":
                return cast(TResult, payload)
            exc_name, exc_message = payload
            if exc_name == "TypeError":
                raise TypeError(exc_message)
            if exc_name == "ValueError":
                raise ValueError(exc_message)
            if exc_name == "TimeoutError":
                raise TimeoutError(exc_message)
            raise RuntimeError(f"{exc_name}: {exc_message}")
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
        result_queue.close()
        result_queue.join_thread()


def run_with_optional_history_chunk(
    *,
    runner: Callable[..., TResult],
    fn: Callable[..., TResult],
    timeout_s: float | None,
    heartbeat_s: float,
    heartbeat: Callable[[int], None],
    use_process_timeout: bool,
    kwargs: dict[str, object],
) -> TResult:
    """Run one fetcher with optional `on_history_chunk` fallback compatibility."""

    try:
        return runner(
            fn,
            timeout_s=timeout_s,
            heartbeat_s=heartbeat_s,
            heartbeat=heartbeat,
            use_process_timeout=use_process_timeout,
            **kwargs,
        )
    except TypeError as exc:
        if "on_history_chunk" not in str(exc):
            raise
        retry_kwargs = dict(kwargs)
        retry_kwargs.pop("on_history_chunk", None)
        return runner(
            fn,
            timeout_s=timeout_s,
            heartbeat_s=heartbeat_s,
            heartbeat=heartbeat,
            use_process_timeout=use_process_timeout,
            **retry_kwargs,
        )
