"""Tests for runtime logging utilities."""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path
from typing import Any, cast

import pytest

from application.services.runtime_service import (
    SingleInstanceError,
    SingleInstanceLock,
    _gzip_log_file,
    _safe_log_module_name,
    apply_repository_runtime_limits,
    configure_logging,
    enforce_log_retention,
    env_bool,
    env_float,
    env_int,
    env_list,
    env_str,
    fetch_concurrency,
    load_env_file,
)


def test_configure_logging_uses_module_name_for_log_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Module-specific logging should write to a matching log filename."""

    monkeypatch.setenv("DEPTH_SYNC_LOG_DIR", str(tmp_path))
    logger = configure_logging(module_name="loader")

    try:
        file_names = [
            Path(cast(Any, handler).baseFilename).name
            for handler in logger.handlers
            if hasattr(handler, "baseFilename")
        ]

        assert logger.name == "crypto_loader.loader"
        assert "loader.log" in file_names
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    logging.getLogger("crypto_loader.loader").handlers.clear()


def test_load_env_file_populates_missing_environment_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Local config files should provide process environment defaults."""

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "SYMBOLS=BTC ETH",
                "LEVELS=50",
                "EXISTING_VALUE=from_file",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("SYMBOLS", raising=False)
    monkeypatch.setenv("EXISTING_VALUE", "from_process")

    load_env_file(str(env_file))

    assert env_list("SYMBOLS", []) == ["BTC", "ETH"]
    assert env_list("MISSING_LIST", ["BTC"]) == ["BTC"]
    assert env_list("EXISTING_VALUE", []) == ["from_process"]


def test_fetch_concurrency_uses_fetch_runtime_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Compatibility wrapper should delegate to the fetch runtime policy owner."""

    monkeypatch.setenv("DEPTH_FETCH_CONCURRENCY", "99")

    assert fetch_concurrency() == 8


def test_apply_repository_runtime_limits_caps_polars_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Polars parallelism should never exceed the repository ceiling."""

    monkeypatch.setenv("POLARS_MAX_THREADS", "16")

    apply_repository_runtime_limits()

    assert os.getenv("POLARS_MAX_THREADS") == "4"


def test_apply_repository_runtime_limits_sets_default_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing Polars limits should fall back to the repository ceiling."""

    monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)

    apply_repository_runtime_limits()

    assert os.getenv("POLARS_MAX_THREADS") == "4"


def test_configure_logging_ignores_global_file_override_for_module_logger(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Module logger should keep per-module file naming even if global file env var is set."""

    monkeypatch.setenv("DEPTH_SYNC_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("DEPTH_SYNC_LOG_FILE", str(tmp_path / "global.log"))
    logger = configure_logging(module_name="silver-build")

    try:
        file_names = [
            Path(cast(Any, handler).baseFilename).name
            for handler in logger.handlers
            if hasattr(handler, "baseFilename")
        ]
        assert "silver-build.log" in file_names
        assert "global.log" not in file_names
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    logging.getLogger("crypto_loader.silver-build").handlers.clear()


def test_configure_logging_uses_unified_format_with_module_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Logger formatter should include module logger name for unified cross-module logs."""

    monkeypatch.setenv("DEPTH_SYNC_LOG_DIR", str(tmp_path))
    logger = configure_logging(module_name="gold-build")
    try:
        formats = [
            cast(str, cast(Any, handler).formatter._fmt)  # noqa: SLF001
            for handler in logger.handlers
            if getattr(handler, "formatter", None) is not None
        ]
        assert any("%(name)s" in item for item in formats)
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    logging.getLogger("crypto_loader.gold-build").handlers.clear()


def test_configure_logging_falls_back_to_stderr_when_log_file_cannot_be_created(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Logging should remain usable when the configured log path rejects file creation."""

    import application.services.runtime_service as module

    class FailingFileHandler:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise OSError("read-only filesystem")

    monkeypatch.setenv("DEPTH_SYNC_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(module, "RetentionTimedRotatingFileHandler", FailingFileHandler)
    logger = configure_logging(module_name="fallback-test")

    try:
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)
        assert not hasattr(logger.handlers[0], "baseFilename")
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    logging.getLogger("crypto_loader.fallback-test").handlers.clear()


def test_configure_logging_reuses_existing_module_logger(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Repeated configuration should preserve the original logger and avoid duplicate handlers."""

    monkeypatch.setenv("DEPTH_SYNC_LOG_DIR", str(tmp_path))
    logger = configure_logging(module_name="reuse-test")

    try:
        handler_count = len(logger.handlers)

        assert configure_logging(module_name="reuse-test", debug=True) is logger
        assert len(logger.handlers) == handler_count
        assert logger.level == logging.INFO
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    logging.getLogger("crypto_loader.reuse-test").handlers.clear()


def test_enforce_log_retention_keeps_five_plain_days_archives_older_and_deletes_stale(tmp_path: Path) -> None:
    """Daily log retention should keep five plain rotations, gzip older days, and prune stale archives."""

    log_path = tmp_path / "loader.log"
    today = date(2026, 7, 12)
    for days_ago in range(1, 8):
        rotated_date = today.fromordinal(today.toordinal() - days_ago)
        (tmp_path / f"loader.log.{rotated_date.isoformat()}").write_text(f"day {days_ago}\n", encoding="utf-8")
    stale_archive_date = today.fromordinal(today.toordinal() - 120)
    stale_archive = tmp_path / f"loader.log.{stale_archive_date.isoformat()}.gz"
    stale_archive.write_bytes(b"stale")

    enforce_log_retention(log_path, today=today)

    plain_dates = {path.name.removeprefix("loader.log.") for path in tmp_path.glob("loader.log.????-??-??")}
    expected_plain_dates = {today.fromordinal(today.toordinal() - days_ago).isoformat() for days_ago in range(1, 6)}
    assert plain_dates == expected_plain_dates
    assert (tmp_path / f"loader.log.{today.fromordinal(today.toordinal() - 6).isoformat()}.gz").exists()
    assert (tmp_path / f"loader.log.{today.fromordinal(today.toordinal() - 7).isoformat()}.gz").exists()
    assert not stale_archive.exists()


def test_runtime_environment_helpers_cover_defaults_and_invalid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment helpers should use defaults for missing or malformed values."""

    monkeypatch.delenv("BOOL_VALUE", raising=False)
    monkeypatch.delenv("FLOAT_VALUE", raising=False)
    monkeypatch.delenv("INT_VALUE", raising=False)
    monkeypatch.delenv("LIST_VALUE", raising=False)
    assert env_bool("BOOL_VALUE", True) is True
    assert env_float("FLOAT_VALUE", 1.5) == 1.5
    assert env_int("INT_VALUE", 7) == 7
    assert env_list("LIST_VALUE", ["BTC"]) == ["BTC"]
    assert env_str("STRING_VALUE", "fallback") == "fallback"

    monkeypatch.setenv("BOOL_VALUE", " yes ")
    monkeypatch.setenv("FLOAT_VALUE", "bad")
    monkeypatch.setenv("INT_VALUE", "bad")
    monkeypatch.setenv("LIST_VALUE", ",  ")
    assert env_bool("BOOL_VALUE", False) is True
    assert env_float("FLOAT_VALUE", 1.5) == 1.5
    assert env_int("INT_VALUE", 7) == 7
    assert env_list("LIST_VALUE", ["BTC"]) == ["BTC"]


def test_load_env_file_ignores_comments_invalid_lines_and_strips_quotes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The local env parser should ignore malformed input and preserve process values."""

    env_file = tmp_path / ".env"
    env_file.write_text("# comment\ninvalid\nQUOTED=\"value\"\nSINGLE='one'\n", encoding="utf-8")
    monkeypatch.delenv("QUOTED", raising=False)
    monkeypatch.delenv("SINGLE", raising=False)
    load_env_file(str(env_file))
    assert os.environ["QUOTED"] == "value"
    assert os.environ["SINGLE"] == "one"
    load_env_file(str(tmp_path / "missing.env"))


def test_runtime_limits_normalize_invalid_and_low_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invalid and below-ceiling Polars settings should be normalized deterministically."""

    monkeypatch.setenv("POLARS_MAX_THREADS", "invalid")
    apply_repository_runtime_limits()
    assert os.environ["POLARS_MAX_THREADS"] == "4"
    monkeypatch.setenv("POLARS_MAX_THREADS", "2")
    apply_repository_runtime_limits()
    assert os.environ["POLARS_MAX_THREADS"] == "2"


def test_log_retention_validates_arguments_and_missing_directory(tmp_path: Path) -> None:
    """Retention should reject unsafe settings and tolerate a missing log directory."""

    with pytest.raises(ValueError, match="plain_daily_files"):
        enforce_log_retention(tmp_path / "missing" / "loader.log", plain_daily_files=-1)
    with pytest.raises(ValueError, match="archive_retention_days"):
        enforce_log_retention(tmp_path / "missing" / "loader.log", archive_retention_days=0)
    enforce_log_retention(tmp_path / "missing" / "loader.log")


def test_single_instance_lock_rejects_when_lock_is_held(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A simulated held lock must fail without leaking the file descriptor."""

    import application.services.runtime_service as module

    monkeypatch.setattr(module.fcntl, "flock", lambda *_args: (_ for _ in ()).throw(BlockingIOError()))
    with pytest.raises(SingleInstanceError):
        with SingleInstanceLock(str(tmp_path / "loader.lock")):
            pass


def test_runtime_log_helpers_handle_existing_archive_and_unsafe_module_names(tmp_path: Path) -> None:
    """Archive writes are idempotent and module names cannot escape the shared log directory."""

    source = tmp_path / "loader.log.2026-07-01"
    source.write_text("old\n", encoding="utf-8")
    archive = _gzip_log_file(source)
    assert archive.exists()
    assert not source.exists()
    source.write_text("duplicate\n", encoding="utf-8")
    assert _gzip_log_file(source) == archive
    assert not source.exists()
    assert _safe_log_module_name(" /loader\\worker ") == "-loader-worker"
    assert _safe_log_module_name("  ") == "crypto-loader"


def test_single_instance_lock_writes_pid_and_releases_file(tmp_path: Path) -> None:
    """A successful lock records its owner and permits a subsequent process lock."""

    lock_path = tmp_path / "locks" / "loader.lock"
    with SingleInstanceLock(str(lock_path)):
        assert lock_path.exists()
        assert lock_path.read_text(encoding="utf-8") == str(os.getpid())
    with SingleInstanceLock(str(lock_path)):
        assert lock_path.exists()
