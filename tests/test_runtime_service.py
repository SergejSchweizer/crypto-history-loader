"""Tests for runtime logging utilities."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any, cast

import pytest

from application.services.runtime_service import (
    configure_logging,
    enforce_log_retention,
    env_list,
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

        assert logger.name == "crypto_history_loader.loader"
        assert "loader.log" in file_names
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        logging.getLogger("crypto_history_loader.loader").handlers.clear()


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
        logging.getLogger("crypto_history_loader.silver-build").handlers.clear()


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
        logging.getLogger("crypto_history_loader.gold-build").handlers.clear()


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
