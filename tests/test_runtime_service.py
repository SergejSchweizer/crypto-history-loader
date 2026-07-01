"""Tests for runtime logging utilities."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

import pytest

from application.services.runtime_service import configure_logging, env_list, fetch_concurrency, load_env_file


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
