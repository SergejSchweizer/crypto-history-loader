"""Tests for script logging integration with runtime logging."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

from scripts.logging_utils import configure_logger


def test_configure_logger_uses_module_file_under_configured_log_root(tmp_path: Path) -> None:
    """Script loggers should use the same per-module file contract as CLI loggers."""

    config_path = tmp_path / "config.yaml"
    log_root = tmp_path / ".logs"
    config_path.write_text(f"env:\n  DEPTH_SYNC_LOG_DIR: {log_root}\n", encoding="utf-8")

    logger = configure_logger("agents-sync", config_path)

    try:
        file_paths = [
            Path(cast(Any, handler).baseFilename) for handler in logger.handlers if hasattr(handler, "baseFilename")
        ]
        formats = [
            cast(str, cast(Any, handler).formatter._fmt)  # noqa: SLF001 - validates handler contract.
            for handler in logger.handlers
            if getattr(handler, "formatter", None) is not None
        ]

        assert logger.name == "crypto_loader.agents-sync"
        assert log_root / "agents-sync.log" in file_paths
        assert log_root / "crypto-loader.log" not in file_paths
        assert any("%(name)s" in item for item in formats)
        assert all("|" not in item for item in formats)
    finally:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    logging.getLogger("crypto_loader.agents-sync").handlers.clear()
