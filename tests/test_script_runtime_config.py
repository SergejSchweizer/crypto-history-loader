"""Tests for script runtime config helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.runtime_config import read_logfile_from_config


def test_read_logfile_from_config_prefers_explicit_log_file(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("env:\n  DEPTH_SYNC_LOG_FILE: custom/run.log\n", encoding="utf-8")

    assert read_logfile_from_config(config_path) == Path("custom/run.log")


def test_read_logfile_from_config_uses_log_dir_default_name(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("env:\n  DEPTH_SYNC_LOG_DIR: .logs\n", encoding="utf-8")

    assert read_logfile_from_config(config_path) == Path(".logs") / "crypto-history-loader.log"


def test_read_logfile_from_config_keeps_legacy_top_level_logfile(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("logfile: legacy.log\n", encoding="utf-8")

    assert read_logfile_from_config(config_path) == Path("legacy.log")


def test_read_logfile_from_config_requires_log_destination(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("env: {}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="DEPTH_SYNC_LOG_FILE"):
        read_logfile_from_config(config_path)
