#!/usr/bin/env python3
"""Shared logging configuration for scripts."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from application.services.runtime_service import configure_logging
from scripts.runtime_config import read_logfile_from_config


def configure_logger(name: str, config_path: Path | None = None) -> logging.Logger:
    """Configure and return a module logger using the shared runtime logging contract."""

    previous_log_dir = os.environ.get("DEPTH_SYNC_LOG_DIR")
    logfile_path = read_logfile_from_config(config_path)
    os.environ["DEPTH_SYNC_LOG_DIR"] = str(logfile_path.parent)
    try:
        return configure_logging(module_name=name)
    finally:
        if previous_log_dir is None:
            os.environ.pop("DEPTH_SYNC_LOG_DIR", None)
        else:
            os.environ["DEPTH_SYNC_LOG_DIR"] = previous_log_dir
