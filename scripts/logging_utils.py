#!/usr/bin/env python3
"""Shared logging configuration for scripts."""

from __future__ import annotations

import logging
from pathlib import Path

from scripts.runtime_config import read_logfile_from_config

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
_LOGGING_CONFIGURED = False


def configure_logger(name: str, config_path: Path | None = None) -> logging.Logger:
    """Configure and return a module logger using shared config.yaml logfile."""

    global _LOGGING_CONFIGURED

    logfile_path = read_logfile_from_config(config_path)
    logfile_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    if not _LOGGING_CONFIGURED:
        formatter = logging.Formatter(LOG_FORMAT)
        file_handler = logging.FileHandler(logfile_path, encoding="utf-8")
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        root_logger.handlers.clear()
        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)
        root_logger.setLevel(logging.INFO)
        _LOGGING_CONFIGURED = True

    return logging.getLogger(name)
