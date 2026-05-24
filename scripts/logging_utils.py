#!/usr/bin/env python3
"""Shared logging configuration for scripts."""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from scripts.runtime_config import module_logfile_from_config

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def configure_logger(name: str, config_path: Path | None = None) -> logging.Logger:
    """Configure and return a module logger with module-specific logfile."""

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logfile_path = module_logfile_from_config(name, config_path)
    logfile_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = TimedRotatingFileHandler(
        filename=logfile_path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        utc=True,
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger
