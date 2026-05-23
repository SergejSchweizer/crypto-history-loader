#!/usr/bin/env python3
"""Runtime configuration loading utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    """Load config YAML as a mapping while preserving explicit error messages."""

    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to parse config.yaml. Install project dependencies.") from exc

    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError("config.yaml must contain a top-level mapping")
    return cast(dict[str, Any], loaded)


def read_logfile_from_config(config_path: Path | None = None) -> Path:
    """Read the logfile path from config.yaml."""

    path = config_path or Path("config.yaml")
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    config = _load_yaml_mapping(path)

    logfile = config.get("logfile")
    if isinstance(logfile, str):
        cleaned = logfile.strip()
        if not cleaned:
            raise ValueError("config.yaml key 'logfile' must not be empty")
        return Path(cleaned)

    env_config = config.get("env")
    if isinstance(env_config, dict):
        configured_file = env_config.get("DEPTH_SYNC_LOG_FILE")
        if isinstance(configured_file, str) and configured_file.strip():
            return Path(configured_file.strip())

        configured_dir = env_config.get("DEPTH_SYNC_LOG_DIR")
        if isinstance(configured_dir, str) and configured_dir.strip():
            return Path(configured_dir.strip()) / "crypto-market-loader.log"

    raise ValueError(
        "config.yaml must define 'env.DEPTH_SYNC_LOG_FILE', 'env.DEPTH_SYNC_LOG_DIR', or top-level 'logfile'"
    )
