"""Validate config.yaml against the project's Pydantic schema."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

import yaml  # type: ignore[import-untyped]

from application.services.config_validation import validate_runtime_config


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate config.yaml with Pydantic")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config file")
    args = parser.parse_args()

    config_path = Path(args.config)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("config file must contain a top-level mapping")
    validate_runtime_config(cast(dict[str, object], payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
