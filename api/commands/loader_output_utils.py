"""Output serialization helpers for the Bronze loader command."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from ingestion.spot import SpotCandle


def sidecar_path_list(parquet_files: list[str], suffix: str) -> list[str]:
    """Build sorted unique sidecar paths for provided parquet files.

    Args:
        parquet_files: Parquet output files emitted by the Bronze persistence layer.
        suffix: Replacement file suffix for the requested sidecar type.

    Returns:
        Absolute sidecar paths sorted for deterministic command output.
    """

    return sorted({str(Path(path).with_suffix(suffix).resolve()) for path in parquet_files})


def serialize_candle(candle: SpotCandle) -> dict[str, object]:
    """Serialize one spot candle for JSON-compatible loader output.

    Args:
        candle: Spot candle DTO returned by an exchange fetcher.

    Returns:
        Dictionary representation with datetime fields encoded as ISO 8601 strings.
    """

    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data
