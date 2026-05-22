"""Shared helpers for Deribit trade ingestion adapters."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast


def utc_now_ms() -> int:
    """Return current UTC timestamp in milliseconds."""

    return int(datetime.now(UTC).timestamp() * 1000)


def env_float_non_negative(value: str, default: float) -> float:
    """Parse non-negative float from env-like value with fallback."""

    try:
        parsed = float(value.strip())
    except ValueError:
        return default
    return max(0.0, parsed)


def env_int_min(value: str, default: int, minimum: int) -> int:
    """Parse bounded integer from env-like value with fallback."""

    try:
        parsed = int(value.strip())
    except ValueError:
        return default
    return max(minimum, parsed)


def is_route_failure(exc: Exception) -> bool:
    """Return whether exception message indicates route/network unreachable."""

    message = str(exc).lower()
    return "no route to host" in message or "network is unreachable" in message


def extract_result_rows(payload: dict[str, Any], *, payload_name: str) -> list[dict[str, object]]:
    """Extract ``result.trades`` list from Deribit payload."""

    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError(f"Unexpected Deribit {payload_name} response payload")
    rows = result.get("trades")
    if not isinstance(rows, list):
        return []
    return [cast(dict[str, object], row) for row in rows if isinstance(row, dict)]


def has_more(payload: dict[str, Any]) -> bool:
    """Return ``result.has_more`` from Deribit payload."""

    result = payload.get("result")
    if not isinstance(result, dict):
        return False
    return bool(result.get("has_more", False))
