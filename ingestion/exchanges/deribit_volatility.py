"""Deribit historical volatility adapters."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import get_json

DERIBIT_HISTORICAL_VOLATILITY_URL = "https://www.deribit.com/api/v2/public/get_historical_volatility"
DERIBIT_VOLATILITY_INDEX_DATA_URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
DERIBIT_VOLATILITY_INDEX_MAX_POINTS_PER_REQUEST = 1000


def fetch_historical_volatility_range(
    *,
    currency: str,
    start_open_ms: int,
    end_open_ms: int,
) -> list[dict[str, object]]:
    """Fetch historical volatility rows in inclusive millisecond range."""

    if end_open_ms < start_open_ms:
        return []
    normalized_currency = currency.upper().strip()
    if not normalized_currency:
        raise ValueError("currency cannot be empty")

    payload = get_json(
        DERIBIT_HISTORICAL_VOLATILITY_URL,
        params={
            "currency": normalized_currency,
            "start_timestamp": start_open_ms,
            "end_timestamp": end_open_ms,
        },
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit historical volatility response format")

    result = payload.get("result")
    if not isinstance(result, list):
        return []

    rows: dict[int, dict[str, object]] = {}
    for item in result:
        if not isinstance(item, list) or len(item) < 2:
            continue
        ts = int(cast(Any, item[0]))
        if start_open_ms <= ts <= end_open_ms:
            rows[ts] = {"timestamp": ts, "volatility": float(cast(Any, item[1]))}
    return [rows[key] for key in sorted(rows)]


def fetch_historical_volatility_all(*, currency: str) -> list[dict[str, object]]:
    """Fetch historical volatility rows from epoch to now."""

    end_ms = int(datetime.now(UTC).timestamp() * 1000)
    return fetch_historical_volatility_range(currency=currency, start_open_ms=0, end_open_ms=end_ms)


def fetch_volatility_index_data_range(
    *,
    currency: str,
    start_open_ms: int,
    end_open_ms: int,
    resolution: str,
) -> list[dict[str, object]]:
    """Fetch volatility index rows in inclusive millisecond range."""

    if end_open_ms < start_open_ms:
        return []
    normalized_currency = currency.upper().strip()
    if not normalized_currency:
        raise ValueError("currency cannot be empty")

    cursor = start_open_ms
    rows: dict[int, dict[str, object]] = {}
    while cursor <= end_open_ms:
        payload = get_json(
            DERIBIT_VOLATILITY_INDEX_DATA_URL,
            params={
                "currency": normalized_currency,
                "start_timestamp": cursor,
                "end_timestamp": end_open_ms,
                "resolution": resolution,
            },
        )
        if not isinstance(payload, dict):
            raise ValueError("Unexpected Deribit volatility index response format")
        result = payload.get("result")
        if not isinstance(result, dict):
            break
        data = result.get("data")
        continuation = result.get("continuation")
        if not isinstance(data, list) or not data:
            break

        last_ts = cursor
        for item in data:
            if not isinstance(item, list) or len(item) < 2:
                continue
            ts = int(cast(Any, item[0]))
            value = float(cast(Any, item[1]))
            if start_open_ms <= ts <= end_open_ms:
                rows[ts] = {"timestamp": ts, "index_value": value}
            if ts > last_ts:
                last_ts = ts

        if isinstance(continuation, bool) and continuation and last_ts >= cursor:
            cursor = last_ts + 1
            continue
        break

    return [rows[key] for key in sorted(rows)]


def fetch_volatility_index_data_all(*, currency: str, resolution: str) -> list[dict[str, object]]:
    """Fetch volatility index rows from epoch to now."""

    end_ms = int(datetime.now(UTC).timestamp() * 1000)
    return fetch_volatility_index_data_range(
        currency=currency,
        start_open_ms=0,
        end_open_ms=end_ms,
        resolution=resolution,
    )
