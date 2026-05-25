"""Deribit historical option trades adapter."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import logging
import os
import time
from typing import Any, cast

from ingestion.exchanges.deribit_trade_common import (
    env_float_non_negative,
    env_int_min,
    extract_result_rows,
    has_more,
    is_route_failure,
    utc_now_ms,
)
from ingestion.http_client import HttpClientError, get_json

DERIBIT_OPTION_TRADES_MAX_PAGE_SIZE = 1000
DERIBIT_OPTION_TRADES_DEFAULT_PAGE_SIZE = 200
DERIBIT_OPTION_TRADES_BASE_URL_DEFAULT = "https://history.deribit.com"
DERIBIT_OPTION_TRADES_FALLBACK_BASE_URL = "https://www.deribit.com"
logger = logging.getLogger(__name__)


def _trades_base_url() -> str:
    """Return Deribit trades API base URL.

    Uses the same primary env override as perp trades for identical behavior,
    while keeping option-specific override as backward-compatible fallback.
    """

    value = os.getenv("DEPTH_DERIBIT_TRADES_BASE_URL")
    if value is None:
        value = os.getenv("DEPTH_DERIBIT_OPTION_TRADES_BASE_URL", DERIBIT_OPTION_TRADES_BASE_URL_DEFAULT)
    value = value.strip()
    return value.rstrip("/")


def _trades_base_urls() -> list[str]:
    """Return ordered base URL candidates for option trades API."""

    primary = _trades_base_url()
    if primary != DERIBIT_OPTION_TRADES_FALLBACK_BASE_URL:
        return [primary, DERIBIT_OPTION_TRADES_FALLBACK_BASE_URL]
    return [primary]


def _extract_result_rows(payload: dict[str, Any]) -> list[dict[str, object]]:
    return extract_result_rows(payload, payload_name="option trades")


def _has_more(payload: dict[str, Any]) -> bool:
    return has_more(payload)


def _utc_now_ms() -> int:
    return utc_now_ms()


def _inter_request_sleep_seconds() -> float:
    value = os.getenv("DEPTH_DERIBIT_TRADES_INTER_REQUEST_SLEEP_S")
    if value is None:
        value = os.getenv("DEPTH_DERIBIT_OPTION_TRADES_INTER_REQUEST_SLEEP_S", "0.15")
    return env_float_non_negative(value=value, default=0.15)


def _route_retry_attempts() -> int:
    value = os.getenv("DEPTH_DERIBIT_TRADES_ROUTE_RETRY_ATTEMPTS")
    if value is None:
        value = os.getenv("DEPTH_DERIBIT_OPTION_TRADES_ROUTE_RETRY_ATTEMPTS", "3")
    return env_int_min(value=value, default=3, minimum=1)


def _route_retry_backoff_base_seconds() -> float:
    value = os.getenv("DEPTH_DERIBIT_TRADES_ROUTE_RETRY_BACKOFF_BASE_S")
    if value is None:
        value = os.getenv("DEPTH_DERIBIT_OPTION_TRADES_ROUTE_RETRY_BACKOFF_BASE_S", "0.5")
    return env_float_non_negative(value=value, default=0.5)


def _max_pages_per_range() -> int:
    value = os.getenv("DEPTH_DERIBIT_TRADES_MAX_PAGES_PER_RANGE")
    if value is None:
        value = os.getenv("DEPTH_DERIBIT_OPTION_TRADES_MAX_PAGES_PER_RANGE", "5000")
    try:
        return int(value)
    except ValueError:
        return 5000


def fetch_option_trades_range(
    *,
    currency: str,
    start_open_ms: int,
    end_open_ms: int,
    count: int = DERIBIT_OPTION_TRADES_MAX_PAGE_SIZE,
) -> list[dict[str, object]]:
    """Fetch Deribit option trades in inclusive millisecond range."""

    if end_open_ms < start_open_ms:
        return []
    if count <= 0:
        raise ValueError("count must be positive")

    normalized_currency = currency.upper().strip()
    if not normalized_currency:
        raise ValueError("currency cannot be empty")
    cursor = start_open_ms
    collected: list[dict[str, object]] = []
    page_size = min(count, DERIBIT_OPTION_TRADES_MAX_PAGE_SIZE)
    max_pages = _max_pages_per_range()
    inter_request_sleep_s = _inter_request_sleep_seconds()
    route_retry_attempts = _route_retry_attempts()
    route_retry_backoff_base_s = _route_retry_backoff_base_seconds()
    pages = 0

    while cursor <= end_open_ms:
        pages += 1
        if max_pages > 0 and pages > max_pages:
            logger.warning(
                "Deribit option trades range page cap reached currency=%s start_ms=%s end_ms=%s max_pages=%s",
                normalized_currency,
                start_open_ms,
                end_open_ms,
                max_pages,
            )
            break
        params = {
            "currency": normalized_currency,
            "kind": "option",
            "start_timestamp": cursor,
            "end_timestamp": end_open_ms,
            "count": page_size,
            "sorting": "asc",
        }
        payload: Any | None = None
        last_error: Exception | None = None
        for base_url in _trades_base_urls():
            for attempt in range(1, route_retry_attempts + 1):
                logger.debug(
                    "Deribit option trades request base_url=%s currency=%s cursor=%s end_ms=%s attempt=%s/%s",
                    base_url,
                    normalized_currency,
                    cursor,
                    end_open_ms,
                    attempt,
                    route_retry_attempts,
                )
                try:
                    payload = get_json(
                        f"{base_url}/api/v2/public/get_last_trades_by_currency_and_time",
                        params=params,
                    )
                    break
                except HttpClientError as exc:
                    last_error = exc
                    if not is_route_failure(exc):
                        raise
                    if attempt < route_retry_attempts:
                        sleep_s = route_retry_backoff_base_s * (2 ** (attempt - 1))
                        if sleep_s > 0:
                            logger.debug(
                                "Deribit option trades retry sleep base_url=%s currency=%s cursor=%s sleep_s=%.3f",
                                base_url,
                                normalized_currency,
                                cursor,
                                sleep_s,
                            )
                            time.sleep(sleep_s)
                        continue
                    logger.warning(
                        "Deribit option trades route failure via base_url=%s currency=%s cursor=%s; trying fallback",
                        base_url,
                        normalized_currency,
                        cursor,
                    )
            if payload is not None:
                break
        if payload is None:
            assert last_error is not None
            raise last_error
        if not isinstance(payload, dict):
            raise ValueError("Unexpected Deribit option trades response format")
        rows = _extract_result_rows(payload)
        if not rows:
            break
        collected.extend(rows)
        last_ts = int(cast(Any, rows[-1]).get("timestamp", 0))
        if last_ts < cursor:
            break
        cursor = last_ts + 1
        if len(rows) < page_size:
            break
        if not _has_more(payload):
            break
        if inter_request_sleep_s > 0:
            logger.debug(
                "Deribit option trades inter-request sleep currency=%s cursor=%s sleep_s=%.3f",
                normalized_currency,
                cursor,
                inter_request_sleep_s,
            )
            time.sleep(inter_request_sleep_s)

    dedup: dict[tuple[int, str, str], dict[str, object]] = {}
    for row in collected:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        instrument_name = str(cast(Any, row).get("instrument_name", ""))
        if start_open_ms <= ts <= end_open_ms:
            dedup[(ts, trade_id, instrument_name)] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_option_trades_all(
    *,
    currency: str,
) -> list[dict[str, object]]:
    """Fetch available Deribit option trade history by paging backwards in fixed windows."""

    window_ms = 24 * 60 * 60 * 1000
    end_ms = _utc_now_ms()
    all_rows: list[dict[str, object]] = []

    while end_ms > 0:
        start_ms = max(0, end_ms - window_ms + 1)
        page_rows = fetch_option_trades_range(
            currency=currency,
            start_open_ms=start_ms,
            end_open_ms=end_ms,
        )
        if not page_rows:
            break
        all_rows.extend(page_rows)
        first_ts = int(cast(Any, page_rows[0]).get("timestamp", 0))
        if first_ts <= 0:
            break
        end_ms = first_ts - 1

    dedup: dict[tuple[int, str, str], dict[str, object]] = {}
    for row in all_rows:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        instrument_name = str(cast(Any, row).get("instrument_name", ""))
        dedup[(ts, trade_id, instrument_name)] = row
    return [dedup[key] for key in sorted(dedup)]
