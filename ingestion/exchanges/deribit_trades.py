"""Deribit historical trades adapter."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import Any, cast

from ingestion.exchanges.deribit import normalize_symbol
from ingestion.exchanges.deribit_trade_common import (
    env_float_non_negative,
    env_int_min,
    extract_result_rows,
    has_more,
    is_route_failure,
    utc_now_ms,
)
from ingestion.http_client import HttpClientError, get_json

DERIBIT_TRADES_MAX_PAGE_SIZE = 1000
DERIBIT_TRADES_DEFAULT_PAGE_SIZE = 200
DERIBIT_TRADES_BASE_URL_DEFAULT = "https://history.deribit.com"
DERIBIT_TRADES_FALLBACK_BASE_URL = "https://www.deribit.com"
logger = logging.getLogger(__name__)


def _utc_now_ms() -> int:
    return utc_now_ms()


def _trades_base_url() -> str:
    """Return Deribit trades API base URL.

    Historical backfill requires the archive host. Can be overridden via
    ``DEPTH_DERIBIT_TRADES_BASE_URL`` for debugging or custom routing.
    """

    value = os.getenv("DEPTH_DERIBIT_TRADES_BASE_URL", DERIBIT_TRADES_BASE_URL_DEFAULT).strip()
    return value.rstrip("/")


def _trades_base_urls() -> list[str]:
    """Return ordered base URL candidates for trades API."""

    primary = _trades_base_url()
    return [primary, DERIBIT_TRADES_FALLBACK_BASE_URL] if primary != DERIBIT_TRADES_FALLBACK_BASE_URL else [primary]


def _extract_result_rows(payload: dict[str, Any]) -> list[dict[str, object]]:
    return extract_result_rows(payload, payload_name="trades")


def _extract_rows_for_instrument(payload: dict[str, Any], *, instrument_name: str) -> list[dict[str, object]]:
    """Extract rows and keep only the requested instrument."""

    rows = _extract_result_rows(payload)
    return [row for row in rows if str(cast(Any, row).get("instrument_name", "")) == instrument_name]


def _has_more(payload: dict[str, Any]) -> bool:
    return has_more(payload)


def _inter_request_sleep_seconds() -> float:
    value = os.getenv("DEPTH_DERIBIT_TRADES_INTER_REQUEST_SLEEP_S", "0.15")
    return env_float_non_negative(value=value, default=0.15)


def _route_retry_attempts() -> int:
    value = os.getenv("DEPTH_DERIBIT_TRADES_ROUTE_RETRY_ATTEMPTS", "3")
    return env_int_min(value=value, default=3, minimum=1)


def _route_retry_backoff_base_seconds() -> float:
    value = os.getenv("DEPTH_DERIBIT_TRADES_ROUTE_RETRY_BACKOFF_BASE_S", "0.5")
    return env_float_non_negative(value=value, default=0.5)


def fetch_trades_range(
    *,
    symbol: str,
    market: str,
    start_open_ms: int,
    end_open_ms: int,
    count: int = DERIBIT_TRADES_MAX_PAGE_SIZE,
) -> list[dict[str, object]]:
    """Fetch Deribit trades in inclusive millisecond range."""

    if end_open_ms < start_open_ms:
        return []
    if count <= 0:
        raise ValueError("count must be positive")

    instrument_name = normalize_symbol(symbol=symbol, market=market)
    cursor = start_open_ms
    collected: list[dict[str, object]] = []
    page_size = min(count, DERIBIT_TRADES_MAX_PAGE_SIZE)
    max_pages = int(os.getenv("DEPTH_DERIBIT_TRADES_MAX_PAGES_PER_RANGE", "5000"))
    inter_request_sleep_s = _inter_request_sleep_seconds()
    route_retry_attempts = _route_retry_attempts()
    route_retry_backoff_base_s = _route_retry_backoff_base_seconds()
    pages = 0

    while cursor <= end_open_ms:
        pages += 1
        if max_pages > 0 and pages > max_pages:
            logger.warning(
                "Deribit trades range page cap reached instrument=%s start_ms=%s end_ms=%s max_pages=%s",
                instrument_name,
                start_open_ms,
                end_open_ms,
                max_pages,
            )
            break
        params: dict[str, object] = {
            "instrument_name": instrument_name,
            "start_timestamp": cursor,
            "end_timestamp": end_open_ms,
            "count": page_size,
            "sorting": "asc",
        }
        currency = instrument_name.split("-", 1)[0]
        currency_params: dict[str, object] = {
            "currency": currency,
            "kind": "future",
            "start_timestamp": cursor,
            "end_timestamp": end_open_ms,
            "count": page_size,
            "sorting": "asc",
        }
        payload: Any | None = None
        endpoint_type: str | None = None
        last_error: Exception | None = None
        for base_url in _trades_base_urls():
            endpoint_attempts: tuple[tuple[str, dict[str, object], str], ...] = (
                (
                    "get_last_trades_by_instrument_and_time",
                    params,
                    f"{base_url}/api/v2/public/get_last_trades_by_instrument_and_time",
                ),
                (
                    "get_last_trades_by_currency_and_time",
                    currency_params,
                    f"{base_url}/api/v2/public/get_last_trades_by_currency_and_time",
                ),
            )
            for endpoint_name, endpoint_params, endpoint_url in endpoint_attempts:
                payload = None
                endpoint_type = None
                for attempt in range(1, route_retry_attempts + 1):
                    logger.debug(
                        (
                            "Deribit perp trades request base_url=%s endpoint=%s instrument=%s cursor=%s "
                            "end_ms=%s attempt=%s/%s"
                        ),
                        base_url,
                        endpoint_name,
                        instrument_name,
                        cursor,
                        end_open_ms,
                        attempt,
                        route_retry_attempts,
                    )
                    try:
                        payload = get_json(endpoint_url, params=endpoint_params)
                        endpoint_type = endpoint_name
                        break
                    except HttpClientError as exc:
                        last_error = exc
                        if not is_route_failure(exc):
                            raise
                        if attempt < route_retry_attempts:
                            sleep_s = route_retry_backoff_base_s * (2 ** (attempt - 1))
                            if sleep_s > 0:
                                logger.debug(
                                    (
                                        "Deribit perp trades retry sleep base_url=%s endpoint=%s instrument=%s "
                                        "cursor=%s sleep_s=%.3f"
                                    ),
                                    base_url,
                                    endpoint_name,
                                    instrument_name,
                                    cursor,
                                    sleep_s,
                                )
                                time.sleep(sleep_s)
                            continue
                        logger.warning(
                            (
                                "Deribit perp trades route failure via base_url=%s endpoint=%s instrument=%s "
                                "cursor=%s; trying fallback"
                            ),
                            base_url,
                            endpoint_name,
                            instrument_name,
                            cursor,
                        )
                if payload is not None:
                    break
            if payload is not None:
                break
        if payload is None:
            assert last_error is not None
            raise last_error
        if not isinstance(payload, dict):
            raise ValueError("Unexpected Deribit trades response format")
        if endpoint_type == "get_last_trades_by_currency_and_time":
            rows = _extract_rows_for_instrument(payload, instrument_name=instrument_name)
        else:
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
                "Deribit perp trades inter-request sleep instrument=%s cursor=%s sleep_s=%.3f",
                instrument_name,
                cursor,
                inter_request_sleep_s,
            )
            time.sleep(inter_request_sleep_s)
        if pages % 100 == 0:
            logger.info(
                (
                    "Deribit trades range progress instrument=%s start_ms=%s end_ms=%s "
                    "pages=%s cursor_ms=%s rows_collected=%s"
                ),
                instrument_name,
                start_open_ms,
                end_open_ms,
                pages,
                cursor,
                len(collected),
            )

    dedup: dict[tuple[int, str], dict[str, object]] = {}
    for row in collected:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        if start_open_ms <= ts <= end_open_ms:
            dedup[(ts, trade_id)] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_trades_all(
    *,
    symbol: str,
    market: str,
    on_page: Callable[[list[dict[str, object]]], None] | None = None,
) -> list[dict[str, object]]:
    """Fetch available Deribit trade history by paging backwards in fixed windows."""

    window_ms = 24 * 60 * 60 * 1000
    end_ms = _utc_now_ms()
    all_rows: list[dict[str, object]] = []

    while end_ms > 0:
        start_ms = max(0, end_ms - window_ms + 1)
        page_rows = fetch_trades_range(
            symbol=symbol,
            market=market,
            start_open_ms=start_ms,
            end_open_ms=end_ms,
        )
        if not page_rows:
            break
        all_rows.extend(page_rows)
        if on_page is not None:
            on_page(page_rows)
        first_ts = int(cast(Any, page_rows[0]).get("timestamp", 0))
        if first_ts <= 0:
            break
        end_ms = first_ts - 1

    dedup: dict[tuple[int, str], dict[str, object]] = {}
    for row in all_rows:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        dedup[(ts, trade_id)] = row
    return [dedup[key] for key in sorted(dedup)]
