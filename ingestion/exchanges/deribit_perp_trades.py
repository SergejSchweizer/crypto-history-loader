"""Deribit historical perp_trades adapter."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, cast

from ingestion.exchanges import deribit_trade_policy
from ingestion.exchanges.deribit import normalize_symbol
from ingestion.exchanges.deribit_trade_common import (
    extract_result_rows,
    has_more,
    is_route_failure,
    utc_now_ms,
)
from ingestion.http_client import HttpClientError, get_json

DERIBIT_PERP_TRADES_MAX_PAGE_SIZE = 1000
DERIBIT_PERP_TRADES_DEFAULT_PAGE_SIZE = 500
DERIBIT_PERP_TRADES_BASE_URL_DEFAULT = "https://history.deribit.com"
DERIBIT_PERP_TRADES_FALLBACK_BASE_URL = "https://www.deribit.com"
logger = logging.getLogger(__name__)


def _utc_now_ms() -> int:
    return utc_now_ms()


def _perp_trades_base_url() -> str:
    """Return Deribit perp_trades API base URL.

    Historical backfill requires the archive host. Can be overridden via
    ``DEPTH_DERIBIT_PERP_TRADES_BASE_URL`` for debugging or custom routing.
    """

    return deribit_trade_policy.normalized_base_url(
        "DEPTH_DERIBIT_PERP_TRADES_BASE_URL",
        "DEPTH_DERIBIT_TRADES_BASE_URL",
        default=DERIBIT_PERP_TRADES_BASE_URL_DEFAULT,
    )


def _perp_trades_base_urls() -> list[str]:
    """Return ordered base URL candidates for perp_trades API."""

    primary = _perp_trades_base_url()
    if primary == DERIBIT_PERP_TRADES_FALLBACK_BASE_URL:
        return [primary]
    return [primary, DERIBIT_PERP_TRADES_FALLBACK_BASE_URL]


def _extract_result_rows(payload: dict[str, Any]) -> list[dict[str, object]]:
    return extract_result_rows(payload, payload_name="perp_trades")


def _extract_rows_for_instrument(payload: dict[str, Any], *, instrument_name: str) -> list[dict[str, object]]:
    """Extract rows and keep only the requested instrument."""

    rows = _extract_result_rows(payload)
    return [row for row in rows if str(cast(Any, row).get("instrument_name", "")) == instrument_name]


def _has_more(payload: dict[str, Any]) -> bool:
    return has_more(payload)


def _inter_request_sleep_seconds() -> float:
    return deribit_trade_policy.non_negative_float(
        "DEPTH_DERIBIT_PERP_TRADES_INTER_REQUEST_SLEEP_S",
        "DEPTH_DERIBIT_TRADES_INTER_REQUEST_SLEEP_S",
        default=deribit_trade_policy.DEFAULT_INTER_REQUEST_SLEEP_S,
    )


def _route_retry_attempts() -> int:
    return deribit_trade_policy.int_at_least(
        "DEPTH_DERIBIT_PERP_TRADES_ROUTE_RETRY_ATTEMPTS",
        "DEPTH_DERIBIT_TRADES_ROUTE_RETRY_ATTEMPTS",
        default=deribit_trade_policy.DEFAULT_ROUTE_RETRY_ATTEMPTS,
        minimum=1,
    )


def _route_retry_backoff_base_seconds() -> float:
    return deribit_trade_policy.non_negative_float(
        "DEPTH_DERIBIT_PERP_TRADES_ROUTE_RETRY_BACKOFF_BASE_S",
        "DEPTH_DERIBIT_TRADES_ROUTE_RETRY_BACKOFF_BASE_S",
        default=deribit_trade_policy.DEFAULT_ROUTE_RETRY_BACKOFF_BASE_S,
    )


def _default_page_size() -> int:
    return deribit_trade_policy.page_size(
        "DEPTH_DERIBIT_PERP_TRADES_PAGE_SIZE",
        "DEPTH_DERIBIT_TRADES_PAGE_SIZE",
        default=DERIBIT_PERP_TRADES_DEFAULT_PAGE_SIZE,
        maximum=DERIBIT_PERP_TRADES_MAX_PAGE_SIZE,
    )


def fetch_perp_trades_range(
    *,
    symbol: str,
    market: str,
    start_open_ms: int,
    end_open_ms: int,
    count: int | None = None,
) -> list[dict[str, object]]:
    """Fetch Deribit perp_trades in inclusive millisecond range."""

    if end_open_ms < start_open_ms:
        return []
    if count is not None and count <= 0:
        raise ValueError("count must be positive")

    instrument_name = normalize_symbol(symbol=symbol, market=market)
    cursor = start_open_ms
    collected: list[dict[str, object]] = []
    page_size = min(count if count is not None else _default_page_size(), DERIBIT_PERP_TRADES_MAX_PAGE_SIZE)
    max_pages = deribit_trade_policy.max_pages_per_range(
        "DEPTH_DERIBIT_PERP_TRADES_MAX_PAGES_PER_RANGE",
        "DEPTH_DERIBIT_TRADES_MAX_PAGES_PER_RANGE",
    )
    inter_request_sleep_s = _inter_request_sleep_seconds()
    route_retry_attempts = _route_retry_attempts()
    route_retry_backoff_base_s = _route_retry_backoff_base_seconds()
    pages = 0

    logger.debug(
        "Deribit perp_trades range start instrument=%s start_ms=%s end_ms=%s page_size=%s max_pages=%s",
        instrument_name,
        start_open_ms,
        end_open_ms,
        page_size,
        max_pages,
    )
    while cursor <= end_open_ms:
        pages += 1
        if max_pages > 0 and pages > max_pages:
            logger.warning(
                "Deribit perp_trades range page cap reached instrument=%s start_ms=%s end_ms=%s max_pages=%s",
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
        for base_url in _perp_trades_base_urls():
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
                            "Deribit perp_trades request base_url=%s endpoint=%s instrument=%s cursor=%s "
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
                                        "Deribit perp_trades retry sleep base_url=%s endpoint=%s instrument=%s "
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
                                "Deribit perp_trades route failure via base_url=%s endpoint=%s instrument=%s "
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
            raise ValueError("Unexpected Deribit perp_trades response format")
        if endpoint_type == "get_last_trades_by_currency_and_time":
            rows = _extract_rows_for_instrument(payload, instrument_name=instrument_name)
        else:
            rows = _extract_result_rows(payload)
        if not rows:
            break
        collected.extend(rows)
        # Deribit's documented ``sorting`` order is trade-id oriented, so do not
        # assume the last row also has the greatest timestamp for pagination.
        max_ts = max(int(cast(Any, row).get("timestamp", 0)) for row in rows)
        if max_ts < cursor:
            break
        cursor = max_ts + 1
        if len(rows) < page_size:
            break
        if not _has_more(payload):
            break
        if inter_request_sleep_s > 0:
            logger.debug(
                "Deribit perp_trades inter-request sleep instrument=%s cursor=%s sleep_s=%.3f",
                instrument_name,
                cursor,
                inter_request_sleep_s,
            )
            time.sleep(inter_request_sleep_s)
        if pages % 100 == 0:
            logger.debug(
                (
                    "Deribit perp_trades range progress instrument=%s start_ms=%s end_ms=%s "
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
    rows_out = [dedup[key] for key in sorted(dedup)]
    logger.debug(
        "Deribit perp_trades range done instrument=%s start_ms=%s end_ms=%s pages=%s rows=%s deduped_rows=%s",
        instrument_name,
        start_open_ms,
        end_open_ms,
        pages,
        len(collected),
        len(rows_out),
    )
    return rows_out


def fetch_perp_trades_all(
    *,
    symbol: str,
    market: str,
    on_page: Callable[[list[dict[str, object]]], None] | None = None,
) -> list[dict[str, object]]:
    """Fetch available Deribit perp_trades history by paging backwards in fixed windows."""

    window_ms = 24 * 60 * 60 * 1000
    end_ms = _utc_now_ms()
    all_rows: list[dict[str, object]] = []

    while end_ms > 0:
        start_ms = max(0, end_ms - window_ms + 1)
        page_rows = fetch_perp_trades_range(
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
