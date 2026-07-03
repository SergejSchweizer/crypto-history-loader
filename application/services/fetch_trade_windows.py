"""Trade-window helpers for Bronze trade fetch orchestration."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast

from application.services.fetch_executors import elapsed_seconds
from application.services.fetch_runtime_policy import trade_window_ms
from ingestion.http_client import HttpClientError
from ingestion.spot_ohlcv import Exchange
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick

logger = logging.getLogger(__name__)


def classify_trade_fetch_error(exc: Exception) -> str:
    """Classify trade fetch errors for operational summaries.

    Args:
        exc: Exception raised by a trade fetch path.

    Returns:
        Stable error class used in logs and per-task error payloads.
    """

    message = str(exc).lower()
    network_markers = (
        "no route to host",
        "name or service not known",
        "temporary failure in name resolution",
        "network is unreachable",
        "connection refused",
    )
    if any(marker in message for marker in network_markers):
        return "NET_UNREACHABLE"
    if "timeout" in message or "timed out" in message:
        return "NET_TIMEOUT"
    return "OTHER"


def is_recoverable_trade_window_error(exc: Exception) -> bool:
    """Return whether a trade-window error should not abort later windows.

    Args:
        exc: Exception raised while fetching one bounded trade window.

    Returns:
        True when the caller can continue later windows and report a partial
        window failure; false when the error should propagate immediately.
    """

    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, HttpClientError):
        return classify_trade_fetch_error(exc) in {"NET_TIMEOUT", "NET_UNREACHABLE"}
    return False


def trade_unique_key(item: TradeTick | OptionTradeTick) -> tuple[datetime, str, str]:
    """Return stable dedup key for perp/option trade ticks.

    Args:
        item: Perp or option trade tick.

    Returns:
        Natural trade key preserving same-timestamp distinct trade IDs and
        separating option instruments that can share trade IDs.
    """

    return (item.trade_time, item.trade_id, getattr(item, "instrument_name", ""))


def dedupe_sort_trade_rows(rows: list[TradeTick | OptionTradeTick]) -> list[TradeTick | OptionTradeTick]:
    """Deduplicate trade rows and return deterministic time/id ordering.

    Args:
        rows: Raw trade ticks from one or more archive windows.

    Returns:
        Deduplicated rows sorted by trade timestamp, trade ID, and option
        instrument name when present.
    """

    unique = {trade_unique_key(item): item for item in rows}
    return [unique[key] for key in sorted(unique)]


def fetch_trade_window(
    *,
    range_fetcher: Callable[..., list[TradeTick] | list[OptionTradeTick] | list[TradeTick | OptionTradeTick]],
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    start_open_ms: int,
    end_open_ms: int,
) -> tuple[list[TradeTick | OptionTradeTick], str | None]:
    """Fetch one trade window and isolate transient archive/network failures.

    Args:
        range_fetcher: Adapter function that fetches one inclusive trade range.
        exchange: Exchange identifier.
        market: Trade dataset family.
        symbol: Exchange symbol or option underlying.
        start_open_ms: Inclusive window start in epoch milliseconds.
        end_open_ms: Inclusive window end in epoch milliseconds.

    Returns:
        Pair of fetched rows and optional recoverable error text.

    Raises:
        Exception: Non-recoverable adapter errors are re-raised unchanged.
    """

    started_at = datetime.now(UTC)
    logger.debug(
        "Trade-window fetch start exchange=%s market=%s symbol=%s start_ms=%s end_ms=%s",
        exchange,
        market,
        symbol,
        start_open_ms,
        end_open_ms,
    )
    try:
        rows = range_fetcher(
            exchange=exchange,
            symbol=symbol,
            market=market,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
        )
    except Exception as exc:
        if not is_recoverable_trade_window_error(exc):
            raise
        error_class = classify_trade_fetch_error(exc)
        message = f"[{error_class}] {exc}"
        logger.warning(
            "Trade-window fetch failed class=%s exchange=%s market=%s symbol=%s start_ms=%s end_ms=%s error=%s",
            error_class,
            exchange,
            market,
            symbol,
            start_open_ms,
            end_open_ms,
            exc,
        )
        return [], message
    elapsed_s = elapsed_seconds(started_at)
    logger.debug(
        "Trade-window fetch done exchange=%s market=%s symbol=%s start_ms=%s end_ms=%s rows=%s elapsed_s=%s",
        exchange,
        market,
        symbol,
        start_open_ms,
        end_open_ms,
        len(rows),
        elapsed_s,
    )
    return cast(list[TradeTick | OptionTradeTick], rows), None


def raise_if_all_trade_windows_failed(
    *,
    failed_windows: list[str],
    attempted_windows: int,
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
) -> None:
    """Fail a trade task only when every attempted trade window failed.

    Args:
        failed_windows: Window failure messages collected during a fetch phase.
        attempted_windows: Count of windows attempted in the same phase.
        exchange: Exchange identifier for the error message.
        market: Trade dataset family for the error message.
        symbol: Symbol for the error message.

    Raises:
        RuntimeError: Every attempted trade window failed.
    """

    if attempted_windows > 0 and len(failed_windows) == attempted_windows:
        raise RuntimeError(
            "all trade windows failed "
            f"exchange={exchange} market={market} symbol={symbol} failures={failed_windows[:5]}"
        )


def trade_window_size_ms(market: TradeMarket) -> int:
    """Return operational trade fetch window size by dataset family.

    Args:
        market: Trade dataset family.

    Returns:
        Bounded window size in milliseconds from the typed runtime policy.
    """

    return trade_window_ms(market)


def split_range_into_trade_windows(
    start_open_ms: int,
    end_open_ms: int,
    *,
    market: TradeMarket,
) -> list[tuple[int, int]]:
    """Split an inclusive trade range into market-specific bounded windows.

    Args:
        start_open_ms: Inclusive range start in epoch milliseconds.
        end_open_ms: Inclusive range end in epoch milliseconds.
        market: Trade dataset family.

    Returns:
        Chronological inclusive windows sized by the runtime trade policy.
    """

    if end_open_ms < start_open_ms:
        return []
    step_ms = trade_window_size_ms(market)
    cursor = start_open_ms
    windows: list[tuple[int, int]] = []
    while cursor <= end_open_ms:
        window_end = min(cursor + step_ms - 1, end_open_ms)
        windows.append((cursor, window_end))
        cursor = window_end + 1
    return windows


def trade_windows_in_random_order(
    start_open_ms: int,
    end_open_ms: int,
    *,
    market: TradeMarket,
) -> list[tuple[int, int]]:
    """Return deterministic trade fetch windows for the requested market.

    Args:
        start_open_ms: Inclusive range start in epoch milliseconds.
        end_open_ms: Inclusive range end in epoch milliseconds.
        market: Trade dataset family.

    Returns:
        Chronological window list. The historical name is preserved because the
        fetch service also has day-window helpers with the same compatibility
        convention.
    """

    return sorted(split_range_into_trade_windows(start_open_ms, end_open_ms, market=market))


def log_trade_window_progress(
    *,
    phase: str,
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    completed_windows: int,
    total_windows: int,
    rows: int,
    failed_windows: int,
    started_at: datetime,
) -> None:
    """Log long-running trade fetch progress without changing fetch behavior.

    Args:
        phase: Fetch phase name used in the log line.
        exchange: Exchange identifier.
        market: Trade dataset family.
        symbol: Symbol being fetched.
        completed_windows: Completed window count.
        total_windows: Planned window count.
        rows: Rows collected so far.
        failed_windows: Recoverable failed-window count.
        started_at: Phase start timestamp used for elapsed seconds.
    """

    if completed_windows == total_windows or completed_windows == 1 or completed_windows % 25 == 0:
        logger.debug(
            ("Trade %s progress exchange=%s market=%s symbol=%s windows=%s/%s rows=%s failed_windows=%s elapsed_s=%s"),
            phase,
            exchange,
            market,
            symbol,
            completed_windows,
            total_windows,
            rows,
            failed_windows,
            elapsed_seconds(started_at),
        )
