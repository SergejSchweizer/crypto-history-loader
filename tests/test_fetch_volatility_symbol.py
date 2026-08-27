"""Tests for volatility symbol-level fetch planning."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from application.services.fetch_volatility_symbol import fetch_symbol_volatility
from ingestion.volatility import VolatilityPoint


def test_fetch_symbol_volatility_tail_uses_dataset_type_and_latest_open_time() -> None:
    latest_open_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    end_open_ms = int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000)
    reader_calls: list[str] = []
    range_calls: list[tuple[int, int]] = []

    def _latest_reader(**kwargs: object) -> datetime:
        reader_calls.append(str(kwargs["dataset_type"]))
        return latest_open_time

    def _range_fetcher(**kwargs: object) -> list[VolatilityPoint]:
        range_calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_volatility(
        exchange="deribit",
        market="perp",
        symbol="btc",
        timeframe="1m",
        lake_root="lake/bronze",
        dataset_type="volatility_index_data",
        timeframe_normalizer=lambda **_kwargs: "1m",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called for tail deltas"),
        range_fetcher=_range_fetcher,
        latest_open_time_reader=_latest_reader,
        tail_delta_only=True,
    )

    assert rows == []
    assert reader_calls == ["volatility_index_data"]
    assert range_calls == [(int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000), end_open_ms)]


def test_fetch_symbol_volatility_full_gap_keeps_range_unsplit() -> None:
    start_ms = int(datetime(2026, 4, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[VolatilityPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    rows = fetch_symbol_volatility(
        exchange="deribit",
        market="perp",
        symbol="btc",
        timeframe="1m",
        lake_root="lake/bronze",
        dataset_type="volatility_index_data",
        open_times_reader=lambda **_kwargs: [datetime(2026, 4, 26, 23, 59, tzinfo=UTC)],
        timeframe_normalizer=lambda **_kwargs: "1m",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_ms,
        ranges_builder=lambda **_kwargs: [(start_ms, end_ms)],
        history_fetcher=lambda **_kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
    )

    assert rows == []
    assert calls == [(start_ms, end_ms)]


def _point(*, open_time: datetime) -> VolatilityPoint:
    return VolatilityPoint(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        open_time=open_time,
        close_time=open_time,
        value=1.0,
        source_endpoint="public_get_volatility_index_data",
        dataset_type="volatility_index_data",
    )


def test_fetch_symbol_volatility_rejects_non_perpetual_and_invalid_tail_setup() -> None:
    """Volatility fetches are perpetual-only and tail mode requires a lake cursor."""

    common = cast(
        Any,
        {
            "exchange": "deribit",
            "symbol": "BTC",
            "timeframe": "1m",
            "lake_root": "lake",
            "dataset_type": "volatility_index_data",
            "history_fetcher": lambda **_kwargs: [],
            "range_fetcher": lambda **_kwargs: [],
        },
    )
    assert fetch_symbol_volatility(market="spot_ohlcv", tail_delta_only=False, **common) == []
    with pytest.raises(ValueError, match="latest_open_time_reader"):
        fetch_symbol_volatility(market="perp", tail_delta_only=True, **common)


def test_fetch_symbol_volatility_tail_bootstrap_and_bounds() -> None:
    """Tail bootstrap honors an explicit bound and returns no rows for a future bound."""

    start = int(datetime(2026, 4, 27, 10, 0, tzinfo=UTC).timestamp() * 1000)
    end = start + 60_000
    calls: list[tuple[int, int]] = []

    def fetcher(**kwargs: object) -> list[VolatilityPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [_point(open_time=datetime.fromtimestamp(start / 1000, tz=UTC))]

    kwargs = {
        "exchange": "deribit",
        "market": "perp",
        "symbol": "BTC",
        "timeframe": "1m",
        "lake_root": "lake",
        "dataset_type": "volatility_index_data",
        "latest_open_time_reader": lambda **_kwargs: None,
        "history_fetcher": lambda **_kwargs: pytest.fail("history should not be used"),
        "range_fetcher": fetcher,
        "timeframe_normalizer": lambda **_kwargs: "1m",
        "interval_ms_resolver": lambda **_kwargs: 60_000,
        "now_open_resolver": lambda **_kwargs: end,
        "tail_delta_only": True,
        "start_open_ms_bound": start,
    }
    expected = [_point(open_time=datetime.fromtimestamp(start / 1000, tz=UTC))]
    assert fetch_symbol_volatility(**cast(Any, kwargs)) == expected
    assert calls == [(start, end)]
    assert fetch_symbol_volatility(**cast(Any, {**kwargs, "start_open_ms_bound": end + 60_000})) == []


def test_fetch_symbol_volatility_tail_resumes_and_returns_sorted_unique_rows() -> None:
    """Tail mode resumes after its cursor and keeps the final point for each timestamp."""

    latest_open_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    next_open_time = datetime(2026, 4, 27, 10, 1, tzinfo=UTC)
    final_open_time = datetime(2026, 4, 27, 10, 2, tzinfo=UTC)
    end_open_ms = int(final_open_time.timestamp() * 1000)
    range_calls: list[tuple[int, int]] = []
    first_duplicate = _point(open_time=next_open_time)
    final_duplicate = VolatilityPoint(
        **{**first_duplicate.__dict__, "value": 2.0},
    )

    def _range_fetcher(**kwargs: object) -> list[VolatilityPoint]:
        range_calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [_point(open_time=final_open_time), first_duplicate, final_duplicate]

    rows = fetch_symbol_volatility(
        exchange="deribit",
        market="perp",
        symbol="btc",
        timeframe="1m",
        lake_root="lake/bronze",
        dataset_type="volatility_index_data",
        timeframe_normalizer=lambda **_kwargs: "1m",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: end_open_ms,
        latest_open_time_reader=lambda **_kwargs: latest_open_time,
        history_fetcher=lambda **_kwargs: pytest.fail("history fetch must not run after a tail cursor"),
        range_fetcher=_range_fetcher,
        tail_delta_only=True,
    )

    assert range_calls == [(int(next_open_time.timestamp() * 1000), end_open_ms)]
    assert rows == [final_duplicate, _point(open_time=final_open_time)]


def test_fetch_symbol_volatility_bootstraps_when_no_rows_are_stored() -> None:
    """An empty lake starts history bootstrap with the normalized fetch contract."""

    expected = [_point(open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC))]
    history_calls: list[dict[str, object]] = []

    def _history_fetcher(**kwargs: object) -> list[VolatilityPoint]:
        history_calls.append(dict(kwargs))
        return expected

    rows = fetch_symbol_volatility(
        exchange="deribit",
        market="perp",
        symbol="btc",
        timeframe="M1",
        lake_root="lake/bronze",
        dataset_type="volatility_index_data",
        open_times_reader=lambda **_kwargs: [],
        timeframe_normalizer=lambda **_kwargs: "1m",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: 0,
        history_fetcher=_history_fetcher,
        range_fetcher=lambda **_kwargs: pytest.fail("range fetch must not run for a bootstrap"),
    )

    assert rows == expected
    assert history_calls == [
        {"exchange": "deribit", "symbol": "btc", "interval": "1m", "market": "perp", "on_history_chunk": None}
    ]


def test_fetch_symbol_volatility_skips_exchange_when_history_is_complete() -> None:
    """Existing history with no planned gaps must not issue an exchange request."""

    open_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    rows = fetch_symbol_volatility(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="1m",
        lake_root="lake/bronze",
        dataset_type="volatility_index_data",
        open_times_reader=lambda **_kwargs: [open_time],
        timeframe_normalizer=lambda **_kwargs: "1m",
        interval_ms_resolver=lambda **_kwargs: 60_000,
        now_open_resolver=lambda **_kwargs: int(open_time.timestamp() * 1000),
        ranges_builder=lambda **_kwargs: [],
        history_fetcher=lambda **_kwargs: pytest.fail("history fetch must not run when rows are stored"),
        range_fetcher=lambda **_kwargs: pytest.fail("range fetch must not run without gaps"),
    )

    assert rows == []
