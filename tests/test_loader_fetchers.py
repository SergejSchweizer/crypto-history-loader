"""Tests for Bronze loader symbol fetch adapters."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from api.commands.loader_fetchers import (
    BronzeSymbolFetchDependencies,
    build_symbol_fetch_dependencies,
    fetch_symbol_candles,
    serialize_candle,
)
from application.services.bronze_runtime_service import BronzeRuntimeBoundsContext
from ingestion.spot_ohlcv import SpotCandle


def test_serialize_candle_converts_datetimes_to_iso_strings() -> None:
    """Serialize command output without leaking datetime objects into JSON."""

    candle = _sample_candle()

    row = serialize_candle(candle)

    assert row["open_time"] == "2026-05-01T00:00:00+00:00"
    assert row["close_time"] == "2026-05-01T00:00:59.999000+00:00"


def test_fetch_symbol_candles_uses_runtime_start_bound() -> None:
    """Pass runtime-bound decisions through the extracted symbol fetch adapter."""

    start_ms = int(datetime(2026, 5, 1, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 5, 1, 0, 1, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _fetch_candles_range(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(kwargs["start_open_ms"]), int(kwargs["end_open_ms"])))
        return [_sample_candle()]

    dependencies = _dependencies(
        fetch_candles_range=_fetch_candles_range,
        last_closed_open_ms=lambda **_kwargs: end_ms,
    )
    runtime_context = BronzeRuntimeBoundsContext(
        tail_delta_only=False,
        global_start_open_ms=None,
        symbol_start_open_ms={"BTC": start_ms},
        exchange_symbol_start_open_ms={},
    )

    rows = fetch_symbol_candles(
        dependencies=dependencies,
        runtime_context=runtime_context,
        exchange="deribit",
        market="spot_ohlcv",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
    )

    assert rows == [_sample_candle()]
    assert calls == [(start_ms, end_ms)]


def test_build_symbol_fetch_dependencies_preserves_adapter_functions() -> None:
    """Dependency bundle construction should keep loader adapter ownership explicit."""

    dependencies = _dependencies(
        fetch_candles_range=lambda **_kwargs: [_sample_candle()],
        last_closed_open_ms=lambda **_kwargs: 1,
    )

    assert dependencies.fetch_candles_range() == [_sample_candle()]
    assert dependencies.last_closed_open_ms() == 1
    assert dependencies.normalize_funding_timeframe() == "8h"


def _sample_candle() -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )


def _dependencies(
    *,
    fetch_candles_range: Any,
    last_closed_open_ms: Any,
) -> BronzeSymbolFetchDependencies:
    return build_symbol_fetch_dependencies(
        open_times_in_lake=lambda **_kwargs: [],
        open_times_in_lake_by_dataset=lambda **_kwargs: [],
        latest_open_time_in_lake=lambda **_kwargs: None,
        latest_open_time_in_lake_by_dataset=lambda **_kwargs: None,
        normalize_storage_symbol=lambda **_kwargs: "BTCUSDT",
        interval_to_milliseconds=lambda **_kwargs: 60_000,
        open_interest_interval_to_milliseconds=lambda **_kwargs: 60_000,
        funding_interval_to_milliseconds=lambda **_kwargs: 28_800_000,
        volatility_interval_to_milliseconds=lambda **_kwargs: 60_000,
        normalize_open_interest_timeframe=lambda **_kwargs: "1m",
        normalize_funding_timeframe=lambda **_kwargs: "8h",
        normalize_volatility_timeframe=lambda **_kwargs: "1m",
        last_closed_open_ms=last_closed_open_ms,
        missing_ranges_ms=lambda **_kwargs: pytest.fail("missing range builder should not be used"),
        fetch_candles_all_history=lambda **_kwargs: pytest.fail("history fetcher should not be used"),
        fetch_candles_range=fetch_candles_range,
        fetch_open_interest_all_history=lambda **_kwargs: [],
        fetch_open_interest_range=lambda **_kwargs: [],
        fetch_funding_all_history=lambda **_kwargs: [],
        fetch_funding_range=lambda **_kwargs: [],
        fetch_volatility_index_all_history=lambda **_kwargs: [],
        fetch_volatility_index_range=lambda **_kwargs: [],
        fetch_trades_all_history=lambda **_kwargs: [],
        fetch_trades_range=lambda **_kwargs: [],
    )
