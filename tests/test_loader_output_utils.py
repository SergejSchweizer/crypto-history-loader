"""Tests for Bronze loader output serialization utilities."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from api.commands.loader_output_utils import serialize_candle, sidecar_path_list
from ingestion.spot_ohlcv import SpotCandle


def test_sidecar_path_list_deduplicates_sorts_and_replaces_suffix(tmp_path: Path) -> None:
    first = tmp_path / "b" / "data.parquet"
    second = tmp_path / "a" / "data.parquet"

    paths = sidecar_path_list(
        [str(first), str(second), str(first)],
        ".manifest.json",
    )

    assert paths == sorted(
        [
            str(first.with_suffix(".manifest.json").resolve()),
            str(second.with_suffix(".manifest.json").resolve()),
        ]
    )


def test_serialize_candle_converts_datetime_fields_to_iso_strings() -> None:
    candle = SpotCandle(
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

    row = serialize_candle(candle)

    assert row["open_time"] == "2026-05-01T00:00:00+00:00"
    assert row["close_time"] == "2026-05-01T00:00:59.999000+00:00"
    assert row["symbol"] == "BTCUSDT"
