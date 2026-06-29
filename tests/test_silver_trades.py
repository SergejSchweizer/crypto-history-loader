"""Tests for Silver trade-family frame transformations."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from application.services.silver_trades import build_trade_feature_frame, build_trade_observed_frame

pl = pytest.importorskip("polars")


def test_build_trade_observed_frame_filters_invalid_and_deduplicates() -> None:
    ts = datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC)
    frame = pl.DataFrame(
        [
            {
                "open_time": ts,
                "ingested_at": datetime(2026, 5, 1, 0, 0, 11, tzinfo=UTC),
                "price": 100.0,
                "quantity": 1.0,
                "trade_id": "dup",
                "side": "BUY",
                "symbol": "BTC-PERPETUAL",
                "exchange": "DERIBIT",
                "instrument_type": "PERP",
            },
            {
                "open_time": ts,
                "ingested_at": datetime(2026, 5, 1, 0, 0, 12, tzinfo=UTC),
                "price": 101.0,
                "quantity": 1.0,
                "trade_id": "dup",
                "side": "buy",
                "symbol": "BTC-PERPETUAL",
                "exchange": "deribit",
                "instrument_type": "perp",
            },
            {
                "open_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
                "ingested_at": datetime(2026, 5, 1, 0, 0, 21, tzinfo=UTC),
                "price": -1.0,
                "quantity": 1.0,
                "trade_id": "bad",
                "side": "sell",
                "symbol": "BTC-PERPETUAL",
                "exchange": "deribit",
                "instrument_type": "perp",
            },
        ]
    )

    observed, invalid_rows, cleaned_rows = build_trade_observed_frame(pl, frame)

    assert invalid_rows == 1
    assert cleaned_rows == 2
    assert observed.height == 1
    row = observed.row(0, named=True)
    assert row["price"] == 101.0
    assert row["exchange"] == "deribit"
    assert row["instrument_type"] == "perp"


def test_build_trade_feature_frame_aggregates_minute_flow_features() -> None:
    frame = pl.DataFrame(
        [
            {
                "trade_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_type": "perp",
                "price": 100.0,
                "quantity": 2.0,
                "side": "buy",
            },
            {
                "trade_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_type": "perp",
                "price": 101.0,
                "quantity": 1.0,
                "side": "sell",
            },
        ]
    )

    feature = build_trade_feature_frame(pl, frame, symbol="BTC")

    assert feature.height == 1
    row = feature.row(0, named=True)
    assert row["open_price"] == 100.0
    assert row["close_price"] == 101.0
    assert row["volume"] == 3.0
    assert row["quote_volume"] == 301.0
    assert row["buy_volume"] == 2.0
    assert row["sell_volume"] == 1.0
    assert row["buy_trade_count"] == 1
    assert row["sell_trade_count"] == 1
    assert row["buy_volume_share"] == pytest.approx(2.0 / 3.0)
