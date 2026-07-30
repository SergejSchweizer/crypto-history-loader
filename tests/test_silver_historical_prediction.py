"""Tests for historical prediction Silver features."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS
from application.services.silver_service import build_historical_prediction_1m_feature_for_symbol
from tests.test_gold_regime_features import _write_silver

pl = pytest.importorskip("polars")


def test_build_historical_prediction_features_from_history_sources(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(3)]

    for dataset_type, source_symbol, base_price in (
        ("spot_ohlcv", "BTC_USDC", 100.0),
        ("perps_ohlcv", "BTC-PERPETUAL", 101.0),
    ):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol=source_symbol,
            timeframe="1m",
            rows=[
                {
                    "open_time": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "open_price": base_price + index - 0.2,
                    "high_price": base_price + index + 0.5,
                    "low_price": base_price + index - 0.5,
                    "close_price": base_price + index,
                    "volume": 100.0 + index,
                }
                for index, timestamp in enumerate(timestamps)
            ],
        )
    _write_silver(
        silver,
        dataset_type="funding_1m_feature",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        rows=[
            {
                "timestamp": timestamp,
                "exchange": "deribit",
                "symbol": "BTC-PERPETUAL",
                "funding_rate_last_known": 0.001 + index * 0.0001,
                "minutes_since_funding": index,
                "is_funding_observation_minute": index == 0,
                "funding_data_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver(
        silver,
        dataset_type="open_interest_1m_feature",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": "deribit",
                "symbol": "BTC-PERPETUAL",
                "open_interest": 1000.0 + 10.0 * index,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    for dataset_type, source_symbol in (
        ("perps_trades_1m_feature", "BTC-PERPETUAL"),
        ("options_trades_1m_feature", "BTC"),
    ):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol=source_symbol,
            timeframe="1m",
            rows=[
                {
                    "timestamp_m1": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "instrument_type": "perp" if dataset_type.startswith("perps") else "option",
                    "open_price": 100.0 + index,
                    "high_price": 101.0 + index,
                    "low_price": 99.0 + index,
                    "close_price": 100.5 + index,
                    "volume": 10.0 + index,
                    "quote_volume": 1000.0 + 100.0 * index,
                    "trade_count": 20 + index,
                    "buy_volume": 7.0 + index,
                    "sell_volume": 3.0,
                    "buy_trade_count": 12 + index,
                    "sell_trade_count": 8,
                    "buy_volume_share": 0.7,
                }
                for index, timestamp in enumerate(timestamps)
            ],
        )

    report = build_historical_prediction_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert report.dataset == "historical_prediction_1m_feature"
    assert report.rows_out == 3
    assert report.columns == SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS
    out_file = (
        silver
        / "dataset_type=historical_prediction_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    feature = pl.read_parquet(out_file).sort("timestamp_m1")
    last = feature.row(2, named=True)
    assert last["historical_prediction_perps_log_return_1m"] is not None
    assert last["historical_prediction_spot_perp_basis"] == pytest.approx((103.0 / 102.0) - 1.0)
    assert last["historical_prediction_open_interest_delta_1m"] == pytest.approx(10.0)
    assert last["historical_prediction_perps_trade_imbalance"] > 0.0
    assert last["historical_prediction_options_trade_imbalance"] > 0.0
    assert last["historical_prediction_leverage_build_up_signal"] == 1
