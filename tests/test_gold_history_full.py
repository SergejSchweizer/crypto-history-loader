"""Integration tests for the canonical historical full Gold dataset."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import gold_dataset_contract
from application.services.gold_service import build_gold_for_symbol
from tests.test_gold_regime_features import _write_silver

pl = pytest.importorskip("polars")


def _manifest(path: str | None) -> dict[str, object]:
    assert path is not None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_trade_features(silver: Path, timestamps: list[datetime]) -> None:
    for dataset_type in ("perps_trades_1m_feature", "options_trades_1m_feature"):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol="BTC-PERPETUAL" if dataset_type == "perps_trades_1m_feature" else "BTC",
            timeframe="1m",
            rows=[
                {
                    "timestamp_m1": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "instrument_type": "perpetual" if dataset_type == "perps_trades_1m_feature" else "option",
                    "open_price": 100.0 + index,
                    "high_price": 101.0 + index,
                    "low_price": 99.0 + index,
                    "close_price": 100.5 + index,
                    "volume": 10.0 + index,
                    "quote_volume": 1000.0 + index,
                    "trade_count": 20 + index,
                    "buy_volume": 6.0 + index,
                    "sell_volume": 4.0,
                    "buy_trade_count": 12 + index,
                    "sell_trade_count": 8,
                    "buy_volume_share": 0.6,
                }
                for index, timestamp in enumerate(timestamps)
            ],
        )


def _write_raw_history_sources(silver: Path, timestamps: list[datetime]) -> None:
    """Write only the Silver outputs backed by raw Bronze datasets fetched by this repository."""

    spot_timestamps = timestamps[1:]
    for dataset_type, symbol, source_timestamps, scale in (
        ("spot_ohlcv", "BTC_USDC", spot_timestamps, 1.0),
        ("perps_ohlcv", "BTC-PERPETUAL", timestamps, 10.0),
    ):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol=symbol,
            timeframe="1m",
            rows=[
                {
                    "open_time": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "open_price": scale + index,
                    "high_price": scale + index + 1.0,
                    "low_price": scale + index - 0.5,
                    "close_price": scale + index + 0.5,
                    "volume": 100.0 + index,
                }
                for index, timestamp in enumerate(source_timestamps)
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
                "funding_rate_last_known": 0.001,
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
                "open_interest": 1000.0 + index,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_trade_features(silver, timestamps)


def test_history_full_gold_joins_historical_sources_without_targets(tmp_path: Path) -> None:
    """The historical full dataset should contain only raw-Bronze-backed historical families."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(2)]
    silver = tmp_path / "silver"
    _write_raw_history_sources(silver, timestamps)

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-history-full"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.history_full.m1",
    )
    history_full = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert history_full.height == 2
    assert history_full["timestamp_m1"].to_list() == timestamps
    assert "spot_ohlcv_close_price" in history_full.columns
    assert "perp_close_price" in history_full.columns
    assert "funding_rate_last_known" in history_full.columns
    assert "open_interest_open_interest" in history_full.columns
    assert "perps_trades_close_price" in history_full.columns
    assert "options_trades_close_price" in history_full.columns
    assert history_full.columns == [
        "timestamp_m1",
        "exchange",
        "symbol",
        "spot_ohlcv_open_price",
        "spot_ohlcv_high_price",
        "spot_ohlcv_low_price",
        "spot_ohlcv_close_price",
        "spot_ohlcv_volume",
        "spot_ohlcv_quote_volume",
        "spot_ohlcv_trade_count",
        "perp_open_price",
        "perp_high_price",
        "perp_low_price",
        "perp_close_price",
        "perp_volume",
        "perp_quote_volume",
        "perp_trade_count",
        "funding_rate_last_known",
        "funding_observed_at",
        "minutes_since_funding",
        "is_funding_observation_minute",
        "funding_data_available",
        "open_interest_open_interest",
        "open_interest_is_observed",
        "open_interest_is_ffill",
        "minutes_since_open_interest_observation",
        "open_interest_observation_lag_sec",
        "open_interest_source_timestamp",
        "perps_trades_open_price",
        "perps_trades_high_price",
        "perps_trades_low_price",
        "perps_trades_close_price",
        "perps_trades_volume",
        "perps_trades_quote_volume",
        "perps_trades_trade_count",
        "perps_trades_buy_volume",
        "perps_trades_sell_volume",
        "perps_trades_buy_trade_count",
        "perps_trades_sell_trade_count",
        "perps_trades_buy_volume_share",
        "options_trades_open_price",
        "options_trades_high_price",
        "options_trades_low_price",
        "options_trades_close_price",
        "options_trades_volume",
        "options_trades_quote_volume",
        "options_trades_trade_count",
        "options_trades_buy_volume",
        "options_trades_sell_volume",
        "options_trades_buy_trade_count",
        "options_trades_sell_trade_count",
        "options_trades_buy_volume_share",
    ]
    assert "rv_1h" not in history_full.columns
    assert "iv_minus_rv_1h" not in history_full.columns
    assert "strategy_momentum_log_return_1m" not in history_full.columns
    assert "strategy_reversion_half_life_5m" not in history_full.columns
    assert "historical_volatility_reference" not in history_full.columns
    assert "perps_trades_buy_volume_share" in history_full.columns
    assert "options_trades_sell_trade_count" in history_full.columns
    assert "minutes_since_open_interest_observation" in history_full.columns
    assert "funding_data_available" in history_full.columns
    assert history_full["spot_ohlcv_close_price"].to_list()[0] is None
    assert not any(column.startswith(("target_", "label_")) for column in history_full.columns)
    assert manifest["dataset_id"] == "gold.market.history_full.m1"
    assert manifest["required_source_datasets"] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "perps_trades_1m_feature",
        "options_trades_1m_feature",
    ]
    assert manifest["optional_source_datasets"] == []
    assert manifest["strategy_feature_lookbacks"] == {}
    assert manifest["prediction_target_definitions"] == {}
    assert manifest["feature_metadata"]["perps_trades_close_price"]["source_dataset"] == "perps_trades_1m_feature"


def test_history_full_gold_contract_declares_canonical_historical_sources() -> None:
    """The typed historical full contract should declare historical sources explicitly."""

    contract = gold_dataset_contract("gold.market.history_full.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "perps_trades_1m_feature",
        "options_trades_1m_feature",
    ]
    assert contract.optional_requirements == ()
