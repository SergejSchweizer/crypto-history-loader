"""Integration tests for the canonical historical full Gold dataset."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import gold_dataset_contract
from application.services.gold_service import build_gold_for_symbol
from tests.test_gold_regime_features import _write_required_sources_for_timestamps, _write_silver

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


def test_history_full_gold_joins_historical_sources_without_targets(tmp_path: Path) -> None:
    """The historical full dataset should be the inference-safe full historical feature table."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(20)]
    silver = tmp_path / "silver"
    _write_required_sources_for_timestamps(silver, timestamps)
    _write_trade_features(silver, timestamps)

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-history-full"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.history_full.m1",
    )
    history_full = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert history_full.height == 20
    assert "spot_ohlcv_close_price" in history_full.columns
    assert "perp_close_price" in history_full.columns
    assert "funding_rate_last_known" in history_full.columns
    assert "open_interest_open_interest" in history_full.columns
    assert "trades_close_price" in history_full.columns
    assert "options_trades_close_price" in history_full.columns
    assert "rv_1h" in history_full.columns
    assert "iv_minus_rv_1h" in history_full.columns
    assert "strategy_momentum_log_return_1m" in history_full.columns
    assert "historical_volatility_reference" in history_full.columns
    assert history_full["historical_volatility_reference"].null_count() == history_full.height
    assert not any(column.startswith(("target_", "label_")) for column in history_full.columns)
    assert manifest["dataset_id"] == "gold.market.history_full.m1"
    assert manifest["required_source_datasets"] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "perps_trades_1m_feature",
        "options_trades_1m_feature",
        "realized_volatility_1m_feature",
        "iv_rv_1m_feature",
    ]
    assert manifest["optional_source_datasets"] == [
        "historical_volatility_observed",
        "index_price_1m_feature",
        "futures_summary_1m_feature",
        "options_surface_1m_feature",
        "perps_l2_1m_feature",
        "options_l2_1m_feature",
    ]
    assert manifest["strategy_feature_lookbacks"]["strategy_momentum_log_return_1m"] == "1m"
    assert manifest["prediction_target_definitions"] == {}
    assert manifest["feature_metadata"]["trades_close_price"]["source_dataset"] == "perps_trades_1m_feature"
    assert manifest["feature_metadata"]["strategy_momentum_log_return_1m"]["source_dataset"] == (
        "gold_strategy_features"
    )


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
        "realized_volatility_1m_feature",
        "iv_rv_1m_feature",
    ]
    assert [requirement.dataset_type for requirement in contract.optional_requirements] == [
        "historical_volatility_observed",
        "index_price_1m_feature",
        "futures_summary_1m_feature",
        "options_surface_1m_feature",
        "perps_l2_1m_feature",
        "options_l2_1m_feature",
    ]
