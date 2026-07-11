"""Integration tests for the Gold regime-feature dataset contract."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import gold_dataset_contract
from application.services.gold_service import (
    GoldBuildReport,
    _dataset_optional_requirements,
    _dataset_requirements,
    build_gold_for_symbol,
)

pl = pytest.importorskip("polars")


def _write_silver(
    root: Path,
    *,
    dataset_type: str,
    symbol: str,
    timeframe: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    target = (
        root
        / f"dataset_type={dataset_type}"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"{symbol}-{dataset_type}.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def _write_required_sources(silver: Path, t0: datetime, t1: datetime) -> None:
    _write_required_sources_for_timestamps(silver, [t0, t1])


def _write_required_sources_for_timestamps(
    silver: Path,
    timestamps: Sequence[datetime],
    *,
    perp_closes: Sequence[float] | None = None,
    volumes: Sequence[float] | None = None,
) -> None:
    closes = list(perp_closes or [10.5 + index for index, _timestamp in enumerate(timestamps)])
    traded_volumes = list(volumes or [100.0 + index for index, _timestamp in enumerate(timestamps)])
    for dataset_type, symbol, scale in (
        ("spot_ohlcv", "BTC_USDC", 1.0),
        ("perps_ohlcv", "BTC-PERPETUAL", 10.0),
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
                    "open_price": (closes[index] - 0.5) if dataset_type == "perps_ohlcv" else scale + index,
                    "high_price": (closes[index] + 0.5) if dataset_type == "perps_ohlcv" else scale + index + 1.0,
                    "low_price": (closes[index] - 1.0) if dataset_type == "perps_ohlcv" else scale + index - 0.5,
                    "close_price": closes[index] if dataset_type == "perps_ohlcv" else scale + index + 0.5,
                    "volume": traded_volumes[index],
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
    _write_silver(
        silver,
        dataset_type="realized_volatility_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
                "rv_5m": 20.0 + index,
                "rv_15m": 21.0 + index,
                "rv_1h": 22.0 + index,
                "rv_4h": 23.0 + index,
                "rv_1d": 24.0 + index,
                "parkinson_rv_1h": 19.0 + index,
                "jump_proxy": 0.1 * index,
                "spot_available": True,
                "perps_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver(
        silver,
        dataset_type="iv_rv_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
                "iv_minus_rv_1h": 5.0 + index,
                "iv_minus_rv_1d": 3.0 + index,
                "iv_rv_ratio_1h": 1.2,
                "iv_rv_ratio_1d": 1.1,
                "iv_rv_zscore_1d": 0.5,
                "iv_rv_percentile_30d": 0.7,
                "minutes_since_iv_observation": 0,
                "minutes_since_rv_observation": 0,
                "iv_available": True,
                "rv_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )


def _write_optional_sources(silver: Path, t0: datetime) -> None:
    _write_silver(
        silver,
        dataset_type="perps_l2_1m_feature",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": "deribit",
                "symbol": "BTC-PERPETUAL",
                "best_bid_price": 99.0,
                "best_ask_price": 101.0,
                "mid_price": 100.0,
                "spread": 2.0,
                "top_bid_size": 5.0,
                "top_ask_size": 4.0,
                "top_of_book_imbalance": 1.0 / 9.0,
                "bid_depth_10bps": 5.0,
                "ask_depth_10bps": 4.0,
                "bid_depth_50bps": 8.0,
                "ask_depth_50bps": 7.0,
                "quote_available": True,
                "quote_age_seconds": 5.0,
                "stale_quote": False,
                "minutes_since_l2_observation": 0,
            }
        ],
    )
    _write_silver(
        silver,
        dataset_type="options_l2_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_name": instrument_name,
                "spread": spread,
                "top_bid_size": 2.0,
                "top_ask_size": 3.0,
                "bid_depth_10bps": 2.0,
                "ask_depth_10bps": 3.0,
                "bid_depth_50bps": 4.0,
                "ask_depth_50bps": 5.0,
                "quote_available": available,
                "quote_age_seconds": age,
                "stale_quote": stale,
            }
            for instrument_name, spread, available, age, stale in (
                ("BTC-8MAY26-100-C", 0.1, True, 5.0, False),
                ("BTC-8MAY26-100-P", 0.2, False, 70.0, True),
            )
        ],
    )
    _write_silver(
        silver,
        dataset_type="options_surface_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": "deribit",
                "symbol": "BTC",
                "atm_iv": 50.0,
                "short_dated_iv": 55.0,
                "skew": 4.0,
                "term_structure": 3.0,
                "put_call_iv_spread": 2.0,
                "contract_count": 20,
                "fresh_quote_count": 15,
                "stale_quote_count": 5,
                "max_quote_age_seconds": 70.0,
                "quote_coverage_ratio": 0.75,
            }
        ],
    )
    _write_silver(
        silver,
        dataset_type="index_price_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": "deribit",
                "symbol": "BTC",
                "index_price": 100.0,
                "index_price_is_observed": True,
                "minutes_since_index_price_observation": 0,
            }
        ],
    )
    _write_silver(
        silver,
        dataset_type="futures_summary_1m_feature",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": "deribit",
                "symbol": "BTC-PERPETUAL",
                "instrument_type": "future",
                "mark_price": 101.0,
                "index_price": 100.0,
                "mark_index_spread": 1.0,
                "mark_index_ratio": 1.01,
                "open_interest": 1000.0,
                "volume": 25.0,
                "turnover": 2525.0,
                "funding_rate": 0.0001,
                "summary_is_observed": True,
                "minutes_since_summary_observation": 0,
            }
        ],
    )
    _write_silver(
        silver,
        dataset_type="historical_volatility_observed",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp": t0,
                "exchange": "deribit",
                "symbol": "BTC",
                "historical_volatility": 30.0,
                "historical_volatility_source_timestamp": t0,
            }
        ],
    )


def _manifest(report: GoldBuildReport) -> dict[str, object]:
    path = report.manifest_path
    assert isinstance(path, str)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_regime_gold_contract_is_stable_with_and_without_optional_sources(tmp_path: Path) -> None:
    """Optional regime inputs should change values and audit state, not rows or schema."""

    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)
    _write_required_sources(silver, t0, t1)

    missing_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-missing"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.regime_features.m1",
    )
    missing = pl.read_parquet(missing_report.parquet_path).sort("timestamp_m1")
    missing_manifest = _manifest(missing_report)

    _write_optional_sources(silver, t0)
    present_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-present"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.regime_features.m1",
    )
    repeated_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-repeated"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.regime_features.m1",
    )
    present = pl.read_parquet(present_report.parquet_path).sort("timestamp_m1")
    repeated = pl.read_parquet(repeated_report.parquet_path).sort("timestamp_m1")
    present_manifest = _manifest(present_report)

    assert missing.height == present.height == repeated.height == 2
    assert missing.columns == present.columns == repeated.columns
    assert missing_report.feature_set_hash == present_report.feature_set_hash
    assert present_report.feature_set_hash == repeated_report.feature_set_hash
    assert present_report.source_data_hash == repeated_report.source_data_hash
    assert present.equals(repeated)
    optional_columns = [
        column
        for column in missing.columns
        if column.startswith(
            (
                "perps_l2_",
                "options_l2_",
                "options_surface_",
                "index_price",
                "futures_summary_",
                "historical_",
            )
        )
        or column == "minutes_since_summary_observation"
    ]
    assert optional_columns
    assert all(missing[column].null_count() == missing.height for column in optional_columns)
    assert present["options_surface_atm_iv"].to_list() == [50.0, None]
    assert present["options_l2_contract_count"].to_list() == [2, None]
    assert present["options_l2_quote_coverage_ratio"].to_list() == [0.5, None]
    assert present["futures_summary_mark_price"].to_list() == [101.0, None]
    assert present["minutes_since_summary_observation"].to_list() == [0, None]
    assert present["historical_volatility_reference"].to_list() == [30.0, None]

    required = [dataset for dataset, _timeframe in _dataset_requirements("gold.market.regime_features.m1")]
    optional = [dataset for dataset, _timeframe in _dataset_optional_requirements("gold.market.regime_features.m1")]
    assert missing_manifest["required_source_datasets"] == required
    assert missing_manifest["optional_source_datasets"] == optional
    missing_availability = missing_manifest["optional_source_availability"]
    present_availability = present_manifest["optional_source_availability"]
    assert all(not missing_availability[dataset]["available"] for dataset in optional)
    assert all(present_availability[dataset]["available"] for dataset in optional)
    assert all(present_availability[dataset]["grid_coverage_ratio"] == 0.5 for dataset in optional)
    assert all(present_availability[dataset]["freshness_minutes_at_grid_end"] == 1.0 for dataset in optional)
    assert not any(
        token in column.lower()
        for column in present.columns
        for token in ("label", "target", "future_return", "prediction")
    )
    assert present_manifest["feature_metadata"]["rv_1h"]["source_dataset"] == ("realized_volatility_1m_feature")
    assert present_manifest["feature_metadata"]["options_surface_atm_iv"]["source_dataset"] == (
        "options_surface_1m_feature"
    )


def test_regime_gold_required_source_gap_fails_loudly(tmp_path: Path) -> None:
    """Missing required regime inputs should fail before emitting a partial feature artifact."""

    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)
    _write_required_sources(silver, t0, t1)
    iv_rv_root = silver / "dataset_type=iv_rv_1m_feature"
    for path in iv_rv_root.rglob("*.parquet"):
        path.unlink()

    with pytest.raises(ValueError, match="Missing silver dataset for symbol=BTC: iv_rv_1m_feature"):
        build_gold_for_symbol(
            silver_root=str(silver),
            gold_root=str(tmp_path / "gold"),
            exchange="deribit",
            symbol="BTC",
            dataset_id="gold.market.regime_features.m1",
        )


def test_regime_gold_strategy_features_are_trailing_state_variables(tmp_path: Path) -> None:
    """Strategy features should be declared, stable on warm-up, and independent of future rows."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(20)]
    base_silver = tmp_path / "silver-base"
    changed_silver = tmp_path / "silver-changed"
    constant_silver = tmp_path / "silver-constant"
    base_prices = [
        100.0,
        101.0,
        102.0,
        101.0,
        103.0,
        104.0,
        105.0,
        104.0,
        106.0,
        107.0,
        108.0,
        107.0,
        109.0,
        110.0,
        111.0,
        112.0,
        111.0,
        113.0,
        114.0,
        115.0,
    ]
    changed_prices = [*base_prices[:-1], 130.0]
    _write_required_sources_for_timestamps(base_silver, timestamps, perp_closes=base_prices)
    _write_required_sources_for_timestamps(changed_silver, timestamps, perp_closes=changed_prices)
    _write_required_sources_for_timestamps(
        constant_silver,
        timestamps,
        perp_closes=[100.0 for _timestamp in timestamps],
        volumes=[0.0 for _timestamp in timestamps],
    )

    base_report = build_gold_for_symbol(
        silver_root=str(base_silver),
        gold_root=str(tmp_path / "gold-base"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.regime_features.m1",
    )
    changed_report = build_gold_for_symbol(
        silver_root=str(changed_silver),
        gold_root=str(tmp_path / "gold-changed"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.regime_features.m1",
    )
    constant_report = build_gold_for_symbol(
        silver_root=str(constant_silver),
        gold_root=str(tmp_path / "gold-constant"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.regime_features.m1",
    )

    base = pl.read_parquet(base_report.parquet_path).sort("timestamp_m1")
    changed = pl.read_parquet(changed_report.parquet_path).sort("timestamp_m1")
    constant = pl.read_parquet(constant_report.parquet_path).sort("timestamp_m1")
    base_manifest = _manifest(base_report)
    lookbacks = base_manifest["strategy_feature_lookbacks"]
    strategy_columns = [column for column in base.columns if column.startswith("strategy_")]

    assert strategy_columns
    assert set(strategy_columns) == set(lookbacks)
    assert lookbacks["strategy_momentum_log_return_5m"] == "5m"
    assert lookbacks["strategy_momentum_log_return_15m"] == "15m"
    assert lookbacks["strategy_trend_ema_slope_10m"] == "10m"
    assert lookbacks["strategy_reversion_vwap_distance_5m"] == "5m"
    assert lookbacks["strategy_reversion_vwap_distance_15m"] == "15m"
    assert not any(
        token in column.lower()
        for column in strategy_columns
        for token in ("label", "target", "future_return", "prediction")
    )
    assert base["strategy_momentum_log_return_1m"].to_list()[0] is None
    assert base["strategy_momentum_log_return_5m"].to_list()[:5] == [None, None, None, None, None]
    assert base["strategy_momentum_log_return_15m"].to_list()[:15] == [None for _index in range(15)]
    assert base["strategy_cost_turnover_notional_1m"].to_list()[1] == pytest.approx(101.0 * 101.0)
    assert base["strategy_cost_turnover_notional_5m"].to_list()[1] == pytest.approx((100.0 * 100.0) + (101.0 * 101.0))
    assert base["strategy_cost_turnover_notional_15m"].to_list()[1] == pytest.approx((100.0 * 100.0) + (101.0 * 101.0))
    assert base["strategy_reversion_vwap_distance_5m"].null_count() < base.height
    assert base["strategy_reversion_vwap_distance_15m"].null_count() < base.height
    assert base["strategy_trend_ema_slope_10m"].null_count() < base.height
    assert base["strategy_trend_persistence_15m"].null_count() < base.height
    assert constant["strategy_momentum_log_return_1m"].to_list()[1:] == [0.0 for _timestamp in timestamps[1:]]
    assert constant["strategy_momentum_log_return_15m"].to_list()[15:] == [0.0 for _timestamp in timestamps[15:]]
    assert constant["strategy_reversion_price_zscore_5m"].null_count() == constant.height
    assert constant["strategy_reversion_price_zscore_15m"].null_count() == constant.height
    assert constant["strategy_reversion_vwap_distance_5m"].null_count() == constant.height
    assert constant["strategy_reversion_vwap_distance_15m"].null_count() == constant.height

    comparable_columns = ["timestamp_m1", *strategy_columns]
    assert base.select(comparable_columns).head(19).equals(changed.select(comparable_columns).head(19))
    assert base_manifest["feature_metadata"]["strategy_momentum_log_return_1m"]["source_dataset"] == (
        "gold_strategy_features"
    )


def test_regime_gold_contract_declares_exact_required_and_optional_sources() -> None:
    """The typed regime contract should match the backlog source policy exactly."""

    contract = gold_dataset_contract("gold.market.regime_features.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "realized_volatility_1m_feature",
        "iv_rv_1m_feature",
    ]
    assert [requirement.dataset_type for requirement in contract.optional_requirements] == [
        "perps_l2_1m_feature",
        "options_l2_1m_feature",
        "options_surface_1m_feature",
        "index_price_1m_feature",
        "futures_summary_1m_feature",
        "historical_volatility_observed",
    ]
