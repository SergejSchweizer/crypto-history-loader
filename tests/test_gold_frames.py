"""Tests for Gold frame discovery, preparation, and validation helpers."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import GOLD_DATASET_CONTRACTS
from application.services import gold_frames

pl = pytest.importorskip("polars")


def test_normalize_symbol_and_discover_symbols_for_dataset(tmp_path: Path) -> None:
    assert gold_frames.normalize_symbol("btc/usdc") == "BTC"
    assert gold_frames.normalize_symbol("eth-perpetual") == "ETH"

    root = tmp_path / "silver"
    (root / "dataset_type=spot_ohlcv" / "exchange=deribit" / "symbol=BTC-PERPETUAL" / "timeframe=1m").mkdir(
        parents=True
    )
    (root / "dataset_type=spot_ohlcv" / "exchange=deribit" / "symbol=ETH-PERPETUAL" / "timeframe=5m").mkdir(
        parents=True
    )

    assert gold_frames.discover_symbols_for_dataset(
        silver_root=str(root),
        exchange="deribit",
        dataset_type="spot_ohlcv",
        timeframe="1m",
    ) == {"BTC"}


def test_read_dataset_frame_loads_all_matching_symbol_partitions(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    symbol_root = root / "dataset_type=spot_ohlcv" / "exchange=deribit" / "symbol=BTC-PERPETUAL" / "timeframe=1m"
    symbol_root.mkdir(parents=True)
    first_file = symbol_root / "first.parquet"
    second_file = symbol_root / "second.parquet"
    pl.DataFrame(
        {
            "open_time": [
                datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
                datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            ],
            "value": [2, 1],
        }
    ).write_parquet(first_file)
    pl.DataFrame(
        {
            "open_time": [datetime(2026, 5, 1, 0, 2, tzinfo=UTC)],
            "value": [3],
        }
    ).write_parquet(second_file)

    frame = gold_frames.read_dataset_frame(
        silver_root=str(root),
        exchange="deribit",
        symbol="BTC",
        dataset_type="spot_ohlcv",
        timeframe="1m",
    )

    assert frame.get_column("value").to_list() == [1, 2, 3]


def test_prepare_open_interest_accepts_current_oi_feature_aliases() -> None:
    timestamp = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    frame = pl.DataFrame(
        {
            "timestamp_m1": [timestamp],
            "exchange": ["deribit"],
            "symbol": ["BTC-PERPETUAL"],
            "open_interest": [1000.0],
            "oi_is_observed": [True],
            "oi_is_ffill": [False],
            "minutes_since_oi_observation": [0],
            "oi_observation_lag_sec": [3],
        }
    )

    prepared = gold_frames.prepare_open_interest(pl, frame, "BTC")

    assert prepared.select(
        [
            "symbol",
            "open_interest_open_interest",
            "open_interest_is_observed",
            "open_interest_is_ffill",
            "minutes_since_open_interest_observation",
            "open_interest_observation_lag_sec",
        ]
    ).to_dicts() == [
        {
            "symbol": "BTC",
            "open_interest_open_interest": 1000.0,
            "open_interest_is_observed": True,
            "open_interest_is_ffill": False,
            "minutes_since_open_interest_observation": 0,
            "open_interest_observation_lag_sec": 3,
        }
    ]


def test_prepare_options_snapshot_aggregates_contract_rows_to_unique_minutes() -> None:
    """Raw live option contracts must not multiply the Gold minute-grid join."""

    timestamp = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    frame = pl.DataFrame(
        {
            "timestamp": [timestamp, timestamp, timestamp],
            "exchange": ["deribit"] * 3,
            "symbol": ["BTC"] * 3,
            "instrument_name": ["BTC-C", "BTC-P", "BTC-C"],
            "implied_volatility": [0.50, 0.60, 0.55],
            "mark_price": [1.0, 1.1, 1.2],
            "bid_price": [0.9, None, 1.1],
            "ask_price": [1.1, None, 1.3],
            "ingested_at": [
                timestamp + timedelta(seconds=1),
                timestamp + timedelta(seconds=2),
                timestamp + timedelta(seconds=3),
            ],
        }
    )

    prepared = gold_frames.prepare_options_snapshot(pl, frame, "BTC")

    assert prepared.height == 1
    assert prepared["options_surface_contract_count"].to_list() == [2]
    assert prepared["options_surface_fresh_quote_count"].to_list() == [2]
    assert prepared["options_surface_quote_coverage_ratio"].to_list() == [pytest.approx(2 / 3)]


def test_prepare_recent_trade_snapshot_deduplicates_tick_rows() -> None:
    """Multiple live executions in one minute must produce one lineage key."""

    timestamp = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    frame = pl.DataFrame(
        {
            "trade_time": [timestamp, timestamp],
            "exchange": ["deribit", "deribit"],
            "symbol": ["BTC", "BTC"],
        }
    )

    prepared = gold_frames.prepare_recent_trade_snapshot(pl, frame, "BTC")

    assert prepared.height == 1


def test_prepare_raw_l2_snapshots_normalizes_timestamp_and_aggregates_contracts() -> None:
    """Live observed L2 schemas should be usable by Gold without feature columns."""

    timestamp = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    common = {
        "timestamp": timestamp,
        "exchange": "deribit",
        "symbol": "BTC",
        "instrument_type": "option",
        "underlying": "BTC",
        "expiry": None,
        "strike": None,
        "option_type": None,
        "best_bid_price": 99.0,
        "best_bid_size": 5.0,
        "best_ask_price": 101.0,
        "best_ask_size": 4.0,
    }
    raw_options = pl.DataFrame(
        [
            {**common, "instrument_name": "BTC-C"},
            {**common, "instrument_name": "BTC-P"},
        ]
    )

    prepared_perps = gold_frames.prepare_perps_l2_snapshot(
        pl,
        raw_options.with_columns(pl.lit("BTC-PERPETUAL").alias("instrument_name")),
        "BTC",
    )
    prepared_options = gold_frames.prepare_options_l2_snapshot(pl, raw_options, "BTC")

    assert prepared_perps.height == 1
    assert prepared_perps["timestamp_m1"].to_list() == [timestamp]
    assert prepared_options.height == 1
    assert prepared_options["options_l2_contract_count"].to_list() == [2]


def test_resample_history_full_frame_aggregates_buckets() -> None:
    timestamps = [datetime(2026, 1, 1, 0, minute, tzinfo=UTC) for minute in range(6)]
    frame = pl.DataFrame(
        {
            "timestamp_m1": timestamps,
            "exchange": ["deribit"] * 6,
            "symbol": ["BTC"] * 6,
            "spot_ohlcv_open_price": [100.0 + minute for minute in range(6)],
            "spot_ohlcv_high_price": [101.0 + minute for minute in range(6)],
            "spot_ohlcv_low_price": [99.0 + minute for minute in range(6)],
            "spot_ohlcv_close_price": [100.5 + minute for minute in range(6)],
            "spot_ohlcv_volume": [10.0 + minute for minute in range(6)],
            "perps_trades_open_price": [200.0 + minute for minute in range(6)],
            "perps_trades_high_price": [201.0 + minute for minute in range(6)],
            "perps_trades_low_price": [199.0 + minute for minute in range(6)],
            "perps_trades_close_price": [200.5 + minute for minute in range(6)],
            "perps_trades_volume": [20.0 + minute for minute in range(6)],
            "perps_trades_buy_volume": [0.6 * (20.0 + minute) for minute in range(6)],
            "perps_trades_buy_volume_share": [0.6] * 6,
            "funding_rate_last_known": [0.001] * 6,
            "funding_data_available": [True] * 6,
            "is_funding_observation_minute": [minute == 0 for minute in range(6)],
            "open_interest_open_interest": [1000.0 + minute for minute in range(6)],
            "open_interest_is_observed": [True] * 6,
            "open_interest_is_ffill": [False] * 6,
        }
    )

    resampled = gold_frames.resample_history_full_frame(pl, frame, "5m")

    assert resampled.height == 2
    assert resampled["timestamp_m1"].to_list() == [
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
    ]
    assert resampled["spot_ohlcv_open_price"].to_list() == [100.0, 105.0]
    assert resampled["spot_ohlcv_high_price"].to_list() == [105.0, 106.0]
    assert resampled["spot_ohlcv_low_price"].to_list() == [99.0, 104.0]
    assert resampled["spot_ohlcv_close_price"].to_list() == [104.5, 105.5]
    assert resampled["spot_ohlcv_volume"].to_list() == [60.0, 15.0]
    assert resampled["perps_trades_buy_volume_share"].to_list() == [pytest.approx(0.6), pytest.approx(0.6)]
    assert resampled["funding_data_available"].to_list() == [True, True]
    assert resampled["open_interest_open_interest"].to_list() == [1004.0, 1005.0]


@pytest.mark.parametrize(
    ("interval", "expected_rows"),
    [
        ("5m", 12),
        ("30m", 2),
        ("1h", 1),
    ],
)
def test_resample_history_full_frame_supports_coarser_buckets(interval: str, expected_rows: int) -> None:
    timestamps = [datetime(2026, 1, 1, 0, minute, tzinfo=UTC) for minute in range(60)]
    frame = pl.DataFrame(
        {
            "timestamp_m1": timestamps,
            "exchange": ["deribit"] * 60,
            "symbol": ["BTC"] * 60,
            "spot_ohlcv_open_price": [100.0 + minute for minute in range(60)],
            "spot_ohlcv_high_price": [101.0 + minute for minute in range(60)],
            "spot_ohlcv_low_price": [99.0 + minute for minute in range(60)],
            "spot_ohlcv_close_price": [100.5 + minute for minute in range(60)],
            "spot_ohlcv_volume": [10.0 + minute for minute in range(60)],
        }
    )

    resampled = gold_frames.resample_history_full_frame(pl, frame, interval)

    assert resampled.height == expected_rows


@pytest.mark.parametrize(
    ("dataset_id", "expected_interval"),
    [
        ("gold.history.full.m5", "5m"),
        ("gold.history.full.m30", "30m"),
        ("gold.history.full.h1", "1h"),
        ("gold.history.extended.m5", "5m"),
        ("gold.history.extended.m30", "30m"),
        ("gold.history.extended.h1", "1h"),
    ],
)
def test_history_full_resample_interval_supports_extended_aliases(
    dataset_id: str,
    expected_interval: str,
) -> None:
    """History-full resample helper should map both canonical and extended aliases."""

    assert gold_frames.history_full_resample_interval(dataset_id) == expected_interval


def test_read_latest_l2_gold_frame_supports_nested_and_flat_layouts(tmp_path: Path) -> None:
    nested = (
        tmp_path
        / "dataset_id=gold.l2.micro.m1"
        / "exchange=deribit"
        / "symbol=BTC"
        / "version=v1.0.0"
        / "build_id=test"
        / "data.parquet"
    )
    nested.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(nested)

    frame, path = gold_frames.read_latest_l2_gold_frame(
        l2_root=str(tmp_path),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
    )

    assert path == nested
    assert frame.get_column("value").to_list() == [1]

    flat_root = tmp_path / "flat"
    flat_root.mkdir()
    flat = flat_root / "BTC_L2_abc_def.parquet"
    pl.DataFrame({"value": [3]}).write_parquet(flat)

    flat_frame, flat_path = gold_frames.read_latest_l2_gold_frame(
        l2_root=str(flat_root),
        exchange="deribit",
        symbol="BTC",
    )

    assert flat_path == flat
    assert flat_frame.get_column("value").to_list() == [3]


def test_validate_or_filter_l2_quality_strict_and_lenient_modes() -> None:
    frame = pl.DataFrame(
        {
            "l2_coverage_ratio": [0.5, 1.2],
            "l2_snapshot_count": [1, 1],
        }
    )

    with pytest.raises(ValueError, match="invalid rows"):
        gold_frames.validate_or_filter_l2_quality(pl, frame, "strict")

    filtered, audit = gold_frames.validate_or_filter_l2_quality(pl, frame, "lenient")

    assert filtered.height == 1
    assert audit == {"l2_invalid_rows_found": 1, "l2_invalid_rows_dropped": 1}


def test_prepare_dataset_frame_and_minute_grid() -> None:
    timestamps = [
        datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 2, tzinfo=UTC),
    ]
    raw = pl.DataFrame(
        {
            "open_time": timestamps,
            "exchange": ["deribit", "deribit"],
            "open_price": [1.0, 2.0],
            "high_price": [1.0, 2.0],
            "low_price": [1.0, 2.0],
            "close_price": [1.0, 2.0],
            "volume": [10.0, 20.0],
        }
    )

    prepared = gold_frames.prepare_dataset_frame(pl, "spot_ohlcv", raw, "BTC")
    grid = gold_frames.build_minute_grid(pl, [prepared], "deribit", "BTC")

    assert prepared.columns == [
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
    ]
    assert prepared.select(["spot_ohlcv_quote_volume", "spot_ohlcv_trade_count"]).to_dicts() == [
        {"spot_ohlcv_quote_volume": None, "spot_ohlcv_trade_count": None},
        {"spot_ohlcv_quote_volume": None, "spot_ohlcv_trade_count": None},
    ]
    assert grid.height == 3
    assert grid.get_column("symbol").to_list() == ["BTC", "BTC", "BTC"]


def test_gold_contract_requirements_have_preparation_specs() -> None:
    registered = set(gold_frames.gold_frame_preparation_specs())
    contract_sources = {
        requirement.dataset_type
        for contract in GOLD_DATASET_CONTRACTS.values()
        for requirement in (*contract.requirements, *contract.optional_requirements)
    }

    assert contract_sources <= registered


def test_gold_frame_validation_and_empty_input_paths(tmp_path: Path) -> None:
    """Frame helpers should reject incomplete inputs and report absent artifacts clearly."""

    assert gold_frames.normalize_symbol("") == ""
    assert (
        gold_frames.discover_symbols_for_dataset(
            silver_root=str(tmp_path / "missing"),
            exchange="deribit",
            dataset_type="perps_ohlcv",
            timeframe="1m",
        )
        == set()
    )
    with pytest.raises(ValueError, match="Missing silver dataset"):
        gold_frames.read_dataset_frame(
            silver_root=str(tmp_path), exchange="deribit", symbol="BTC", dataset_type="spot_ohlcv", timeframe="1m"
        )
    with pytest.raises(ValueError, match="Missing L2 parquet"):
        gold_frames.read_latest_l2_gold_frame(l2_root=str(tmp_path), exchange="deribit", symbol="BTC")
    with pytest.raises(ValueError, match="missing required"):
        gold_frames.prepare_l2(pl, pl.DataFrame({"value": [1]}), "BTC")
    with pytest.raises(ValueError, match="Unsupported l2_validation_mode"):
        gold_frames.validate_or_filter_l2_quality(pl, pl.DataFrame({"l2_snapshot_count": [1]}), "bad")
    with pytest.raises(ValueError, match="no supported"):
        gold_frames.validate_or_filter_l2_quality(pl, pl.DataFrame({"value": [1]}), "strict")
    with pytest.raises(ValueError, match="Unsupported optional"):
        gold_frames.optional_feature_schema(pl, "unknown")
    with pytest.raises(ValueError, match="Unsupported dataset_type"):
        gold_frames.prepare_dataset_frame(pl, "unknown", pl.DataFrame(), "BTC")
    with pytest.raises(ValueError, match="No timestamp coverage"):
        gold_frames.build_minute_grid(pl, [pl.DataFrame()], "deribit", "BTC")


def test_gold_frame_optional_columns_and_resample_edge_paths() -> None:
    """Optional feature defaults and one-minute resampling preserve the explicit contract."""

    timestamp = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    index = gold_frames.prepare_index_price(
        pl,
        pl.DataFrame({"timestamp": [timestamp], "exchange": ["deribit"], "index_price": [100.0]}),
        "BTC",
    )
    assert index.columns == [
        "timestamp_m1",
        "exchange",
        "symbol",
        "index_price",
        "index_price_is_observed",
        "minutes_since_index_price_observation",
    ]
    with pytest.raises(ValueError, match="Unsupported history_full"):
        gold_frames.resample_history_full_frame(pl, index, "2m")
    with pytest.raises(ValueError, match="requires timestamp"):
        gold_frames.resample_history_full_frame(pl, pl.DataFrame({"value": [1]}), "5m")
    assert gold_frames.resample_history_full_frame(pl, index, "1m").height == 1


def test_gold_target_helpers_cover_grouping_and_incomplete_future_windows() -> None:
    """Prediction helpers keep groups separate and leave incomplete horizons null."""

    rows = [
        {
            "exchange": "deribit",
            "symbol": "BTC",
            "perp_close_price": 100.0,
            "iv_minus_rv_1h": 0.1,
            "iv_rv_zscore_1d": 0.0,
            "funding_rate_last_known": 0.01,
            "rv_1h": 0.2,
        },
        {
            "exchange": "deribit",
            "symbol": "BTC",
            "perp_close_price": 101.0,
            "iv_minus_rv_1h": 0.2,
            "iv_rv_zscore_1d": 2.0,
            "funding_rate_last_known": 0.0,
            "rv_1h": 0.3,
        },
        {
            "exchange": "deribit",
            "symbol": "ETH",
            "perp_close_price": None,
            "iv_minus_rv_1h": None,
            "iv_rv_zscore_1d": None,
            "funding_rate_last_known": None,
            "rv_1h": None,
        },
    ]
    groups = gold_frames._group_target_rows(rows)
    assert [len(group) for group in groups] == [2, 1]
    output = gold_frames._prediction_target_rows(rows[:2])
    assert output[0]["target_forward_return_1h"] is None
    assert output[0]["label_regime_shift_1h"] is None
    assert gold_frames._float_or_none(True) is None
    assert gold_frames._float_or_none("1") is None
    assert gold_frames._float_or_none(2) == 2.0
    assert gold_frames._min_float([None, "bad", 3, 1]) == 1.0
    assert gold_frames._min_float([None, "bad"]) is None
    assert gold_frames._forward_log_return(100.0, 110.0) == pytest.approx(math.log(1.1))
    assert gold_frames._forward_log_return(0.0, 110.0) is None
    assert gold_frames.prediction_target_definitions()
    assert gold_frames.strategy_feature_lookbacks()
