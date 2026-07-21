"""Tests for Gold frame discovery, preparation, and validation helpers."""

from __future__ import annotations

import os
from datetime import UTC, datetime
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


def test_read_dataset_frame_uses_newest_matching_symbol_file(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    symbol_root = root / "dataset_type=spot_ohlcv" / "exchange=deribit" / "symbol=BTC-PERPETUAL" / "timeframe=1m"
    symbol_root.mkdir(parents=True)
    old_file = symbol_root / "old.parquet"
    new_file = symbol_root / "new.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(old_file)
    pl.DataFrame({"value": [2]}).write_parquet(new_file)
    os.utime(old_file, (1, 1))
    os.utime(new_file, (2, 2))

    frame = gold_frames.read_dataset_frame(
        silver_root=str(root),
        exchange="deribit",
        symbol="BTC",
        dataset_type="spot_ohlcv",
        timeframe="1m",
    )

    assert frame.get_column("value").to_list() == [2]


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
