"""Integration tests for the canonical live full Gold dataset."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import gold_dataset_contract
from application.services.gold_service import build_gold_for_symbol
from tests.test_gold_live_microstructure import _l2_row

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


def _manifest(path: str | None) -> dict[str, object]:
    assert path is not None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_volatility_feature(silver: Path, timestamps: list[datetime]) -> None:
    _write_silver(
        silver,
        dataset_type="volatility_index_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
                "iv_open": 50.0 + index,
                "iv_high": 51.0 + index,
                "iv_low": 49.0 + index,
                "iv_close": 50.5 + index,
                "iv_range": 2.0,
                "iv_return_1m": None if index == 0 else 0.01,
                "iv_change_5m": None,
                "iv_change_15m": None,
                "iv_change_1h": None,
                "iv_zscore_1d": None,
                "iv_zscore_7d": None,
                "iv_percentile_30d": None,
                "iv_source_dataset": "volatility_index_snapshot_1m_observed",
                "iv_source_timestamp": timestamp,
                "minutes_since_iv_observation": 0,
                "iv_data_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )


def test_live_full_gold_combines_live_origin_features_without_historical_fill(tmp_path: Path) -> None:
    """The live full dataset should be one inference table from live-loader-derived inputs."""

    t0 = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)
    t2 = t0 + timedelta(minutes=2)
    silver = tmp_path / "silver"
    _write_volatility_feature(silver, [t0, t2])
    _write_silver(
        silver,
        dataset_type="perps_l2_1m_feature",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        rows=[
            _l2_row(t0, instrument_name="BTC-PERPETUAL"),
            _l2_row(t2, instrument_name="BTC-PERPETUAL"),
        ],
    )
    _write_silver(
        silver,
        dataset_type="options_l2_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            _l2_row(t0, instrument_name="BTC-29MAY26-100-C", available=True),
            _l2_row(t0, instrument_name="BTC-29MAY26-100-P", available=False),
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-live-full"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.live.full.m1",
    )
    live_full = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert live_full.height == 3
    assert live_full["iv_close"].to_list() == [50.5, None, 51.5]
    assert live_full["perps_l2_mid_price"].to_list() == [100.0, None, 100.0]
    assert live_full["options_l2_contract_count"].to_list() == [2, None, None]
    assert live_full["live_snapshot_derived"].to_list() == [True, None, True]
    assert live_full["perps_l2_live_snapshot_derived"].to_list() == [True, None, True]
    assert live_full["options_l2_live_snapshot_derived"].to_list() == [True, None, None]
    assert "index_price" in live_full.columns
    assert live_full["index_price"].null_count() == live_full.height
    assert not any(column.startswith(("target_", "label_")) for column in live_full.columns)
    assert manifest["dataset_id"] == "gold.live.full.m1"
    assert manifest["origin_repository"] == "crypto-live-loader"
    assert manifest["required_source_datasets"] == [
        "volatility_index_1m_feature",
        "perps_l2_1m_feature",
        "options_l2_1m_feature",
    ]
    assert manifest["optional_source_datasets"] == [
        "index_price_1m_feature",
        "futures_summary_1m_feature",
        "options_surface_1m_feature",
    ]
    assert manifest["missing_value_count_by_column"]["iv_close"] == 1
    assert manifest["missing_value_count_by_column"]["options_l2_contract_count"] == 2


def test_live_full_gold_contract_declares_live_sources() -> None:
    """The typed live full contract should be isolated to live-loader-derived sources."""

    contract = gold_dataset_contract("gold.live.full.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "volatility_index_1m_feature",
        "perps_l2_1m_feature",
        "options_l2_1m_feature",
    ]
    assert [requirement.dataset_type for requirement in contract.optional_requirements] == [
        "index_price_1m_feature",
        "futures_summary_1m_feature",
        "options_surface_1m_feature",
    ]
    assert contract.missing_data_policy == "observed_only"
