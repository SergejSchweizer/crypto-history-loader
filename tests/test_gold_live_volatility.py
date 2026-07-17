"""Integration tests for live-origin Gold volatility features."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import gold_dataset_contract
from application.services.gold_service import build_gold_for_symbol

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


def test_live_volatility_gold_preserves_iv_feature_contract_and_lineage(tmp_path: Path) -> None:
    """Live volatility Gold should expose snapshot IV features without historical fill."""

    t0 = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)
    t2 = t0 + timedelta(minutes=2)
    silver = tmp_path / "silver"
    _write_silver(
        silver,
        dataset_type="volatility_index_1m_feature",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": "deribit",
                "symbol": "BTC",
                "iv_open": 50.0,
                "iv_high": 51.0,
                "iv_low": 49.0,
                "iv_close": 50.5,
                "iv_range": 2.0,
                "iv_return_1m": None,
                "iv_change_5m": None,
                "iv_change_15m": None,
                "iv_change_1h": None,
                "iv_zscore_1d": None,
                "iv_zscore_7d": None,
                "iv_percentile_30d": None,
                "iv_30d_annualized_pct": 50.5,
                "iv_source_dataset": "volatility_index_snapshot_1m_observed",
                "iv_source_timestamp": t0,
                "minutes_since_iv_observation": 0,
                "iv_data_available": True,
            },
            {
                "timestamp_m1": t2,
                "exchange": "deribit",
                "symbol": "BTC",
                "iv_open": 52.0,
                "iv_high": 54.0,
                "iv_low": 51.0,
                "iv_close": 53.0,
                "iv_range": 3.0,
                "iv_return_1m": 0.0495,
                "iv_change_5m": 2.5,
                "iv_change_15m": 2.5,
                "iv_change_1h": 2.5,
                "iv_zscore_1d": 1.0,
                "iv_zscore_7d": 0.5,
                "iv_percentile_30d": 0.75,
                "iv_30d_annualized_pct": 53.0,
                "iv_source_dataset": "volatility_index_snapshot_1m_observed",
                "iv_source_timestamp": t2,
                "minutes_since_iv_observation": 0,
                "iv_data_available": True,
            },
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-live-volatility"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.live.volatility_features.m1",
    )
    live = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert live.columns == [
        "timestamp_m1",
        "exchange",
        "symbol",
        "iv_open",
        "iv_high",
        "iv_low",
        "iv_close",
        "iv_range",
        "iv_return_1m",
        "iv_change_5m",
        "iv_change_15m",
        "iv_change_1h",
        "iv_zscore_1d",
        "iv_zscore_7d",
        "iv_percentile_30d",
        "iv_30d_annualized_pct",
        "iv_source_dataset",
        "iv_source_timestamp",
        "minutes_since_iv_observation",
        "iv_data_available",
        "as_of",
        "live_snapshot_derived",
    ]
    assert live.height == 3
    assert live["iv_close"].to_list() == [50.5, None, 53.0]
    assert live["iv_source_dataset"].to_list() == [
        "volatility_index_snapshot_1m_observed",
        None,
        "volatility_index_snapshot_1m_observed",
    ]
    assert live["iv_data_available"].to_list() == [True, None, True]
    assert live["as_of"].to_list() == [t0, None, t2]
    assert live["live_snapshot_derived"].to_list() == [True, None, True]
    assert not any(column.startswith(("target_", "label_")) for column in live.columns)
    assert manifest["required_source_datasets"] == ["volatility_index_1m_feature"]
    assert manifest["optional_source_datasets"] == []
    assert manifest["source_silver_datasets"]["volatility_index_1m_feature"]["rows"] == 2
    assert manifest["missing_value_count_by_column"]["iv_close"] == 1
    assert manifest["feature_metadata"]["iv_open"]["source_dataset"] == "volatility_index_1m_feature"
    assert manifest["feature_metadata"]["as_of"]["source_dataset"] == "gold_live_lineage"


def test_live_volatility_gold_contract_declares_single_live_source() -> None:
    """The live volatility contract should not depend on historical feature sources."""

    contract = gold_dataset_contract("gold.live.volatility_features.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "volatility_index_1m_feature",
    ]
    assert contract.optional_requirements == ()
    assert contract.missing_data_policy == "observed_only"
