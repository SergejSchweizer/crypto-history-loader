"""Integration tests for live-origin Gold microstructure features."""

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


def _l2_row(timestamp: datetime, *, instrument_name: str, available: bool = True) -> dict[str, object]:
    return {
        "timestamp_m1": timestamp,
        "exchange": "deribit",
        "symbol": "BTC",
        "instrument_type": "option" if instrument_name != "BTC-PERPETUAL" else "perpetual",
        "instrument_name": instrument_name,
        "underlying": "BTC",
        "expiry": None,
        "strike": None,
        "option_type": None,
        "best_bid_price": 99.0 if available else None,
        "best_ask_price": 101.0 if available else None,
        "mid_price": 100.0 if available else None,
        "spread": 2.0 if available else None,
        "top_bid_size": 5.0,
        "top_ask_size": 4.0,
        "top_of_book_imbalance": 1.0 / 9.0,
        "bid_depth_10bps": 5.0,
        "ask_depth_10bps": 4.0,
        "bid_depth_50bps": 8.0,
        "ask_depth_50bps": 7.0,
        "quote_available": available,
        "quote_age_seconds": 5.0 if available else None,
        "stale_quote": False,
        "minutes_since_l2_observation": 0 if available else None,
    }


def test_live_microstructure_gold_preserves_l2_availability_and_lineage(tmp_path: Path) -> None:
    """Live microstructure Gold should expose L2 state without filling missing live minutes."""

    t0 = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)
    t2 = t0 + timedelta(minutes=2)
    silver = tmp_path / "silver"
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
        gold_root=str(tmp_path / "gold-live-microstructure"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.live.microstructure_features.m1",
    )
    live = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert live.height == 3
    assert live["perps_l2_mid_price"].to_list() == [100.0, None, 100.0]
    assert live["perps_l2_quote_available"].to_list() == [True, None, True]
    assert live["perps_l2_as_of"].to_list() == [t0, None, t2]
    assert live["perps_l2_live_snapshot_derived"].to_list() == [True, None, True]
    assert live["options_l2_contract_count"].to_list() == [2, None, None]
    assert live["options_l2_quote_coverage_ratio"].to_list() == [0.5, None, None]
    assert live["options_l2_as_of"].to_list() == [t0, None, None]
    assert live["options_l2_live_snapshot_derived"].to_list() == [True, None, None]
    assert not any(column.startswith(("target_", "label_")) for column in live.columns)
    assert manifest["required_source_datasets"] == ["perps_l2_1m_feature", "options_l2_1m_feature"]
    assert manifest["optional_source_datasets"] == []
    assert manifest["missing_value_count_by_column"]["perps_l2_mid_price"] == 1
    assert manifest["missing_value_count_by_column"]["options_l2_contract_count"] == 2
    assert manifest["feature_metadata"]["perps_l2_as_of"]["source_dataset"] == "perps_l2_1m_feature"
    assert manifest["feature_metadata"]["options_l2_as_of"]["source_dataset"] == "options_l2_1m_feature"


def test_live_microstructure_gold_contract_declares_l2_sources() -> None:
    """The live microstructure contract should be isolated to L2 Silver feature inputs."""

    contract = gold_dataset_contract("gold.live.microstructure_features.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "perps_l2_1m_feature",
        "options_l2_1m_feature",
    ]
    assert contract.optional_requirements == ()
    assert contract.missing_data_policy == "observed_only"
