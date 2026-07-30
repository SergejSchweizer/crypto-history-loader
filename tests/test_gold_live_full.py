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


def _write_volatility_snapshot(silver: Path, timestamps: list[datetime]) -> None:
    _write_silver(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
                "volatility_value": 50.5 + index,
                "volatility_open": 50.0 + index,
                "volatility_high": 51.0 + index,
                "volatility_low": 49.0 + index,
                "volatility_close": 50.5 + index,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )


def _write_index_price_snapshot(silver: Path, timestamps: list[datetime]) -> None:
    _write_silver(
        silver,
        dataset_type="index_price_snapshot_1m_observed",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
                "index_price": 100.0 + index,
                "index_price_is_observed": True,
                "minutes_since_index_price_observation": 0,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )


def _write_futures_summary_snapshot(silver: Path, timestamps: list[datetime]) -> None:
    _write_silver(
        silver,
        dataset_type="futures_summary_snapshot_1m_observed",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_type": "perpetual",
                "mark_price": 100.0 + index,
                "index_price": 99.0 + index,
                "mark_index_spread": 1.0,
                "mark_index_ratio": 1.01,
                "open_interest": 1000.0 + index,
                "volume": 10.0 + index,
                "turnover": 1000.0 + index,
                "funding_rate": 0.001,
                "summary_is_observed": True,
                "minutes_since_summary_observation": 0,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )


def _write_options_surface_snapshots(silver: Path, timestamps: list[datetime]) -> None:
    for dataset_type in ("options_ticker_snapshot_1m_observed", "options_instrument_ticker_snapshot_1m_observed"):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol="BTC",
            timeframe="1m",
            rows=[
                {
                    "timestamp_m1": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "atm_iv": 40.0 + index,
                    "short_dated_iv": 41.0 + index,
                    "skew": 0.1 + index,
                    "term_structure": 0.2 + index,
                    "put_call_iv_spread": 0.3 + index,
                    "contract_count": 2 + index,
                    "fresh_quote_count": 1 + index,
                    "stale_quote_count": 0,
                    "max_quote_age_seconds": 5.0,
                    "quote_coverage_ratio": 0.5,
                }
                for index, timestamp in enumerate(timestamps)
            ],
        )


def _write_live_trade_metadata_sources(silver: Path, timestamps: list[datetime]) -> None:
    _write_silver(
        silver,
        dataset_type="recent_trade_snapshot_1m_observed",
        symbol="BTC",
        timeframe="1m",
        rows=[
            {
                "trade_time": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
            }
            for timestamp in timestamps
        ],
    )
    for dataset_type, timeframe in (
        ("instrument_metadata_snapshot_daily_observed", "1d"),
        ("futures_instrument_metadata_snapshot_daily_observed", "1d"),
    ):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol="BTC",
            timeframe=timeframe,
            rows=[
                {
                    "snapshot_date": timestamps[0].date(),
                    "exchange": "deribit",
                    "symbol": "BTC",
                }
            ],
        )


def test_live_full_gold_combines_live_origin_features_without_historical_fill(tmp_path: Path) -> None:
    """The live full dataset should be one inference table from live-loader-derived inputs."""

    t0 = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)
    t2 = t0 + timedelta(minutes=2)
    silver = tmp_path / "silver"
    _write_volatility_snapshot(silver, [t0, t2])
    _write_index_price_snapshot(silver, [t0, t2])
    _write_futures_summary_snapshot(silver, [t0, t2])
    _write_options_surface_snapshots(silver, [t0, t2])
    _write_live_trade_metadata_sources(silver, [t0, t2])
    for dataset_type, symbol, rows in (
        (
            "perps_l2_snapshot_1m_observed",
            "BTC-PERPETUAL",
            [
                _l2_row(t0, instrument_name="BTC-PERPETUAL"),
                _l2_row(t2, instrument_name="BTC-PERPETUAL"),
            ],
        ),
        (
            "options_l2_snapshot_1m_observed",
            "BTC",
            [
                _l2_row(t0, instrument_name="BTC-29MAY26-100-C", available=True),
                _l2_row(t0, instrument_name="BTC-29MAY26-100-P", available=False),
            ],
        ),
    ):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol=symbol,
            timeframe="1m",
            rows=rows,
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

    timestamps = live_full["timestamp_m1"].to_list()
    assert t0 in timestamps
    assert t2 in timestamps
    assert live_full["volatility_index_close"].null_count() < live_full.height
    assert live_full["index_price"].null_count() < live_full.height
    assert live_full["futures_summary_mark_price"].null_count() < live_full.height
    assert live_full["options_surface_atm_iv"].null_count() < live_full.height
    assert live_full["perps_l2_mid_price"].null_count() < live_full.height
    assert live_full["options_l2_contract_count"].null_count() < live_full.height
    assert live_full["perps_l2_live_snapshot_derived"].any()
    assert live_full["options_l2_live_snapshot_derived"].any()
    assert not any(column.startswith(("target_", "label_")) for column in live_full.columns)
    assert manifest["dataset_id"] == "gold.live.full.m1"
    assert manifest["origin_repository"] == "crypto-live-loader"
    assert manifest["required_source_datasets"] == [
        "volatility_index_snapshot_1m_observed",
        "index_price_snapshot_1m_observed",
        "futures_summary_snapshot_1m_observed",
        "options_ticker_snapshot_1m_observed",
        "options_instrument_ticker_snapshot_1m_observed",
        "perps_l2_snapshot_1m_observed",
        "options_l2_snapshot_1m_observed",
        "recent_trade_snapshot_1m_observed",
        "instrument_metadata_snapshot_daily_observed",
        "futures_instrument_metadata_snapshot_daily_observed",
    ]
    assert manifest["optional_source_datasets"] == []
    assert manifest["missing_value_count_by_column"]["volatility_index_close"] > 0
    assert manifest["missing_value_count_by_column"]["options_l2_contract_count"] > 0


def test_live_full_gold_contract_declares_live_sources() -> None:
    """The typed live full contract should be isolated to live-loader-derived sources."""

    contract = gold_dataset_contract("gold.live.full.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "volatility_index_snapshot_1m_observed",
        "index_price_snapshot_1m_observed",
        "futures_summary_snapshot_1m_observed",
        "options_ticker_snapshot_1m_observed",
        "options_instrument_ticker_snapshot_1m_observed",
        "perps_l2_snapshot_1m_observed",
        "options_l2_snapshot_1m_observed",
        "recent_trade_snapshot_1m_observed",
        "instrument_metadata_snapshot_daily_observed",
        "futures_instrument_metadata_snapshot_daily_observed",
    ]
    assert contract.optional_requirements == ()
    assert contract.missing_data_policy == "observed_only"
