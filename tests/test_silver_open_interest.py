"""Tests for Silver open-interest dataset-family transformations."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services import silver_open_interest
from application.services.silver_service import (
    SilverBuildReport,
    _bronze_month_files,
    _iso_utc,
    _normalize_symbol_expr,
    _require_polars,
    _silver_month_path,
    _silver_open_interest_feature_month_path,
    discover_months,
)

pl = pytest.importorskip("polars")


def test_build_open_interest_observed_and_feature_use_dataset_family_module(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    rows = [
        {
            "exchange": "Deribit",
            "symbol": "btc_perpetual",
            "instrument_type": "perp",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1000.0,
            "ingested_at": datetime(2026, 5, 1, 0, 0, 30, tzinfo=UTC),
        },
        {
            "exchange": "Deribit",
            "symbol": "btc_perpetual",
            "instrument_type": "perp",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1000.0,
            "ingested_at": datetime(2026, 5, 1, 0, 0, 35, tzinfo=UTC),
        },
        {
            "exchange": "Deribit",
            "symbol": "btc_perpetual",
            "instrument_type": "perp",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1025.0,
            "ingested_at": datetime(2026, 5, 1, 0, 2, 30, tzinfo=UTC),
        },
        {
            "exchange": "Deribit",
            "symbol": "btc_perpetual",
            "instrument_type": "perp",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": -1.0,
            "ingested_at": datetime(2026, 5, 1, 0, 3, 30, tzinfo=UTC),
        },
    ]
    target = (
        bronze
        / "dataset_type=open_interest"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "date=2026-05-01"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(target)

    observed_report = silver_open_interest.build_open_interest_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        dependencies=_dependencies(),
    )

    assert isinstance(observed_report, SilverBuildReport)
    assert observed_report.rows_in == 4
    assert observed_report.rows_out == 2
    assert observed_report.duplicates_removed == 1
    assert observed_report.invalid_ohlc_rows == 1

    feature_report = silver_open_interest.build_open_interest_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        observed_timeframe="1m",
        dependencies=_dependencies(),
    )

    assert isinstance(feature_report, SilverBuildReport)
    assert feature_report.rows_out == 3
    feature_file = (
        silver
        / "dataset_type=open_interest_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-PERPETUAL-2026-05.parquet"
    )
    feature = pl.read_parquet(feature_file)
    assert feature.select("minutes_since_open_interest_observation").to_series().to_list() == [0, 1, 0]


def _dependencies() -> silver_open_interest.OpenInterestDependencies:
    return silver_open_interest.OpenInterestDependencies(
        require_polars=_require_polars,
        discover_months=discover_months,
        bronze_month_files=_bronze_month_files,
        silver_month_path=_silver_month_path,
        silver_open_interest_feature_month_path=_silver_open_interest_feature_month_path,
        normalize_symbol_expr=_normalize_symbol_expr,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )
