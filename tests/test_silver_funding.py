"""Tests for Silver funding dataset-family transformations."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services import silver_funding
from application.services.silver_service import (
    SilverBuildReport,
    _bronze_month_files,
    _iso_utc,
    _require_polars,
    _silver_funding_feature_month_path,
    _silver_month_path,
    discover_months,
)

pl = pytest.importorskip("polars")


def test_build_funding_observed_and_feature_use_dataset_family_module(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    rows = [
        _funding_row(symbol=symbol, open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC), funding_rate=0.001),
        _funding_row(
            symbol=symbol,
            open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            funding_rate=0.002,
            ingested_at=datetime(2026, 5, 1, 0, 20, tzinfo=UTC),
        ),
        _funding_row(symbol=symbol, open_time=datetime(2026, 5, 1, 0, 2, tzinfo=UTC), funding_rate=0.003),
        _funding_row(symbol=symbol, open_time=datetime(2026, 5, 1, 0, 3, tzinfo=UTC), funding_rate=None),
    ]
    target = (
        bronze
        / "dataset_type=funding"
        / "exchange=deribit"
        / "instrument_type=perp"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "date=2026-05-01"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(target)

    observed_report = silver_funding.build_funding_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        dependencies=_dependencies(),
    )

    assert isinstance(observed_report, SilverBuildReport)
    assert observed_report.rows_in == 4
    assert observed_report.rows_out == 2
    assert observed_report.duplicates_removed == 1
    assert observed_report.null_price_rows == 1

    feature_report = silver_funding.build_funding_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="1m",
        dependencies=_dependencies(),
    )

    assert isinstance(feature_report, SilverBuildReport)
    assert feature_report.rows_out == 3
    feature_file = (
        silver
        / "dataset_type=funding_1m_feature"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    feature = pl.read_parquet(feature_file)
    assert feature.select("minutes_since_funding").to_series().to_list() == [0, 1, 0]
    assert feature.select("funding_rate_last_known").to_series().to_list() == [0.002, 0.002, 0.003]


def _funding_row(
    *,
    symbol: str,
    open_time: datetime,
    funding_rate: float | None,
    ingested_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "exchange": "deribit",
        "symbol": symbol,
        "instrument_type": "perp",
        "source_endpoint": "public_funding",
        "open_time": open_time,
        "timeframe": "1m",
        "funding_rate": funding_rate,
        "ingested_at": ingested_at or open_time,
    }


def _dependencies() -> silver_funding.FundingDependencies:
    return silver_funding.FundingDependencies(
        require_polars=_require_polars,
        discover_months=discover_months,
        bronze_month_files=_bronze_month_files,
        silver_month_path=_silver_month_path,
        silver_funding_feature_month_path=_silver_funding_feature_month_path,
        iso_utc=_iso_utc,
        report_factory=SilverBuildReport,
    )
