"""Tests for Silver volatility dataset-family transformations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services import silver_volatility
from application.services.silver_service import (
    SilverBuildReport,
    _bronze_month_files,
    _iso_utc,
    _normalize_symbol_expr,
    _require_polars,
    _silver_month_path,
    discover_months,
)

pl = pytest.importorskip("polars")


def _write_bronze_day_file(
    root: Path,
    *,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
    day: str,
    rows: Sequence[Mapping[str, object]],
    instrument_type: str,
) -> None:
    """Write one Bronze volatility day partition for direct module tests."""

    target = (
        root
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month}"
        / f"date={day}"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def test_build_volatility_observed_for_symbol_uses_dataset_family_module(tmp_path: Path) -> None:
    """The volatility module writes observed rows and reports invalid and duplicate input rows."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "volatility_index_data",
            "exchange": "Deribit",
            "symbol": "btc",
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 30, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_get_volatility_index_data",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "value": 55.0,
            "origin_payload": "{}",
        },
        {
            "schema_version": "v1",
            "dataset_type": "volatility_index_data",
            "exchange": "Deribit",
            "symbol": "btc",
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 40, tzinfo=UTC),
            "run_id": "r2",
            "source_endpoint": "public_get_volatility_index_data",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "value": 56.0,
            "origin_payload": "{}",
        },
        {
            "schema_version": "v1",
            "dataset_type": "volatility_index_data",
            "exchange": "Deribit",
            "symbol": "btc",
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 1, 30, tzinfo=UTC),
            "run_id": "r3",
            "source_endpoint": "public_get_volatility_index_data",
            "open_time": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
            "timeframe": "1m",
            "value": -1.0,
            "origin_payload": "{}",
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="volatility_index_data",
        exchange="deribit",
        symbol="BTC",
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        instrument_type="perp",
    )

    report = silver_volatility.build_volatility_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
        timeframe="1m",
        bronze_dataset_type="volatility_index_data",
        output_dataset_type="volatility_index_data_observed",
        dependencies=silver_volatility.VolatilityObservedDependencies(
            require_polars=_require_polars,
            discover_months=discover_months,
            bronze_month_files=_bronze_month_files,
            silver_month_path=_silver_month_path,
            normalize_symbol_expr=_normalize_symbol_expr,
            iso_utc=_iso_utc,
            report_factory=SilverBuildReport,
        ),
    )

    assert isinstance(report, SilverBuildReport)
    assert report.dataset == "volatility_index_data_observed"
    assert report.rows_in == 3
    assert report.rows_out == 1
    assert report.duplicates_removed == 1
    assert report.invalid_ohlc_rows == 1
    assert report.symbols == ["BTC"]

    output_path = (
        silver
        / "dataset_type=volatility_index_data_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    written = pl.read_parquet(output_path)
    assert written.height == 1
    assert written["symbol"].to_list() == ["BTC"]
    assert written["volatility_value"].to_list() == [56.0]
