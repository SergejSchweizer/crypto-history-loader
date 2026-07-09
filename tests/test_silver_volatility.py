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
    build_volatility_index_1m_feature_for_symbol,
    build_volatility_snapshot_observed_for_symbol,
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


def _write_snapshot_bronze_hour_file(
    root: Path,
    *,
    exchange: str,
    currency: str,
    month: str,
    day: str,
    hour: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Write one live-loader volatility snapshot Bronze hour partition."""

    target = (
        root
        / "dataset_type=volatility_index_snapshot_1m"
        / f"exchange={exchange}"
        / f"currency={currency}"
        / "source=rest_get_volatility_index_data"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month.split('-', 1)[1]}"
        / f"date={day}"
        / f"hour={hour}"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def _write_silver_observed_file(
    root: Path,
    *,
    dataset_type: str,
    exchange: str,
    symbol: str,
    month: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Write one Silver volatility-observed month file for feature tests."""

    target = (
        root
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month}"
        / f"{symbol}-{month}.parquet"
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
            "open": 54.0,
            "high": 56.0,
            "low": 53.5,
            "close": 55.0,
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
            "open": 55.0,
            "high": 57.0,
            "low": 54.5,
            "close": 56.0,
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
            "open": -1.0,
            "high": -1.0,
            "low": -1.0,
            "close": -1.0,
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
    assert written["volatility_open"].to_list() == [55.0]
    assert written["volatility_high"].to_list() == [57.0]
    assert written["volatility_low"].to_list() == [54.5]
    assert written["volatility_close"].to_list() == [56.0]


def test_build_volatility_observed_for_symbol_reads_legacy_value_only_bronze(tmp_path: Path) -> None:
    """Legacy Bronze volatility files without OHLC columns should remain readable."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    row = {
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
        "value": 42.0,
        "origin_payload": "{}",
    }
    _write_bronze_day_file(
        bronze,
        market="volatility_index_data",
        exchange="deribit",
        symbol="BTC",
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=[row],
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

    assert report.rows_out == 1
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
    assert written["volatility_value"].to_list() == [42.0]
    assert written["volatility_open"].to_list() == [42.0]
    assert written["volatility_high"].to_list() == [42.0]
    assert written["volatility_low"].to_list() == [42.0]
    assert written["volatility_close"].to_list() == [42.0]


def test_build_volatility_snapshot_observed_for_btc_and_eth(tmp_path: Path) -> None:
    """BTC and ETH live snapshot partitions should build into canonical observed Silver rows."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    for currency, close in (("BTC", 61.0), ("ETH", 72.0)):
        _write_snapshot_bronze_hour_file(
            bronze,
            exchange="deribit",
            currency=currency,
            month="2026-06",
            day="2026-06-12",
            hour="19",
            rows=[
                {
                    "schema_version": "v1",
                    "dataset_type": "volatility_index_snapshot_1m",
                    "exchange": "deribit",
                    "source": "rest_get_volatility_index_data",
                    "currency": currency.lower(),
                    "source_currency": currency,
                    "timestamp": datetime(2026, 6, 12, 19, 48, tzinfo=UTC),
                    "open": close - 1.0,
                    "high": close + 1.0,
                    "low": close - 2.0,
                    "close": close,
                    "resolution": 60,
                    "snapshot_time": datetime(2026, 6, 12, 19, 48, 30, tzinfo=UTC),
                    "ingested_at": datetime(2026, 6, 12, 19, 48, 31, tzinfo=UTC),
                    "run_id": f"{currency}-1",
                    "raw_payload_hash": f"{currency}-hash",
                }
            ],
        )

        report = build_volatility_snapshot_observed_for_symbol(
            bronze_root=str(bronze),
            silver_root=str(silver),
            exchange="deribit",
            symbol=currency,
        )

        assert report.dataset == "volatility_index_snapshot_1m_observed"
        assert report.rows_in == 1
        assert report.rows_out == 1
        output_path = (
            silver
            / "dataset_type=volatility_index_snapshot_1m_observed"
            / "exchange=deribit"
            / f"symbol={currency}"
            / "timeframe=1m"
            / "year=2026"
            / "month=2026-06"
            / f"{currency}-2026-06.parquet"
        )
        written = pl.read_parquet(output_path)
        assert written["symbol"].to_list() == [currency]
        assert written["volatility_value"].to_list() == [close]
        assert written["volatility_open"].to_list() == [close - 1.0]
        assert written["volatility_high"].to_list() == [close + 1.0]
        assert written["volatility_low"].to_list() == [close - 2.0]
        assert written["volatility_close"].to_list() == [close]
        assert written["source_endpoint"].to_list() == ["rest_get_volatility_index_data"]


def test_build_volatility_index_1m_feature_prefers_snapshot_with_historical_fallback(
    tmp_path: Path,
) -> None:
    """Canonical IV features should use snapshot rows first and historical rows only as fallback."""

    silver = tmp_path / "silver"
    month = "2026-06"
    t0 = datetime(2026, 6, 12, 19, 47, tzinfo=UTC)
    t1 = datetime(2026, 6, 12, 19, 48, tzinfo=UTC)
    t2 = datetime(2026, 6, 12, 19, 49, tzinfo=UTC)
    common = {
        "exchange": "deribit",
        "symbol": "BTC",
        "instrument_type": "perp",
        "source_endpoint": "rest_get_volatility_index_data",
    }
    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange="deribit",
        symbol="BTC",
        month=month,
        rows=[
            {
                **common,
                "timestamp": t0,
                "dataset_type": "volatility_index_data",
                "volatility_value": 50.0,
                "volatility_open": 49.0,
                "volatility_high": 51.0,
                "volatility_low": 48.0,
                "volatility_close": 50.0,
                "volatility_source_timestamp": t0,
                "ingested_at": datetime(2026, 6, 12, 19, 47, 30, tzinfo=UTC),
            },
            {
                **common,
                "timestamp": t1,
                "dataset_type": "volatility_index_data",
                "volatility_value": 55.0,
                "volatility_open": 54.0,
                "volatility_high": 56.0,
                "volatility_low": 53.0,
                "volatility_close": 55.0,
                "volatility_source_timestamp": t1,
                "ingested_at": datetime(2026, 6, 12, 20, 0, tzinfo=UTC),
            },
        ],
    )
    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        month=month,
        rows=[
            {
                **common,
                "timestamp": t1,
                "dataset_type": "volatility_index_snapshot_1m",
                "volatility_value": 60.0,
                "volatility_open": 59.0,
                "volatility_high": 61.0,
                "volatility_low": 58.0,
                "volatility_close": 60.0,
                "volatility_source_timestamp": t1,
                "ingested_at": datetime(2026, 6, 12, 19, 48, 30, tzinfo=UTC),
            },
            {
                **common,
                "timestamp": t2,
                "dataset_type": "volatility_index_snapshot_1m",
                "volatility_value": 62.0,
                "volatility_open": 61.0,
                "volatility_high": 63.0,
                "volatility_low": 60.0,
                "volatility_close": 62.0,
                "volatility_source_timestamp": t2,
                "ingested_at": datetime(2026, 6, 12, 19, 49, 20, tzinfo=UTC),
            },
            {
                **common,
                "timestamp": t2,
                "dataset_type": "volatility_index_snapshot_1m",
                "volatility_value": 64.0,
                "volatility_open": 63.0,
                "volatility_high": 65.0,
                "volatility_low": 62.0,
                "volatility_close": 64.0,
                "volatility_source_timestamp": t2,
                "ingested_at": datetime(2026, 6, 12, 19, 49, 40, tzinfo=UTC),
            },
        ],
    )

    report = build_volatility_index_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert report.dataset == "volatility_index_1m_feature"
    assert report.rows_in == 5
    assert report.rows_out == 3
    assert report.duplicates_removed == 2
    output_path = (
        silver
        / "dataset_type=volatility_index_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )
    written = pl.read_parquet(output_path)
    assert written["timestamp_m1"].to_list() == [t0, t1, t2]
    assert written["iv_close"].to_list() == [50.0, 60.0, 64.0]
    assert written["iv_source_dataset"].to_list() == [
        "volatility_index_data_observed",
        "volatility_index_snapshot_1m_observed",
        "volatility_index_snapshot_1m_observed",
    ]
    assert written["iv_range"].to_list() == [3.0, 3.0, 3.0]
    assert written["minutes_since_iv_observation"].to_list() == [0, 0, 0]
    assert written["iv_data_available"].to_list() == [True, True, True]
