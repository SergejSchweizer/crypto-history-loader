"""Tests for Silver volatility dataset-family transformations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from math import log
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


def test_build_volatility_observed_for_symbol_keeps_newest_ingested_duplicate(tmp_path: Path) -> None:
    """Historical volatility observed deduplication should be deterministic by newest ingestion."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    rows = []
    for run_id, ingested_at, close in (
        ("new", datetime(2026, 5, 1, 0, 0, 40, tzinfo=UTC), 57.0),
        ("old", datetime(2026, 5, 1, 0, 0, 30, tzinfo=UTC), 55.0),
    ):
        rows.append(
            {
                "schema_version": "v1",
                "dataset_type": "volatility_index_data",
                "exchange": "Deribit",
                "symbol": "btc",
                "instrument_type": "perp",
                "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                "ingested_at": ingested_at,
                "run_id": run_id,
                "source_endpoint": "public_get_volatility_index_data",
                "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                "timeframe": "1m",
                "value": close,
                "open": close - 1.0,
                "high": close + 1.0,
                "low": close - 2.0,
                "close": close,
                "origin_payload": "{}",
            }
        )
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

    assert report.duplicates_removed == 1
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
    assert written["volatility_close"].to_list() == [57.0]


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


def test_build_volatility_index_1m_feature_uses_trailing_iv_windows(tmp_path: Path) -> None:
    """IV rolling features should use current and past timestamps only."""

    silver = tmp_path / "silver"
    month = "2026-06"
    timestamps = [
        datetime(2026, 6, 12, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 12, 0, 5, tzinfo=UTC),
        datetime(2026, 6, 12, 0, 15, tzinfo=UTC),
        datetime(2026, 6, 12, 1, 0, tzinfo=UTC),
        datetime(2026, 6, 12, 1, 1, tzinfo=UTC),
    ]
    closes = [50.0, 55.0, 70.0, 80.0, 1000.0]
    rows = []
    for index, timestamp in enumerate(timestamps):
        close = closes[index]
        rows.append(
            {
                "timestamp": timestamp,
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_type": "perp",
                "dataset_type": "volatility_index_snapshot_1m",
                "volatility_value": close,
                "volatility_open": close - 1.0,
                "volatility_high": close + 1.0,
                "volatility_low": close - 2.0,
                "volatility_close": close,
                "volatility_source_timestamp": timestamp,
                "ingested_at": timestamp,
                "source_endpoint": "rest_get_volatility_index_data",
            }
        )
    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        month=month,
        rows=rows,
    )

    build_volatility_index_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

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
    assert written["iv_change_5m"].to_list() == [None, 5.0, 15.0, 10.0, 930.0]
    assert written["iv_change_15m"].to_list() == [None, None, 20.0, 10.0, 930.0]
    assert written["iv_change_1h"].to_list() == [None, None, None, 30.0, 950.0]
    assert written["iv_percentile_30d"].to_list() == [1.0, 1.0, 1.0, 1.0, 1.0]
    assert written["iv_zscore_1d"].null_count() == 1
    assert written["iv_zscore_7d"].null_count() == 1


def test_build_volatility_index_1m_feature_preserves_previous_close_across_month_boundary(
    tmp_path: Path,
) -> None:
    """QC-02: the first minute of a month must see the prior month's final close."""

    silver = tmp_path / "silver"

    def _row(timestamp: datetime, close: float) -> dict[str, object]:
        return {
            "timestamp": timestamp,
            "exchange": "deribit",
            "symbol": "BTC",
            "instrument_type": "perp",
            "dataset_type": "volatility_index_snapshot_1m",
            "volatility_value": close,
            "volatility_open": close - 1.0,
            "volatility_high": close + 1.0,
            "volatility_low": close - 2.0,
            "volatility_close": close,
            "volatility_source_timestamp": timestamp,
            "ingested_at": timestamp,
            "source_endpoint": "rest_get_volatility_index_data",
        }

    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        month="2026-01",
        rows=[_row(datetime(2026, 1, 31, 23, 59, tzinfo=UTC), 50.0)],
    )
    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        month="2026-02",
        rows=[_row(datetime(2026, 2, 1, 0, 0, tzinfo=UTC), 55.0)],
    )

    build_volatility_index_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    january_output = pl.read_parquet(
        silver
        / "dataset_type=volatility_index_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-01"
        / "BTC-2026-01.parquet"
    )
    february_output = pl.read_parquet(
        silver
        / "dataset_type=volatility_index_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-02"
        / "BTC-2026-02.parquet"
    )

    # Storage-partition trimming: each month's output only contains its own rows.
    assert january_output.height == 1
    assert february_output.height == 1
    # Without cross-month buffering this would be null because the prior close
    # lived in a different monthly partition.
    assert february_output["iv_return_1m"].to_list()[0] == pytest.approx(log(55.0 / 50.0))


def test_build_volatility_index_1m_feature_preserves_previous_close_across_year_boundary(
    tmp_path: Path,
) -> None:
    """QC-02: the first minute of a year must see the prior year's final close."""

    silver = tmp_path / "silver"

    def _row(timestamp: datetime, close: float) -> dict[str, object]:
        return {
            "timestamp": timestamp,
            "exchange": "deribit",
            "symbol": "BTC",
            "instrument_type": "perp",
            "dataset_type": "volatility_index_snapshot_1m",
            "volatility_value": close,
            "volatility_open": close - 1.0,
            "volatility_high": close + 1.0,
            "volatility_low": close - 2.0,
            "volatility_close": close,
            "volatility_source_timestamp": timestamp,
            "ingested_at": timestamp,
            "source_endpoint": "rest_get_volatility_index_data",
        }

    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        month="2025-12",
        rows=[_row(datetime(2025, 12, 31, 23, 59, tzinfo=UTC), 50.0)],
    )
    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        month="2026-01",
        rows=[_row(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 55.0)],
    )

    build_volatility_index_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    december_output = pl.read_parquet(
        silver
        / "dataset_type=volatility_index_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2025"
        / "month=2025-12"
        / "BTC-2025-12.parquet"
    )
    january_output = pl.read_parquet(
        silver
        / "dataset_type=volatility_index_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-01"
        / "BTC-2026-01.parquet"
    )

    assert december_output.height == 1
    assert january_output.height == 1
    # Without cross-year buffering this would be null because the prior close
    # lived in a different calendar-year monthly partition.
    assert january_output["iv_return_1m"].to_list()[0] == pytest.approx(log(55.0 / 50.0))


def test_snapshot_layout_discovery_handles_missing_and_mixed_month_directories(tmp_path: Path) -> None:
    """Snapshot discovery returns normalized symbols and canonical month keys."""

    bronze = tmp_path / "bronze"
    assert (
        silver_volatility.discover_snapshot_symbols(
            bronze_root=str(bronze),
            dataset_type="volatility_index_snapshot_1m",
            exchange="deribit",
        )
        == []
    )

    source_root = (
        bronze
        / "dataset_type=volatility_index_snapshot_1m"
        / "exchange=deribit"
        / "currency=btc"
        / "source=rest_get_volatility_index_data"
    )
    (source_root / "year=2026" / "month=06").mkdir(parents=True)
    (source_root / "year=2026" / "month=2026-07").mkdir(parents=True)
    (bronze / "dataset_type=volatility_index_snapshot_1m" / "exchange=deribit" / "currency=eth").mkdir(parents=True)

    assert silver_volatility.discover_snapshot_symbols(
        bronze_root=str(bronze),
        dataset_type="volatility_index_snapshot_1m",
        exchange="deribit",
    ) == ["BTC", "ETH"]
    assert silver_volatility._discover_snapshot_months(
        bronze_root=str(bronze),
        dataset_type="volatility_index_snapshot_1m",
        exchange="deribit",
        currency="btc",
        source="rest_get_volatility_index_data",
    ) == ["2026-06", "2026-07"]


def test_rolling_percentile_uses_closed_trailing_window_and_resets_by_symbol() -> None:
    """Percentiles retain same-cutoff rows, discard older rows, and isolate symbols."""

    frame = pl.DataFrame(
        {
            "exchange": ["deribit", "deribit", "deribit", "deribit"],
            "symbol": ["BTC", "BTC", "BTC", "ETH"],
            "timestamp_m1": [
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 1, 2, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
            ],
            "iv_close": [10.0, 20.0, 10.0, 5.0],
        }
    )

    assert silver_volatility._rolling_percentile_30d(frame) == [1.0, 1.0, 0.5, 1.0]


def test_read_dedup_observed_month_prefers_snapshot_then_newest_ingestion(tmp_path: Path) -> None:
    """Snapshot observations override historical rows at equal timestamps deterministically."""

    silver = tmp_path / "silver"
    timestamp = datetime(2026, 6, 12, 19, 48, tzinfo=UTC)
    base_row = {
        "exchange": "deribit",
        "symbol": "BTC",
        "timestamp": timestamp,
        "ingested_at": datetime(2026, 6, 12, 19, 49, tzinfo=UTC),
    }
    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange="deribit",
        symbol="BTC",
        month="2026-06",
        rows=[base_row],
    )
    _write_silver_observed_file(
        silver,
        dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        month="2026-06",
        rows=[{**base_row, "ingested_at": datetime(2026, 6, 12, 19, 48, tzinfo=UTC)}],
    )

    selected, rows_in = silver_volatility._read_dedup_observed_month(
        pl,
        silver_root=str(silver),
        historical_dataset_type="volatility_index_data_observed",
        snapshot_dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="BTC",
        timeframe="1m",
        month="2026-06",
    )

    assert rows_in == 2
    assert selected is not None
    assert selected["iv_source_dataset"].to_list() == ["volatility_index_snapshot_1m_observed"]
    assert silver_volatility._read_dedup_observed_month(
        pl,
        silver_root=str(silver),
        historical_dataset_type="volatility_index_data_observed",
        snapshot_dataset_type="volatility_index_snapshot_1m_observed",
        exchange="deribit",
        symbol="ETH",
        timeframe="1m",
        month="2026-06",
    ) == (None, 0)
