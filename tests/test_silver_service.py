"""Tests for silver transformation service."""

from __future__ import annotations

import inspect
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

import application.services.silver_service as silver_service
from application.services.silver_service import (
    _build_trade_feature_frame,
    _build_trade_observed_frame,
    build_funding_1m_feature_for_symbol,
    build_funding_observed_for_symbol,
    build_open_interest_1m_feature_for_symbol,
    build_open_interest_observed_for_symbol,
    build_perps_trades_1m_feature_for_symbol,
    build_perps_trades_observed_for_symbol,
    build_silver_for_symbol,
    build_volatility_observed_for_symbol,
    discover_months,
    discover_symbols,
)
from application.services.silver_sidecars import write_monthly_sidecars
from ingestion.lake_writes import write_empty_trade_minutes

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
    dataset_type: str | None = None,
    instrument_type: str | None = None,
) -> None:
    ds = dataset_type or market
    instrument = instrument_type or ("perp" if market == "perps_ohlcv" else market)
    target = (
        root
        / f"dataset_type={ds}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month}"
        / f"date={day}"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def _write_bronze_empty_minutes(
    root: Path,
    *,
    dataset_type: str,
    exchange: str,
    instrument_type: str,
    symbol: str,
    start: datetime,
    end: datetime,
) -> None:
    write_empty_trade_minutes(
        lake_root=str(root),
        dataset_type=dataset_type,
        exchange=exchange,
        instrument_type=instrument_type,
        symbol=symbol,
        timeframe="tick",
        start_open_ms=int(start.timestamp() * 1000),
        end_open_ms=int(end.timestamp() * 1000),
        checked_at=datetime(2026, 7, 23, 12, 0, tzinfo=UTC),
    )


def test_build_silver_for_symbol_writes_monthly_parquet_and_aggregated_report(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    base = {
        "schema_version": "v1",
        "dataset_type": "perps_ohlcv",
        "exchange": "deribit",
        "symbol": symbol,
        "instrument_type": "perp",
        "timeframe": "1m",
        "run_id": "r1",
        "source_endpoint": "public_market_data",
        "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        "volume": 1.0,
        "quote_volume": 1.0,
        "trade_count": 1,
    }
    rows_day1 = [
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
            "open_price": 100.0,
            "high_price": 101.0,
            "low_price": 99.0,
            "close_price": 100.5,
        },
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "open_price": 101.0,
            "high_price": 102.0,
            "low_price": 100.0,
            "close_price": 101.5,
        },
    ]
    rows_day2 = [
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),  # duplicate key
            "close_time": datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "open_price": 101.1,
            "high_price": 102.1,
            "low_price": 100.1,
            "close_price": 101.6,
        },
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 2, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "open_price": 100.0,
            "high_price": 99.0,  # invalid high
            "low_price": 98.0,
            "close_price": 100.5,
        },
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 3, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 4, tzinfo=UTC),
            "open_price": None,  # null price
            "high_price": 103.0,
            "low_price": 99.0,
            "close_price": 101.0,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="perps_ohlcv",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows_day1,
    )
    _write_bronze_day_file(
        bronze,
        market="perps_ohlcv",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        month="2026-05",
        day="2026-05-02",
        rows=rows_day2,
    )

    assert discover_symbols(str(bronze), "perps_ohlcv", "deribit") == [symbol]
    assert discover_months(str(bronze), "perps_ohlcv", "deribit", symbol) == ["2026-05"]

    report = build_silver_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        market="perps_ohlcv",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
    )
    assert report.rows_in == 5
    assert report.rows_out == 2
    assert report.duplicates_removed == 1
    assert report.invalid_ohlc_rows == 1
    assert report.null_price_rows == 1
    assert report.period_start == "2026-05"
    assert report.period_end == "2026-05"
    assert report.symbols == [symbol]
    assert "close_price" in report.columns

    silver_file = (
        silver
        / "dataset_type=perps_ohlcv"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    assert silver_file.exists()
    written = pl.read_parquet(silver_file)
    assert written.height == 2

    manifest_paths, plot_paths = write_monthly_sidecars(
        silver_root=str(silver),
        market="perps_ohlcv",
        exchange="deribit",
        symbol=symbol,
        report=report,
        write_manifest=True,
        plot=False,
    )
    assert len(manifest_paths) == 1
    assert plot_paths == []
    monthly_manifest_path = Path(manifest_paths[0])
    assert monthly_manifest_path.exists()
    assert monthly_manifest_path.name == f"{symbol}-2026-05.json"
    monthly_payload = json.loads(monthly_manifest_path.read_text(encoding="utf-8"))
    assert monthly_payload["dataset"] == "perps_ohlcv_1m"
    assert "column_hash" in monthly_payload
    assert "source_silver_datasets" in monthly_payload
    assert "feature_metadata" in monthly_payload
    assert "plot_generated" in monthly_payload


def test_build_funding_observed_and_1m_feature(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "funding",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 10, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_funding",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "funding_rate": 0.001,
            "index_price": 100.0,
            "mark_price": 99.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "funding",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 20, tzinfo=UTC),
            "run_id": "r2",
            "source_endpoint": "public_funding",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "funding_rate": 0.002,
            "index_price": 101.0,
            "mark_price": 100.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "funding",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 8, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 8, 10, tzinfo=UTC),
            "run_id": "r3",
            "source_endpoint": "public_funding",
            "open_time": datetime(2026, 5, 1, 8, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 8, 0, tzinfo=UTC),
            "timeframe": "1m",
            "funding_rate": 0.003,
            "index_price": 102.0,
            "mark_price": 101.0,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="funding",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="funding",
        instrument_type="perp",
    )

    observed_report = build_funding_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
    )
    assert observed_report.rows_in == 3
    assert observed_report.rows_out == 2
    assert observed_report.duplicates_removed == 1
    assert "funding_time" in observed_report.columns

    feature_report = build_funding_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="1m",
    )
    assert feature_report.rows_out > 0
    assert "funding_rate_last_known" in feature_report.columns

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
    assert feature_file.exists()
    feature = pl.read_parquet(feature_file)
    assert "funding_rate_last_known" in feature.columns
    assert "funding_observed_at" in feature.columns
    assert "minutes_since_funding" in feature.columns
    assert "is_funding_observation_minute" in feature.columns

    observed_file = (
        silver
        / "dataset_type=funding_observed"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    observed = pl.read_parquet(observed_file)
    assert feature.select(pl.col("timestamp").max()).item() == observed.select(pl.col("funding_time").max()).item()


def test_build_open_interest_observed_and_1m_feature(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "btc_perpetual"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "open_interest",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 30, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1000.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "open_interest",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 35, tzinfo=UTC),
            "run_id": "r2",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1000.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "open_interest",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 2, 30, tzinfo=UTC),
            "run_id": "r3",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1025.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "open_interest",
            "exchange": "Deribit",
            "symbol": None,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 3, 30, tzinfo=UTC),
            "run_id": "r4",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1030.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "open_interest",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 4, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 4, 30, tzinfo=UTC),
            "run_id": "r5",
            "source_endpoint": "public_open_interest",
            "open_time": None,
            "close_time": datetime(2026, 5, 1, 0, 4, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1035.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "open_interest",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 5, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 5, 30, tzinfo=UTC),
            "run_id": "r6",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 5, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 5, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": -1.0,
            "open_interest_value": 0.0,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="open_interest",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="open_interest",
        instrument_type="perp",
    )

    observed_report = build_open_interest_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
    )
    assert observed_report.rows_in == 6
    assert observed_report.rows_out == 2
    assert observed_report.duplicates_removed == 1
    assert observed_report.invalid_ohlc_rows == 3
    assert "open_interest_source_timestamp" in observed_report.columns

    feature_report = build_open_interest_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        observed_timeframe="1m",
    )
    assert feature_report.rows_out > 0
    assert "open_interest_is_observed" in feature_report.columns
    assert "minutes_since_open_interest_observation" in feature_report.columns

    observed_file = (
        silver
        / "dataset_type=open_interest_observed"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-PERPETUAL-2026-05.parquet"
    )
    observed = pl.read_parquet(observed_file)
    assert observed["symbol"].to_list() == ["BTC-PERPETUAL", "BTC-PERPETUAL"]

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
    minute_0 = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 0, tzinfo=UTC))
    minute_1 = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 1, tzinfo=UTC))
    minute_2 = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 2, tzinfo=UTC))
    assert minute_0.select("open_interest_is_observed").item() is True
    assert minute_0.select("open_interest_is_ffill").item() is False
    assert minute_1.select("open_interest_is_observed").item() is False
    assert minute_1.select("open_interest_is_ffill").item() is True
    assert minute_1.select("minutes_since_open_interest_observation").item() == 1
    assert minute_2.select("open_interest_is_observed").item() is True


def test_build_perps_trades_1m_feature_for_symbol(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "perps_trades",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 11, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_trades",
            "open_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
            "timeframe": "tick",
            "trade_id": "t1",
            "price": 100.0,
            "quantity": 2.0,
            "side": "buy",
            "is_maker": True,
        },
        {
            "schema_version": "v1",
            "dataset_type": "perps_trades",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 21, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_trades",
            "open_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "timeframe": "tick",
            "trade_id": "t2",
            "price": 101.0,
            "quantity": 1.0,
            "side": "sell",
            "is_maker": False,
        },
        {
            "schema_version": "v1",
            "dataset_type": "perps_trades",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 1, 5, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 1, 6, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_trades",
            "open_time": datetime(2026, 5, 1, 0, 1, 5, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 1, 5, tzinfo=UTC),
            "timeframe": "tick",
            "trade_id": "t3",
            "price": 102.0,
            "quantity": 3.0,
            "side": "buy",
            "is_maker": True,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="perps_trades",
        exchange="deribit",
        symbol=symbol,
        timeframe="tick",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="perps_trades",
        instrument_type="perp",
    )
    observed_report = build_perps_trades_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        instrument_type="perp",
        timeframe="tick",
    )
    assert observed_report.dataset == "perps_trades_observed"
    report = build_perps_trades_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="tick",
    )
    assert report.dataset == "perps_trades_1m_feature"
    assert report.rows_in == 3
    assert report.rows_out == 2
    out_file = (
        silver
        / "dataset_type=perps_trades_1m_feature"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    assert out_file.exists()


def test_silver_discovery_helpers_cover_missing_and_legacy_layouts(tmp_path: Path) -> None:
    """Silver discovery should handle absent roots and both month directory layouts."""

    assert discover_symbols(str(tmp_path / "missing"), "perps_ohlcv", "deribit") == []
    assert discover_months(str(tmp_path / "missing"), "perps_ohlcv", "deribit", "BTC") == []
    root = (
        tmp_path
        / "dataset_type=perps_ohlcv"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC"
        / "timeframe=1m"
    )
    (root / "year=2026" / "month=2026-05").mkdir(parents=True)
    (root / "month=2026-06").mkdir(parents=True)
    assert discover_symbols(str(tmp_path), "perps_ohlcv", "deribit") == ["BTC"]
    assert discover_months(str(tmp_path), "perps_ohlcv", "deribit", "BTC") == ["2026-05", "2026-06"]


def test_build_trade_observed_frame_filters_invalid_and_deduplicates() -> None:
    ts = datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC)
    frame = pl.DataFrame(
        [
            {
                "open_time": ts,
                "ingested_at": datetime(2026, 5, 1, 0, 0, 11, tzinfo=UTC),
                "price": 100.0,
                "quantity": 1.0,
                "trade_id": "dup",
                "side": "BUY",
                "symbol": "BTC-PERPETUAL",
                "exchange": "DERIBIT",
                "instrument_type": "PERP",
            },
            {
                "open_time": ts,
                "ingested_at": datetime(2026, 5, 1, 0, 0, 12, tzinfo=UTC),
                "price": 101.0,
                "quantity": 1.0,
                "trade_id": "dup",
                "side": "buy",
                "symbol": "BTC-PERPETUAL",
                "exchange": "deribit",
                "instrument_type": "perp",
            },
            {
                "open_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
                "ingested_at": datetime(2026, 5, 1, 0, 0, 21, tzinfo=UTC),
                "price": -1.0,
                "quantity": 1.0,
                "trade_id": "bad",
                "side": "sell",
                "symbol": "BTC-PERPETUAL",
                "exchange": "deribit",
                "instrument_type": "perp",
            },
        ]
    )

    observed, invalid_rows, cleaned_rows = _build_trade_observed_frame(pl, frame)
    assert invalid_rows == 1
    assert cleaned_rows == 2
    assert observed.height == 1
    row = observed.row(0, named=True)
    assert row["price"] == 101.0
    assert row["exchange"] == "deribit"
    assert row["instrument_type"] == "perp"


def test_build_trade_feature_frame_aggregates_minute_flow_features() -> None:
    frame = pl.DataFrame(
        [
            {
                "trade_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_type": "perp",
                "price": 100.0,
                "quantity": 2.0,
                "side": "buy",
            },
            {
                "trade_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_type": "perp",
                "price": 101.0,
                "quantity": 1.0,
                "side": "sell",
            },
        ]
    )

    feature = _build_trade_feature_frame(pl, frame, symbol="BTC")
    assert feature.height == 1
    row = feature.row(0, named=True)
    assert row["open_price"] == 100.0
    assert row["close_price"] == 101.0
    assert row["volume"] == 3.0
    assert row["quote_volume"] == 301.0
    assert row["buy_volume"] == 2.0
    assert row["sell_volume"] == 1.0
    assert row["buy_trade_count"] == 1
    assert row["sell_trade_count"] == 1
    assert row["buy_volume_share"] == pytest.approx(2.0 / 3.0)


def test_build_trade_feature_frame_adds_confirmed_empty_minutes_with_past_close() -> None:
    frame = pl.DataFrame(
        [
            {
                "trade_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
                "exchange": "deribit",
                "symbol": "BTC",
                "instrument_type": "perp",
                "price": 100.0,
                "quantity": 2.0,
                "side": "buy",
            }
        ]
    )
    empty_minutes = pl.DataFrame(
        [
            {
                "dataset_type": "perps_trades",
                "exchange": "deribit",
                "instrument_type": "perp",
                "symbol": "BTC",
                "timeframe": "tick",
                "minute": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
                "status": "confirmed_empty",
                "checked_at": datetime(2026, 7, 23, 12, 0, tzinfo=UTC),
                "request_start_ms": 0,
                "request_end_ms": 0,
                "row_count": 0,
            }
        ]
    )

    feature = _build_trade_feature_frame(pl, frame, symbol="BTC", empty_minutes_frame=empty_minutes)

    assert feature.height == 2
    empty_row = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 1, tzinfo=UTC)).row(0, named=True)
    assert empty_row["open_price"] == 100.0
    assert empty_row["high_price"] == 100.0
    assert empty_row["low_price"] == 100.0
    assert empty_row["close_price"] == 100.0
    assert empty_row["volume"] == 0.0
    assert empty_row["quote_volume"] == 0.0
    assert empty_row["trade_count"] == 0
    assert empty_row["buy_volume_share"] == 0.0


def test_build_perps_trades_1m_feature_filters_invalid_and_deduplicates(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    base = {
        "schema_version": "v1",
        "dataset_type": "perps_trades",
        "exchange": "deribit",
        "symbol": symbol,
        "instrument_type": "perp",
        "source_endpoint": "public_trades",
        "timeframe": "tick",
    }
    ts0 = datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC)
    rows = [
        {
            **base,
            "event_time": ts0,
            "ingested_at": datetime(2026, 5, 1, 0, 0, 11, tzinfo=UTC),
            "run_id": "r1",
            "open_time": ts0,
            "close_time": ts0,
            "trade_id": "dup",
            "price": 100.0,
            "quantity": 1.0,
            "side": "buy",
            "is_maker": True,
        },
        {
            **base,
            "event_time": ts0,
            "ingested_at": datetime(2026, 5, 1, 0, 0, 12, tzinfo=UTC),
            "run_id": "r2",
            "open_time": ts0,
            "close_time": ts0,
            "trade_id": "dup",
            "price": 101.0,
            "quantity": 1.0,
            "side": "buy",
            "is_maker": True,
        },
        {
            **base,
            "event_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 21, tzinfo=UTC),
            "run_id": "r3",
            "open_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "trade_id": "bad",
            "price": -1.0,
            "quantity": 1.0,
            "side": "sell",
            "is_maker": False,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="perps_trades",
        exchange="deribit",
        symbol=symbol,
        timeframe="tick",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="perps_trades",
        instrument_type="perp",
    )
    observed_report = build_perps_trades_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        instrument_type="perp",
        timeframe="tick",
    )
    assert observed_report.rows_in == 3
    assert observed_report.rows_out == 1
    assert observed_report.duplicates_removed == 1
    assert observed_report.invalid_ohlc_rows == 1
    report = build_perps_trades_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="tick",
    )
    assert report.rows_in == 1
    assert report.rows_out == 1


def test_build_perps_trades_1m_feature_includes_confirmed_empty_minutes(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    trade_time = datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC)
    _write_bronze_day_file(
        bronze,
        market="perps_trades",
        exchange="deribit",
        symbol=symbol,
        timeframe="tick",
        month="2026-05",
        day="2026-05-01",
        rows=[
            {
                "schema_version": "v1",
                "dataset_type": "perps_trades",
                "exchange": "deribit",
                "symbol": symbol,
                "instrument_type": "perp",
                "event_time": trade_time,
                "ingested_at": datetime(2026, 5, 1, 0, 0, 11, tzinfo=UTC),
                "run_id": "r1",
                "source_endpoint": "public_trades",
                "open_time": trade_time,
                "close_time": trade_time,
                "timeframe": "tick",
                "trade_id": "t1",
                "price": 100.0,
                "quantity": 2.0,
                "side": "buy",
                "is_maker": True,
            }
        ],
        dataset_type="perps_trades",
        instrument_type="perp",
    )
    _write_bronze_empty_minutes(
        bronze,
        dataset_type="perps_trades",
        exchange="deribit",
        instrument_type="perp",
        symbol=symbol,
        start=datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
        end=datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
    )
    build_perps_trades_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        instrument_type="perp",
        timeframe="tick",
    )

    report = build_perps_trades_1m_feature_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="tick",
    )

    assert report.rows_in == 2
    assert report.rows_out == 2
    out_file = (
        silver
        / "dataset_type=perps_trades_1m_feature"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    feature = pl.read_parquet(out_file)
    empty_row = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 1, tzinfo=UTC)).row(0, named=True)
    assert empty_row["trade_count"] == 0
    assert empty_row["volume"] == 0.0
    assert empty_row["close_price"] == 100.0


def test_build_perps_trades_1m_feature_fills_empty_month_from_previous_close(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    trade_time = datetime(2026, 5, 31, 23, 59, 10, tzinfo=UTC)
    _write_bronze_day_file(
        bronze,
        market="perps_trades",
        exchange="deribit",
        symbol=symbol,
        timeframe="tick",
        month="2026-05",
        day="2026-05-31",
        rows=[
            {
                "schema_version": "v1",
                "dataset_type": "perps_trades",
                "exchange": "deribit",
                "symbol": symbol,
                "instrument_type": "perp",
                "event_time": trade_time,
                "ingested_at": datetime(2026, 5, 31, 23, 59, 11, tzinfo=UTC),
                "run_id": "r1",
                "source_endpoint": "public_trades",
                "open_time": trade_time,
                "close_time": trade_time,
                "timeframe": "tick",
                "trade_id": "t1",
                "price": 100.0,
                "quantity": 2.0,
                "side": "buy",
                "is_maker": True,
            }
        ],
        dataset_type="perps_trades",
        instrument_type="perp",
    )
    _write_bronze_empty_minutes(
        bronze,
        dataset_type="perps_trades",
        exchange="deribit",
        instrument_type="perp",
        symbol=symbol,
        start=datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        end=datetime(2026, 6, 1, 0, 0, 59, 999000, tzinfo=UTC),
    )
    build_perps_trades_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        instrument_type="perp",
        timeframe="tick",
    )

    report = build_perps_trades_1m_feature_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="tick",
    )

    assert report.months_processed == ["2026-05", "2026-06"]
    june_file = (
        silver
        / "dataset_type=perps_trades_1m_feature"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / f"{symbol}-2026-06.parquet"
    )
    june_row = pl.read_parquet(june_file).row(0, named=True)
    assert june_row["trade_count"] == 0
    assert june_row["close_price"] == 100.0


def test_build_options_trades_1m_feature_for_symbol(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "options_trades",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "option",
            "event_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 11, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_options_trades",
            "open_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC),
            "timeframe": "tick",
            "trade_id": "o1",
            "price": 10.0,
            "quantity": 2.0,
            "side": "buy",
            "is_maker": True,
            "instrument_name": "BTC-31MAY26-100000-C",
            "expiry": "31MAY26",
            "strike": 100000.0,
            "option_type": "call",
        },
        {
            "schema_version": "v1",
            "dataset_type": "options_trades",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "option",
            "event_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 21, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_options_trades",
            "open_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, 20, tzinfo=UTC),
            "timeframe": "tick",
            "trade_id": "o2",
            "price": 12.0,
            "quantity": 1.0,
            "side": "sell",
            "is_maker": False,
            "instrument_name": "BTC-31MAY26-105000-C",
            "expiry": "31MAY26",
            "strike": 105000.0,
            "option_type": "call",
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="options_trades",
        exchange="deribit",
        symbol=symbol,
        timeframe="tick",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="options_trades",
        instrument_type="option",
    )

    observed_report = build_perps_trades_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        instrument_type="option",
        timeframe="tick",
        bronze_dataset_type="options_trades",
        output_dataset_type="options_trades_observed",
    )
    assert observed_report.dataset == "options_trades_observed"

    feature_report = build_perps_trades_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="tick",
        observed_dataset_type="options_trades_observed",
        output_dataset_type="options_trades_1m_feature",
    )
    assert feature_report.dataset == "options_trades_1m_feature"
    assert feature_report.rows_in == 2
    assert feature_report.rows_out == 1

    out_file = (
        silver
        / "dataset_type=options_trades_1m_feature"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    assert out_file.exists()


def test_build_options_trades_1m_feature_includes_confirmed_empty_minutes(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC"
    trade_time = datetime(2026, 5, 1, 0, 0, 10, tzinfo=UTC)
    _write_bronze_day_file(
        bronze,
        market="options_trades",
        exchange="deribit",
        symbol=symbol,
        timeframe="tick",
        month="2026-05",
        day="2026-05-01",
        rows=[
            {
                "schema_version": "v1",
                "dataset_type": "options_trades",
                "exchange": "deribit",
                "symbol": symbol,
                "instrument_type": "option",
                "event_time": trade_time,
                "ingested_at": datetime(2026, 5, 1, 0, 0, 11, tzinfo=UTC),
                "run_id": "r1",
                "source_endpoint": "public_options_trades",
                "open_time": trade_time,
                "close_time": trade_time,
                "timeframe": "tick",
                "trade_id": "o1",
                "price": 10.0,
                "quantity": 2.0,
                "side": "buy",
                "is_maker": True,
                "instrument_name": "BTC-31MAY26-100000-C",
                "expiry": "31MAY26",
                "strike": 100000.0,
                "option_type": "call",
            }
        ],
        dataset_type="options_trades",
        instrument_type="option",
    )
    _write_bronze_empty_minutes(
        bronze,
        dataset_type="options_trades",
        exchange="deribit",
        instrument_type="option",
        symbol=symbol,
        start=datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
        end=datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
    )
    build_perps_trades_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        instrument_type="option",
        timeframe="tick",
        bronze_dataset_type="options_trades",
        output_dataset_type="options_trades_observed",
    )

    report = build_perps_trades_1m_feature_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="tick",
        observed_dataset_type="options_trades_observed",
        output_dataset_type="options_trades_1m_feature",
        bronze_dataset_type="options_trades",
        instrument_type="option",
    )

    assert report.rows_in == 2
    assert report.rows_out == 2
    out_file = (
        silver
        / "dataset_type=options_trades_1m_feature"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    feature = pl.read_parquet(out_file)
    empty_row = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 1, tzinfo=UTC)).row(0, named=True)
    assert empty_row["trade_count"] == 0
    assert empty_row["volume"] == 0.0
    assert empty_row["close_price"] == 10.0


def test_build_volatility_observed_for_symbol(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "volatility_index_data",
            "exchange": "Deribit",
            "symbol": symbol,
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
            "symbol": symbol,
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
            "symbol": symbol,
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
        symbol=symbol,
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="volatility_index_data",
        instrument_type="perp",
    )

    report = build_volatility_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        bronze_dataset_type="volatility_index_data",
        output_dataset_type="volatility_index_data_observed",
    )
    assert report.dataset == "volatility_index_data_observed"
    assert report.rows_in == 3
    assert report.rows_out == 1
    assert report.duplicates_removed == 1
    assert report.invalid_ohlc_rows == 1
    assert "volatility_value" in report.columns

    out_file = (
        silver
        / "dataset_type=volatility_index_data_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    assert out_file.exists()
    observed = pl.read_parquet(out_file)
    assert observed.height == 1
    assert observed["volatility_value"].to_list() == [56.0]
    assert observed["volatility_open"].to_list() == [55.0]
    assert observed["volatility_high"].to_list() == [57.0]
    assert observed["volatility_low"].to_list() == [54.5]
    assert observed["volatility_close"].to_list() == [56.0]


def test_silver_delegating_builders_reject_invalid_reports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Every public adapter must enforce the shared SilverBuildReport contract."""

    builders = [
        ("silver_funding", "build_funding_observed_for_symbol"),
        ("silver_funding", "build_funding_1m_feature_for_symbol"),
        ("silver_open_interest", "build_open_interest_observed_for_symbol"),
        ("silver_open_interest", "build_open_interest_1m_feature_for_symbol"),
        ("silver_volatility", "build_volatility_observed_for_symbol"),
        ("silver_volatility", "build_volatility_snapshot_observed_for_symbol"),
        ("silver_volatility", "build_volatility_index_1m_feature_for_symbol"),
        ("silver_realized_volatility", "build_realized_volatility_1m_feature_for_symbol"),
        ("silver_historical_prediction", "build_historical_prediction_1m_feature_for_symbol"),
        ("silver_iv_rv", "build_iv_rv_1m_feature_for_symbol"),
        ("silver_index_price", "build_index_price_observed_for_symbol"),
        ("silver_index_price", "build_index_price_1m_feature_for_symbol"),
        ("silver_futures_summary", "build_futures_summary_observed_for_symbol"),
        ("silver_futures_summary", "build_futures_summary_1m_feature_for_symbol"),
        ("silver_options_ticker", "build_options_ticker_observed_for_symbol"),
        ("silver_options_ticker", "build_options_instrument_ticker_observed_for_symbol"),
        ("silver_options_surface", "build_options_surface_1m_feature_for_symbol"),
        ("silver_l2", "build_l2_observed_for_symbol"),
        ("silver_l2", "build_l2_1m_feature_for_symbol"),
        ("silver_recent_trades", "build_recent_trade_snapshot_observed_for_symbol"),
        ("silver_instrument_metadata", "build_instrument_metadata_observed_for_symbol"),
        ("silver_historical_volatility", "build_historical_volatility_observed_for_symbol"),
    ]
    values: dict[str, object] = {
        "bronze_root": str(tmp_path / "bronze"),
        "silver_root": str(tmp_path / "silver"),
        "exchange": "deribit",
        "symbol": "BTC",
        "timeframe": "1m",
        "observed_timeframe": "8h",
        "cutoff_time": datetime(2026, 5, 1, tzinfo=UTC),
        "bronze_dataset_type": "volatility_index_data",
        "output_dataset_type": "volatility_index_data_observed",
    }

    for module_name, builder_name in builders:
        module = getattr(silver_service, module_name)
        monkeypatch.setattr(module, builder_name, lambda **_kwargs: None)
        public_name = builder_name
        if builder_name == "build_l2_observed_for_symbol":
            public_name = "build_perps_l2_observed_for_symbol"
        elif builder_name == "build_l2_1m_feature_for_symbol":
            public_name = "build_perps_l2_1m_feature_for_symbol"
        public_builder = getattr(silver_service, public_name)
        kwargs = {
            name: values[name]
            for name, parameter in inspect.signature(public_builder).parameters.items()
            if parameter.default is inspect.Parameter.empty and name in values
        }
        with pytest.raises(TypeError, match="unexpected report"):
            public_builder(**kwargs)
