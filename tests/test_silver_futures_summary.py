"""Tests for futures-summary snapshot Silver transformations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import (
    SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS,
    SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS,
)
from application.services.silver_service import (
    build_futures_summary_1m_feature_for_symbol,
    build_futures_summary_observed_for_symbol,
    discover_futures_summary_symbols,
)

pl = pytest.importorskip("polars")


def _write_futures_summary_hour_file(
    root: Path,
    *,
    exchange: str,
    currency: str,
    month: str,
    day: str,
    hour: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    target = (
        root
        / "dataset_type=futures_summary_snapshot_1m"
        / f"exchange={exchange}"
        / "instrument_type=future"
        / f"currency={currency}"
        / "source=rest_get_book_summary_by_currency"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month.split('-', 1)[1]}"
        / f"date={day}"
        / f"hour={hour}"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def test_build_futures_summary_handles_optional_fields_and_duplicates(tmp_path: Path) -> None:
    """Futures summary should tolerate missing optional fields and dedupe snapshots."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 6, 12, 19, 48, tzinfo=UTC)
    t1 = datetime(2026, 6, 12, 19, 50, tzinfo=UTC)
    common = {
        "schema_version": "v1",
        "dataset_type": "futures_summary_snapshot_1m",
        "exchange": "deribit",
        "source": "rest_get_book_summary_by_currency",
        "currency": "BTC",
        "requested_currency": "BTC",
        "source_currency": "BTC",
        "instrument_name": "BTC-27JUN26",
        "instrument_type": "future",
        "exchange_creation_time": t0,
        "run_id": "r",
        "bid_price": 100.0,
        "ask_price": 102.0,
        "mid_price": 101.0,
        "last": 101.0,
        "high": 110.0,
        "low": 90.0,
        "price_change": 1.0,
        "raw_payload_hash": "h",
    }
    _write_futures_summary_hour_file(
        bronze,
        exchange="deribit",
        currency="BTC",
        month="2026-06",
        day="2026-06-12",
        hour="19",
        rows=[
            {
                **common,
                "snapshot_time": t0,
                "ingested_at": datetime(2026, 6, 12, 19, 48, 1, tzinfo=UTC),
                "mark_price": 101.0,
                "open_interest": 10.0,
                "volume": 1.0,
                "volume_usd": 101.0,
                "estimated_delivery_price": 100.0,
            },
            {
                **common,
                "snapshot_time": t0,
                "ingested_at": datetime(2026, 6, 12, 19, 48, 2, tzinfo=UTC),
                "mark_price": 103.0,
                "open_interest": 11.0,
                "volume": 2.0,
                "volume_usd": 206.0,
                "estimated_delivery_price": 100.0,
            },
            {
                **common,
                "snapshot_time": t1,
                "ingested_at": datetime(2026, 6, 12, 19, 50, 1, tzinfo=UTC),
                "mark_price": 110.0,
                "open_interest": 12.0,
                "volume": 3.0,
                "volume_usd": 330.0,
                "estimated_delivery_price": 100.0,
            },
        ],
    )

    assert discover_futures_summary_symbols(bronze_root=str(bronze), exchange="deribit") == ["BTC"]

    observed_report = build_futures_summary_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )
    feature_report = build_futures_summary_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert observed_report.rows_in == 3
    assert observed_report.rows_out == 2
    assert observed_report.duplicates_removed == 1
    assert feature_report.rows_out == 3
    observed = pl.read_parquet(
        silver
        / "dataset_type=futures_summary_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )
    feature = pl.read_parquet(
        silver
        / "dataset_type=futures_summary_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-06"
        / "BTC-2026-06.parquet"
    )
    assert observed.columns == SILVER_FUTURES_SUMMARY_OBSERVED_COLUMNS
    assert observed["mark_price"].to_list() == [103.0, 110.0]
    assert observed["funding_rate"].to_list() == [None, None]
    assert feature.columns == SILVER_FUTURES_SUMMARY_FEATURE_COLUMNS
    assert feature["mark_index_spread"].to_list() == [3.0, 3.0, 10.0]
    assert feature["mark_index_ratio"].to_list() == [1.03, 1.03, 1.1]
    assert feature["summary_is_observed"].to_list() == [True, False, True]
    assert feature["minutes_since_summary_observation"].to_list() == [0, 1, 0]


def test_build_futures_summary_ignores_unused_bronze_schema_drift(tmp_path: Path) -> None:
    """Unused Bronze fields should not break month-level parquet reads when their inferred dtype changes."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 7, 9, 12, tzinfo=UTC)
    t1 = datetime(2026, 7, 9, 13, tzinfo=UTC)
    common = {
        "schema_version": "v1",
        "dataset_type": "futures_summary_snapshot_1m",
        "exchange": "deribit",
        "source": "rest_get_book_summary_by_currency",
        "currency": "SOL",
        "requested_currency": "SOL",
        "source_currency": "SOL",
        "instrument_name": "SOL_USDC-31JUL26",
        "instrument_type": "future",
        "exchange_creation_time": t0,
        "run_id": "r",
        "bid_price": 100.0,
        "ask_price": 102.0,
        "mid_price": 101.0,
        "last": 101.0,
        "low": 90.0,
        "price_change": 1.0,
        "raw_payload_hash": "h",
        "mark_price": 101.0,
        "open_interest": 10.0,
        "volume": 1.0,
        "volume_usd": 101.0,
        "estimated_delivery_price": 100.0,
    }
    _write_futures_summary_hour_file(
        bronze,
        exchange="deribit",
        currency="SOL",
        month="2026-07",
        day="2026-07-09",
        hour="12",
        rows=[
            {
                **common,
                "snapshot_time": t0,
                "ingested_at": datetime(2026, 7, 9, 12, 0, 1, tzinfo=UTC),
                "high": None,
            },
        ],
    )
    _write_futures_summary_hour_file(
        bronze,
        exchange="deribit",
        currency="SOL",
        month="2026-07",
        day="2026-07-09",
        hour="13",
        rows=[
            {
                **common,
                "snapshot_time": t1,
                "ingested_at": datetime(2026, 7, 9, 13, 0, 1, tzinfo=UTC),
                "high": 110.0,
            },
        ],
    )

    observed_report = build_futures_summary_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="SOL",
    )

    assert observed_report.rows_in == 2
    assert observed_report.rows_out == 2


def test_build_futures_summary_observed_drops_blank_symbols_and_missing_timestamps(tmp_path: Path) -> None:
    """Observed summaries should publish only rows with usable snapshot identities."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    timestamp = datetime(2026, 8, 10, 9, 30, tzinfo=UTC)
    _write_futures_summary_hour_file(
        bronze,
        exchange="deribit",
        currency="ETH",
        month="2026-08",
        day="2026-08-10",
        hour="09",
        rows=[
            {
                "snapshot_time": timestamp,
                "instrument_name": " eth-29aug26 ",
                "exchange": " DERIBIT ",
                "instrument_type": " FUTURE ",
                "mark_price": 101.0,
                "underlying_price": 100.0,
                "estimated_delivery_price": 99.0,
                "open_interest": 4.0,
                "volume": 2.0,
                "volume_usd": 202.0,
                "interest_rate": 0.001,
                "ingested_at": timestamp,
                "source": "rest_get_book_summary_by_currency",
            },
            {
                "snapshot_time": timestamp,
                "instrument_name": "   ",
                "exchange": "deribit",
                "instrument_type": "future",
                "ingested_at": timestamp,
                "source": "rest_get_book_summary_by_currency",
            },
            {
                "snapshot_time": None,
                "instrument_name": "ETH-29AUG26",
                "exchange": "deribit",
                "instrument_type": "future",
                "ingested_at": timestamp,
                "source": "rest_get_book_summary_by_currency",
            },
        ],
    )

    report = build_futures_summary_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="eth",
    )

    observed = pl.read_parquet(
        silver
        / "dataset_type=futures_summary_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=ETH"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-08"
        / "ETH-2026-08.parquet"
    )
    assert report.rows_in == 3
    assert report.rows_out == 1
    assert observed.to_dicts() == [
        {
            "timestamp": timestamp,
            "exchange": "deribit",
            "symbol": "ETH-29AUG26",
            "instrument_type": "future",
            "mark_price": 101.0,
            "index_price": 100.0,
            "open_interest": 4.0,
            "volume": 2.0,
            "turnover": 202.0,
            "funding_rate": 0.001,
            "ingested_at": timestamp,
            "source_endpoint": "rest_get_book_summary_by_currency",
        }
    ]


def test_build_futures_summary_feature_skips_non_datetime_observed_timestamps(tmp_path: Path) -> None:
    """Feature building should not publish a partition whose observed range is invalid."""

    silver = tmp_path / "silver"
    observed_path = (
        silver
        / "dataset_type=futures_summary_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-08"
        / "BTC-2026-08.parquet"
    )
    observed_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "timestamp": ["not-a-timestamp"],
            "exchange": ["deribit"],
            "symbol": ["BTC-29AUG26"],
            "instrument_type": ["future"],
        }
    ).write_parquet(observed_path)

    report = build_futures_summary_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )

    assert report.months_processed == ["2026-08"]
    assert report.rows_in == 0
    assert report.rows_out == 0
    assert not (
        silver
        / "dataset_type=futures_summary_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-08"
        / "BTC-2026-08.parquet"
    ).exists()
