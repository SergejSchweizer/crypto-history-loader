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
