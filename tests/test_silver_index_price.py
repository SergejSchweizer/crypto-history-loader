"""Tests for index-price snapshot Silver transformations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_INDEX_PRICE_FEATURE_COLUMNS, SILVER_INDEX_PRICE_OBSERVED_COLUMNS
from application.services.silver_service import (
    build_index_price_1m_feature_for_symbol,
    build_index_price_observed_for_symbol,
    discover_index_price_symbols,
)

pl = pytest.importorskip("polars")


def _write_index_price_hour_file(
    root: Path,
    *,
    exchange: str,
    index_name: str,
    month: str,
    day: str,
    hour: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    target = (
        root
        / "dataset_type=index_price_snapshot_1m"
        / f"exchange={exchange}"
        / f"index_name={index_name}"
        / f"year={month.split('-', 1)[0]}"
        / f"month={month.split('-', 1)[1]}"
        / f"date={day}"
        / f"hour={hour}"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def test_build_index_price_observed_deduplicates_and_feature_forward_fills(tmp_path: Path) -> None:
    """Index price snapshots should dedupe by newest ingest and produce freshness-aware features."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 24, 14, 48, tzinfo=UTC)
    t1 = datetime(2026, 5, 24, 14, 50, tzinfo=UTC)
    _write_index_price_hour_file(
        bronze,
        exchange="deribit",
        index_name="btc_usd",
        month="2026-05",
        day="2026-05-24",
        hour="14",
        rows=[
            {
                "schema_version": "v1",
                "dataset_type": "index_price_snapshot_1m",
                "exchange": "deribit",
                "source": "rest_get_index_price",
                "index_name": "btc_usd",
                "snapshot_time": t0,
                "event_time": t0,
                "price": 100.0,
                "ingested_at": datetime(2026, 5, 24, 14, 48, 1, tzinfo=UTC),
                "run_id": "old",
                "raw_payload_hash": "old",
                "extra_field": "ignored",
            },
            {
                "schema_version": "v1",
                "dataset_type": "index_price_snapshot_1m",
                "exchange": "deribit",
                "source": "rest_get_index_price",
                "index_name": "btc_usd",
                "snapshot_time": t0,
                "event_time": t0,
                "price": 101.0,
                "ingested_at": datetime(2026, 5, 24, 14, 48, 2, tzinfo=UTC),
                "run_id": "new",
                "raw_payload_hash": "new",
                "extra_field": "ignored",
            },
            {
                "schema_version": "v1",
                "dataset_type": "index_price_snapshot_1m",
                "exchange": "deribit",
                "source": "rest_get_index_price",
                "index_name": "btc_usd",
                "snapshot_time": t1,
                "event_time": t1,
                "price": 110.0,
                "ingested_at": datetime(2026, 5, 24, 14, 50, 1, tzinfo=UTC),
                "run_id": "later",
                "raw_payload_hash": "later",
                "extra_field": "ignored",
            },
        ],
    )

    assert discover_index_price_symbols(bronze_root=str(bronze), exchange="deribit") == ["BTC"]

    observed_report = build_index_price_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )
    feature_report = build_index_price_1m_feature_for_symbol(
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
        / "dataset_type=index_price_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    feature = pl.read_parquet(
        silver
        / "dataset_type=index_price_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    assert observed.columns == SILVER_INDEX_PRICE_OBSERVED_COLUMNS
    assert observed["index_price"].to_list() == [101.0, 110.0]
    assert feature.columns == SILVER_INDEX_PRICE_FEATURE_COLUMNS
    assert feature["index_price"].to_list() == [101.0, 101.0, 110.0]
    assert feature["index_price_is_observed"].to_list() == [True, False, True]
    assert feature["minutes_since_index_price_observation"].to_list() == [0, 1, 0]

    repeated_feature_report = build_index_price_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC",
    )
    assert repeated_feature_report.rows_in == 0
    assert repeated_feature_report.rows_out == 3
