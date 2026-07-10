"""Tests for L2 observed and minute-feature Silver builders."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_L2_FEATURE_COLUMNS, SILVER_L2_OBSERVED_COLUMNS
from application.services.silver_service import (
    build_perps_l2_1m_feature_for_symbol,
    build_perps_l2_observed_for_symbol,
    discover_l2_symbols,
)

pl = pytest.importorskip("polars")


def _level(price: float, amount: float) -> dict[str, float]:
    return {"price": price, "amount": amount}


def _row(*, timestamp: datetime, bids: list[dict[str, float]], asks: list[dict[str, float]]) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "dataset_type": "perps_l2_snapshot_1m",
        "exchange": "deribit",
        "symbol": "BTC-PERPETUAL",
        "instrument_type": "perp",
        "event_time": timestamp,
        "ingested_at": timestamp,
        "run_id": "test",
        "source": "rest_order_book",
        "depth": 50,
        "bids": bids,
        "asks": asks,
    }


def _write_bronze(root: Path, rows: list[dict[str, object]]) -> None:
    target = (
        root
        / "dataset_type=perps_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC-PERPETUAL"
        / "depth=50"
        / "source=rest_order_book"
        / "year=2026"
        / "month=05"
        / "date=2026-05-05"
        / "hour=12"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)


def test_perps_l2_filters_malformed_books_and_uses_latest_snapshot_per_minute(
    tmp_path: Path,
) -> None:
    """Malformed books should be rejected while empty observed books remain explicit."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 5, 12, 0, 10, tzinfo=UTC)
    rows = [
        _row(
            timestamp=t0,
            bids=[_level(99.9, 1.0), _level(99.8, 2.0)],
            asks=[_level(100.2, 1.0), _level(100.3, 2.0)],
        ),
        _row(
            timestamp=t0.replace(second=40),
            bids=[_level(100.0, 6.0), _level(99.95, 4.0)],
            asks=[_level(100.1, 3.0), _level(100.15, 4.0)],
        ),
        _row(
            timestamp=t0.replace(second=20),
            bids=[_level(99.0, 1.0), _level(100.0, 1.0)],
            asks=[_level(101.0, 1.0)],
        ),
        _row(
            timestamp=t0.replace(second=21),
            bids=[_level(100.0, -1.0)],
            asks=[_level(101.0, 1.0)],
        ),
        _row(
            timestamp=t0.replace(second=22),
            bids=[_level(101.0, 1.0)],
            asks=[_level(100.0, 1.0)],
        ),
        _row(timestamp=t0.replace(minute=1), bids=[], asks=[]),
    ]
    _write_bronze(bronze, rows)

    assert discover_l2_symbols(bronze_root=str(bronze), exchange="deribit") == ["BTC-PERPETUAL"]
    observed_report = build_perps_l2_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="btc-perpetual",
    )
    feature_report = build_perps_l2_1m_feature_for_symbol(
        silver_root=str(silver), exchange="deribit", symbol="BTC-PERPETUAL"
    )

    assert observed_report.rows_in == 6
    assert observed_report.rows_out == 3
    assert observed_report.invalid_ohlc_rows == 3
    assert feature_report.rows_in == 3
    assert feature_report.rows_out == 2
    assert feature_report.duplicates_removed == 1
    observed_path = (
        silver
        / "dataset_type=perps_l2_snapshot_1m_observed"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-PERPETUAL-2026-05.parquet"
    )
    feature_path = Path(str(observed_path).replace("perps_l2_snapshot_1m_observed", "perps_l2_1m_feature"))
    observed = pl.read_parquet(observed_path)
    feature = pl.read_parquet(feature_path).sort("timestamp_m1")
    assert observed.columns == SILVER_L2_OBSERVED_COLUMNS
    assert feature.columns == SILVER_L2_FEATURE_COLUMNS
    first = feature.row(0, named=True)
    second = feature.row(1, named=True)
    assert first["best_bid_price"] == pytest.approx(100.0)
    assert first["best_ask_price"] == pytest.approx(100.1)
    assert first["mid_price"] == pytest.approx(100.05)
    assert first["spread"] == pytest.approx(0.1)
    assert first["top_of_book_imbalance"] == pytest.approx(1.0 / 3.0)
    assert first["bid_depth_10bps"] == pytest.approx(10.0)
    assert first["ask_depth_10bps"] == pytest.approx(7.0)
    assert first["bid_depth_50bps"] == pytest.approx(10.0)
    assert first["ask_depth_50bps"] == pytest.approx(7.0)
    assert first["quote_available"] is True
    assert second["quote_available"] is False
    assert second["mid_price"] is None
    assert second["bid_depth_10bps"] is None
