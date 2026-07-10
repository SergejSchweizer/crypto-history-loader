"""Tests for the external historical-volatility Silver reference."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import (
    SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS,
    SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS,
)
from application.services.silver_service import build_historical_volatility_observed_for_symbol

pl = pytest.importorskip("polars")


def test_historical_volatility_preserves_source_and_rejects_invalid_values(tmp_path: Path) -> None:
    """External volatility should remain distinct from internally computed RV features."""

    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    t0 = datetime(2026, 5, 8, 18, tzinfo=UTC)
    rows = [
        {
            "exchange": "deribit",
            "symbol": "BTC",
            "instrument_type": "perp",
            "event_time": t0,
            "open_time": t0,
            "ingested_at": t0 + timedelta(days=1),
            "source_endpoint": "public_get_historical_volatility",
            "value": 30.0,
        },
        {
            "exchange": "deribit",
            "symbol": "BTC",
            "instrument_type": "perp",
            "event_time": t0,
            "open_time": t0,
            "ingested_at": t0 + timedelta(days=2),
            "source_endpoint": "public_get_historical_volatility",
            "value": 31.0,
        },
        {
            "exchange": "deribit",
            "symbol": "BTC",
            "instrument_type": "perp",
            "event_time": t0 + timedelta(hours=1),
            "open_time": t0 + timedelta(hours=1),
            "ingested_at": t0 + timedelta(days=1),
            "source_endpoint": "public_get_historical_volatility",
            "value": -1.0,
        },
        {
            "exchange": "deribit",
            "symbol": "BTC",
            "instrument_type": "perp",
            "event_time": t0 + timedelta(hours=2),
            "open_time": t0 + timedelta(hours=2),
            "ingested_at": t0 + timedelta(days=1),
            "source_endpoint": "public_get_historical_volatility",
            "value": float("nan"),
        },
    ]
    target = (
        bronze
        / "dataset_type=historical_volatility"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "date=2026-05-08"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)

    report = build_historical_volatility_observed_for_symbol(
        bronze_root=str(bronze), silver_root=str(silver), exchange="deribit", symbol="btc"
    )

    assert report.dataset == "historical_volatility_observed"
    assert report.rows_in == 4
    assert report.rows_out == 1
    assert report.duplicates_removed == 1
    assert report.invalid_ohlc_rows == 2
    output = pl.read_parquet(
        silver
        / "dataset_type=historical_volatility_observed"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    assert output.columns == SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS
    assert output["historical_volatility"].to_list() == [31.0]
    assert output["historical_volatility_source_timestamp"].to_list() == [t0]
    assert output["source_endpoint"].to_list() == ["public_get_historical_volatility"]
    assert "historical_volatility" not in SILVER_REALIZED_VOLATILITY_FEATURE_COLUMNS
    assert "rv_1h" not in SILVER_HISTORICAL_VOLATILITY_OBSERVED_COLUMNS
