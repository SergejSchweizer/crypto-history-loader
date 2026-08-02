"""Tests for Silver sidecar writers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from application.services.silver_sidecars import _iso_utc, _with_timestamp_m1, write_monthly_sidecars


@dataclass(frozen=True)
class _Report:
    dataset: str
    exchange: str
    symbol: str
    timeframe: str
    months_processed: list[str]
    calculation_lookback_days: int | None = None


def test_write_monthly_sidecars_writes_manifest_from_parquet(tmp_path: Path) -> None:
    """Silver sidecars should be generated without invoking transformation builders."""

    silver_root = tmp_path / "silver"
    parquet_path = (
        silver_root
        / "dataset_type=perps_ohlcv"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    parquet_path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "open_time": datetime(2026, 5, 1, tzinfo=UTC),
                "exchange": "deribit",
                "symbol": "BTC",
                "close_price": 100.0,
            }
        ]
    ).write_parquet(parquet_path)

    manifest_paths, plot_paths = write_monthly_sidecars(
        silver_root=str(silver_root),
        market="perps_ohlcv",
        exchange="deribit",
        symbol="BTC",
        report=_Report(
            dataset="perps_ohlcv_1m",
            exchange="deribit",
            symbol="BTC",
            timeframe="1m",
            months_processed=["2026-05"],
        ),
        write_manifest=True,
        plot=False,
    )

    assert plot_paths == []
    assert len(manifest_paths) == 1
    payload = json.loads(Path(manifest_paths[0]).read_text(encoding="utf-8"))
    assert payload["dataset"] == "perps_ohlcv_1m"
    assert payload["source_silver_datasets"]["perps_ohlcv_1m"]["source_symbols"] == ["BTC"]
    assert payload["feature_metadata"]["close_price"]["source_exchange"] == "deribit"
    assert payload["quantitative_feature_semantics"] == {}


def test_write_monthly_sidecars_skips_missing_files_and_optional_outputs(tmp_path: Path) -> None:
    """Missing months and disabled sidecars should be harmless and idempotent."""

    report = _Report(
        dataset="spot_ohlcv_1m",
        exchange="deribit",
        symbol="BTC",
        timeframe="1m",
        months_processed=["2026-05", "2026-06"],
    )
    assert write_monthly_sidecars(
        silver_root=str(tmp_path),
        market="spot_ohlcv",
        exchange="deribit",
        symbol="BTC",
        report=report,
        write_manifest=False,
        plot=False,
    ) == ([], [])
    assert _iso_utc(None) is None
    frame = pl.DataFrame({"timestamp": [datetime(2026, 5, 1, tzinfo=UTC)], "value": [1.0]})
    assert "timestamp_m1" in _with_timestamp_m1(frame).columns


def test_write_monthly_sidecars_handles_empty_frame_without_timestamp(tmp_path: Path) -> None:
    """An empty parquet without a time column still receives a valid manifest."""

    path = (
        tmp_path
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-2026-05.parquet"
    )
    path.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": pl.Series([], dtype=pl.String), "value": pl.Series([], dtype=pl.Float64)}).write_parquet(
        path
    )
    manifests, plots = write_monthly_sidecars(
        silver_root=str(tmp_path),
        market="spot_ohlcv",
        exchange="deribit",
        symbol="BTC",
        report=_Report("spot_ohlcv_1m", "deribit", "BTC", "1m", ["2026-05"]),
    )
    assert len(manifests) == 1
    assert plots == []
    payload = json.loads(Path(manifests[0]).read_text(encoding="utf-8"))
    assert payload["min_timestamp"] is None
