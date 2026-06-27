"""Tests for Silver sidecar writers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from application.services.silver_sidecars import write_monthly_sidecars


@dataclass(frozen=True)
class _Report:
    dataset: str
    exchange: str
    symbol: str
    timeframe: str
    months_processed: list[str]


def test_write_monthly_sidecars_writes_manifest_from_parquet(tmp_path: Path) -> None:
    """Silver sidecars should be generated without invoking transformation builders."""

    silver_root = tmp_path / "silver"
    parquet_path = (
        silver_root
        / "dataset_type=perp"
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
        market="perp",
        exchange="deribit",
        symbol="BTC",
        report=_Report(
            dataset="perp_1m",
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
    assert payload["dataset"] == "perp_1m"
    assert payload["source_silver_datasets"]["perp_1m"]["source_symbols"] == ["BTC"]
    assert payload["feature_metadata"]["close_price"]["source_exchange"] == "deribit"
