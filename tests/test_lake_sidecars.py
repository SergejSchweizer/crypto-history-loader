"""Tests for Bronze parquet sidecar helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

import ingestion.lake_sidecars as sidecars
from ingestion.lake_sidecars import DEFAULT_BRONZE_SIDECAR_DATASET_TYPES, ensure_bronze_sidecars, write_bronze_sidecars


def test_default_bronze_sidecar_dataset_types_are_stable() -> None:
    """Guard the default repair scan surface for Bronze sidecars."""

    assert DEFAULT_BRONZE_SIDECAR_DATASET_TYPES == (
        "spot_ohlcv",
        "perps_ohlcv",
        "open_interest",
        "funding",
        "perps_trades",
        "options_trades",
    )


def test_ensure_bronze_sidecars_returns_empty_for_missing_root(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Missing lake roots are a no-op for sidecar repair."""

    assert ensure_bronze_sidecars(lake_root=str(tmp_path / "missing")) == []


def test_write_bronze_sidecars_records_manifest_and_requires_plot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A Bronze sidecar has deterministic metadata and fails when its required plot is unavailable."""

    parquet = tmp_path / "data.parquet"
    monkeypatch.setattr(
        sidecars, "write_feature_distribution_plot", lambda *_args, **_kwargs: str(tmp_path / "data.png")
    )
    write_bronze_sidecars(
        file_path=parquet,
        dataset_type="spot_ohlcv",
        key=("deribit", "spot_ohlcv", "BTC", "1m", "2026-08-02"),
        rows=[{"open_time": datetime(2026, 8, 2, tzinfo=UTC), "close_price": 1.0}],
    )
    payload = json.loads(parquet.with_suffix(".json").read_text(encoding="utf-8"))
    assert payload["rows_out"] == 1
    assert payload["min_timestamp"] == "2026-08-02T00:00:00Z"

    monkeypatch.setattr(sidecars, "write_feature_distribution_plot", lambda *_args, **_kwargs: None)
    with pytest.raises(RuntimeError, match="requires plot generation"):
        write_bronze_sidecars(
            file_path=parquet,
            dataset_type="spot_ohlcv",
            key=("deribit", "spot_ohlcv", "BTC", "1m", "2026-08-02"),
            rows=[],
        )


def test_ensure_bronze_sidecars_skips_complete_invalid_and_repairs_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Repair scans only write sidecars for valid partitions missing either required artifact."""

    valid = (
        tmp_path
        / "dataset_type=spot_ohlcv"
        / "exchange=deribit"
        / "instrument_type=spot_ohlcv"
        / "symbol=BTC"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-08"
        / "date=2026-08-02"
        / "data.parquet"
    )
    valid.parent.mkdir(parents=True)
    valid.write_bytes(b"parquet")
    complete = valid.with_name("complete.parquet")
    complete.write_bytes(b"parquet")
    complete.with_suffix(".json").write_text("{}", encoding="utf-8")
    complete.with_suffix(".png").write_bytes(b"png")
    logs: list[str] = []
    repaired: list[Path] = []

    class _Table:
        def to_pylist(self) -> list[dict[str, object]]:
            return []

    class _ParquetFile:
        def __init__(self, _path: Path) -> None:
            pass

        def read(self) -> _Table:
            return _Table()

    monkeypatch.setattr(sidecars, "_require_pyarrow_parquet", lambda: type("PQ", (), {"ParquetFile": _ParquetFile}))
    monkeypatch.setattr(sidecars, "dataset_data_files", lambda _root, _dataset: [valid, complete])
    monkeypatch.setattr(sidecars, "write_bronze_sidecars", lambda **kwargs: repaired.append(kwargs["file_path"]))
    assert ensure_bronze_sidecars(
        lake_root=str(tmp_path), dataset_types=["spot_ohlcv"], log_fn=lambda message, *_args: logs.append(message)
    ) == [str(valid.resolve())]
    assert repaired == [valid]
    assert any("backfill complete" in message for message in logs)


def test_ensure_bronze_sidecars_skips_mismatched_partition_dataset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A path parsed as another dataset must not be repaired under the selected label."""

    parquet = tmp_path / "data.parquet"
    parquet.write_bytes(b"parquet")
    monkeypatch.setattr(sidecars, "dataset_data_files", lambda *_args: [parquet])
    monkeypatch.setattr(
        sidecars,
        "partition_key_from_parquet_path",
        lambda _path: ("funding", ("deribit", "perp", "BTC", "8h", "2026-08-02")),
    )

    assert ensure_bronze_sidecars(lake_root=str(tmp_path), dataset_types=["spot_ohlcv"]) == []
