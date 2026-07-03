"""Feature metadata helpers independent of plotting side effects."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any


def feature_hash(columns: list[str]) -> str:
    """Return a stable short hash for an ordered feature column set."""

    payload = "|".join(columns)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def feature_source_dataset(column_name: str) -> str:
    """Infer the source dataset label from a derived feature column name."""

    if column_name.startswith("option_trades_"):
        return "option_trades_1m_feature"
    if column_name.startswith("spot_"):
        return "spot_1m"
    if column_name.startswith("perp_"):
        return "peprs_ohlcv_1m"
    if column_name.startswith("oi_"):
        return "oi_1m_feature"
    if column_name.startswith("funding_"):
        return "funding_1m_feature"
    if column_name.startswith("trades_"):
        return "perps_trades_1m_feature"
    if column_name.startswith(("volatility_index_data_", "volatility_index_")):
        return "volatility_index_data_observed"
    return "gold_merged"


def feature_metadata(pl: Any, frame: Any, exchange: str) -> dict[str, dict[str, object]]:
    """Build per-column metadata used by Bronze, Silver, and Gold manifests."""

    meta: dict[str, dict[str, object]] = {}
    for col, dtype in zip(frame.columns, frame.dtypes, strict=False):
        null_count = int(frame.select(pl.col(col).is_null().sum()).item())
        time_filtered = frame.filter(pl.col(col).is_not_null()) if col != "timestamp_m1" else frame
        feature_min_ts = (
            time_filtered.select(pl.col("timestamp_m1").min()).item() if "timestamp_m1" in frame.columns else None
        )
        feature_max_ts = (
            time_filtered.select(pl.col("timestamp_m1").max()).item() if "timestamp_m1" in frame.columns else None
        )
        row: dict[str, object] = {
            "dtype": str(dtype),
            "null_count": null_count,
            "missing_values": null_count,
            "non_null_count": int(frame.height - null_count),
            "source_dataset": feature_source_dataset(col),
            "source_exchange": exchange,
            "time_range": {
                "min_timestamp": _iso_utc(feature_min_ts if isinstance(feature_min_ts, datetime) else None),
                "max_timestamp": _iso_utc(feature_max_ts if isinstance(feature_max_ts, datetime) else None),
            },
        }
        if dtype.is_numeric():
            stats = frame.select(
                [
                    pl.col(col).drop_nulls().count().alias("count"),
                    pl.col(col).drop_nulls().mean().alias("mean"),
                    pl.col(col).drop_nulls().std().alias("std"),
                    pl.col(col).drop_nulls().min().alias("min"),
                    pl.col(col).drop_nulls().max().alias("max"),
                ]
            ).to_dicts()[0]
            row.update(stats)
        meta[col] = row
    return meta


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
