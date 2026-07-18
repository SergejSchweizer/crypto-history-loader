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

    volatility_index_feature_columns = {
        "iv_open",
        "iv_high",
        "iv_low",
        "iv_close",
        # QC-01: annualized 30d-horizon alias of `iv_close`; must resolve to the
        # same source dataset, not the generic `iv_` -> iv_rv_1m_feature fallback.
        "iv_30d_annualized_pct",
        "iv_range",
        "iv_return_1m",
        "iv_change_5m",
        "iv_change_15m",
        "iv_change_1h",
        "iv_zscore_1d",
        "iv_zscore_7d",
        "iv_percentile_30d",
        "iv_source_dataset",
        "iv_source_timestamp",
        "iv_data_available",
    }
    if column_name.startswith("options_trades_"):
        return "options_trades_1m_feature"
    if column_name.startswith("spot_ohlcv_"):
        return "spot_ohlcv_1m"
    if column_name.startswith("perp_"):
        return "perps_ohlcv_1m"
    if column_name.startswith("open_interest_"):
        return "open_interest_1m_feature"
    if column_name.startswith("funding_"):
        return "funding_1m_feature"
    if column_name.startswith("trades_"):
        return "perps_trades_1m_feature"
    if column_name.startswith(("volatility_index_data_", "volatility_index_")):
        return "volatility_index_data_observed"
    if column_name in volatility_index_feature_columns:
        return "volatility_index_1m_feature"
    if column_name in {"as_of", "live_snapshot_derived"}:
        return "gold_live_lineage"
    if column_name in {"iv_available", "rv_available"}:
        return "iv_rv_1m_feature"
    if column_name in {
        "canonical_rv_source",
        "canonical_rv_source_available",
        "spot_available",
        "perps_available",
        "spot_perps_basis_available",
    }:
        return "realized_volatility_1m_feature"
    if column_name.startswith(("rv_", "spot_rv_", "perps_rv_", "spot_log_return", "perps_log_return")):
        return "realized_volatility_1m_feature"
    if column_name.startswith(("parkinson_rv_", "jump_proxy")):
        return "realized_volatility_1m_feature"
    if column_name.startswith(("iv_", "minutes_since_iv_", "minutes_since_rv_")):
        return "iv_rv_1m_feature"
    if column_name.startswith("perps_l2_"):
        return "perps_l2_1m_feature"
    if column_name.startswith("options_l2_"):
        return "options_l2_1m_feature"
    if column_name.startswith("options_surface_"):
        return "options_surface_1m_feature"
    if column_name.startswith(("index_price", "minutes_since_index_price_")):
        return "index_price_1m_feature"
    if column_name.startswith(("futures_summary_", "minutes_since_summary_observation")):
        return "futures_summary_1m_feature"
    if column_name.startswith("strategy_"):
        return "gold_strategy_features"
    if column_name.startswith(("target_", "label_")):
        return "gold_prediction_targets"
    if column_name.startswith("historical_volatility_"):
        return "historical_volatility_observed"
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
