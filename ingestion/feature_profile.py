"""Feature profile metadata and plot helpers shared by lake writers."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MAX_PLOT_POINTS = 3_000


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
        return "perp_1m"
    if column_name.startswith("oi_"):
        return "oi_1m_feature"
    if column_name.startswith("funding_"):
        return "funding_1m_feature"
    if column_name.startswith("trades_"):
        return "perp_trades_1m_feature"
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


def write_feature_distribution_plot(
    frame: Any,
    output_path: Path,
    *,
    normalize_y: bool = True,
) -> str | None:
    """Write a numeric feature profile plot, returning the resolved path when generated."""

    try:
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        return None

    pl = _require_polars()
    full_frame = frame
    frame = _sample_frame_for_plot(full_frame)
    numeric_cols = _ordered_numeric_columns(full_frame)
    if not numeric_cols:
        return None

    row_count = len(numeric_cols)
    full_missing_by_col: dict[str, int] = {
        col: int(full_frame.select(pl.col(col).is_null().sum()).item()) for col in numeric_cols
    }
    full_available_by_col: dict[str, int] = {
        col: int(full_frame.height - full_missing_by_col[col]) for col in numeric_cols
    }
    full_time_range_by_col: dict[str, str] = {}
    full_numeric_stats_by_col: dict[str, dict[str, float | None]] = {}
    today_utc = _today_utc_date()
    for col in numeric_cols:
        non_null_frame = full_frame.filter(pl.col(col).is_not_null())
        if non_null_frame.height == 0:
            full_time_range_by_col[col] = "n/a"
            full_numeric_stats_by_col[col] = {"mean": None, "std": None, "var": None}
            continue
        min_ts = non_null_frame.select(pl.col("timestamp_m1").min()).item()
        max_ts = non_null_frame.select(pl.col("timestamp_m1").max()).item()
        if isinstance(min_ts, datetime) and isinstance(max_ts, datetime):
            min_iso = _iso_utc(min_ts)
            full_time_range_by_col[col] = f"{min_iso} -> {today_utc}"
        else:
            full_time_range_by_col[col] = "n/a"
        stats_row = non_null_frame.select(
            [
                pl.col(col).mean().alias("mean"),
                pl.col(col).std().alias("std"),
                pl.col(col).var().alias("var"),
            ]
        ).to_dicts()[0]
        full_numeric_stats_by_col[col] = {
            "mean": float(stats_row["mean"]) if stats_row["mean"] is not None else None,
            "std": float(stats_row["std"]) if stats_row["std"] is not None else None,
            "var": float(stats_row["var"]) if stats_row["var"] is not None else None,
        }
    fig = plt.figure(
        figsize=(12, max(0.85 * row_count + 4.0, 12.0) * 1.2), facecolor="#070b16", constrained_layout=True
    )
    grid = fig.add_gridspec(
        row_count + 1,
        2,
        height_ratios=[1.15, *([1.0] * row_count)],
        width_ratios=[8, 2],
        wspace=0.08,
        hspace=0.30,
    )
    profile_ax = fig.add_subplot(grid[0, :])
    profile_ax.set_axis_off()
    profile_ax.set_facecolor("#070b16")
    ts_min = frame.select(pl.col("timestamp_m1").min()).item() if "timestamp_m1" in frame.columns else None
    ts_max = frame.select(pl.col("timestamp_m1").max()).item() if "timestamp_m1" in frame.columns else None
    exchange_val = (
        str(frame.get_column("exchange")[0]) if "exchange" in frame.columns and frame.height > 0 else "unknown"
    )
    symbol_val = str(frame.get_column("symbol")[0]) if "symbol" in frame.columns and frame.height > 0 else "unknown"
    time_window = (
        f"{_iso_utc(ts_min if isinstance(ts_min, datetime) else None)} -> "
        f"{_iso_utc(ts_max if isinstance(ts_max, datetime) else None)}"
    )
    profile_ax.text(
        0.5,
        0.94,
        f"Gold 1m profile | {exchange_val} {symbol_val}",
        color="#e5e7eb",
        fontsize=9,
        ha="center",
        va="top",
    )
    profile_ax.text(
        0.0,
        0.56,
        "\n".join(
            [
                f"numeric features: {row_count}",
                f"window: {time_window}",
                f"output: {output_path.name}",
            ]
        ),
        color="#b8c2d6",
        fontsize=5.6,
        family="monospace",
        ha="left",
        va="top",
    )
    fig.text(0.06, 0.93, "Gold M1 numeric feature lines", color="#e2e8f0", fontsize=8.5, ha="left", va="bottom")
    fig.text(0.90, 0.93, "Distribution", color="#d1d5db", fontsize=8, ha="center", va="bottom")

    for idx, col in enumerate(numeric_cols):
        series_df = frame.select(["timestamp_m1", col]).sort("timestamp_m1")
        values_all = series_df.get_column(col).to_list()
        ts = series_df.get_column("timestamp_m1").to_list()
        arr_non_null, arr_plot_normalized, _missing_values = _normalized_series(values_all)
        left_ax = fig.add_subplot(grid[idx + 1, 0])
        right_ax = fig.add_subplot(grid[idx + 1, 1])

        for axis in (left_ax, right_ax):
            axis.set_facecolor("#0d1424")
            axis.tick_params(colors="#cbd5e1", labelsize=8)
            axis.spines["top"].set_color("#22324c")
            axis.spines["right"].set_color("#22324c")
            axis.spines["left"].set_color("#22324c")
            axis.spines["bottom"].set_color("#22324c")

        if arr_non_null:
            arr = arr_non_null
            arr_plot = (
                arr_plot_normalized
                if normalize_y
                else [float(v) if v is not None else float("nan") for v in values_all]
            )
            sparse_ratio = (len(arr) / frame.height) if frame.height > 0 else 0.0
            is_sparse = sparse_ratio < 0.15 or len(arr) < 200
            line_width = 1.1 if is_sparse else 0.8
            line_alpha = 0.95 if is_sparse else 0.86
            left_ax.plot(ts, arr_plot, color="#8cd7f3", linewidth=line_width, alpha=line_alpha)
            if is_sparse:
                marker_every = max(1, len(arr_plot) // 80)
                left_ax.plot(
                    ts,
                    arr_plot,
                    linestyle="None",
                    marker="o",
                    markersize=2.2,
                    color="#b5edff",
                    alpha=0.88,
                    markevery=marker_every,
                )
            mask = [value == value for value in arr_plot]
            if normalize_y:
                left_ax.fill_between(ts, arr_plot, [0.0] * len(arr_plot), where=mask, color="#234b6e", alpha=0.16)
            else:
                y_min = min(arr)
                y_max = max(arr)
                if y_max > y_min:
                    baseline = y_min
                    left_ax.fill_between(
                        ts, arr_plot, [baseline] * len(arr_plot), where=mask, color="#234b6e", alpha=0.10
                    )
            major_locator, minor_locator, major_formatter = _time_axis_style(mdates, mticker, ts)
            left_ax.xaxis.set_major_locator(major_locator)
            if minor_locator is not None:
                left_ax.xaxis.set_minor_locator(minor_locator)
            left_ax.xaxis.set_major_formatter(major_formatter)
            left_ax.tick_params(axis="x", rotation=35, labelsize=6.5)
            left_ax.tick_params(axis="x", which="minor", length=2, color="#475569")
            left_ax.grid(axis="x", which="major", color="#2f3b52", alpha=0.28, linewidth=0.6)
            left_ax.grid(axis="x", which="minor", color="#253047", alpha=0.16, linewidth=0.4)
            if normalize_y:
                left_ax.set_ylim(-0.05, 1.05)
            else:
                y_low = min(arr)
                y_high = max(arr)
                if y_high > y_low:
                    pad = (y_high - y_low) * 0.05
                    left_ax.set_ylim(y_low - pad, y_high + pad)
            missing_ts = [t for t, v in zip(ts, values_all, strict=False) if v is None]
            if missing_ts:
                if normalize_y:
                    ymin, ymax = -0.05, 1.05
                else:
                    y_low = min(arr)
                    y_high = max(arr)
                    pad = (y_high - y_low) * 0.05 if y_high > y_low else 1.0
                    ymin, ymax = y_low - pad, y_high + pad
                left_ax.vlines(missing_ts, ymin=ymin, ymax=ymax, color="#5b233b", alpha=0.22, linewidth=0.7)
            left_ax.set_ylabel(col, color="#cbd5e1", fontsize=6.8)
            right_ax.hist(arr, bins=24, color="#a8be8f", alpha=0.92, edgecolor="#a8be8f")
            right_ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.4g}"))
            if full_available_by_col[col] > 0:
                missing_ratio = 100.0 * full_missing_by_col[col] / full_available_by_col[col]
            else:
                missing_ratio = 0.0
            std_value = full_numeric_stats_by_col[col]["std"]
            std_scalar = float(std_value) if isinstance(std_value, (int, float)) else None
            stats_box = "\n".join(
                [
                    f"feature: {col}",
                    f"time range: {full_time_range_by_col[col]}",
                    f"all rows: {full_available_by_col[col]}",
                    f"missing rows: {missing_ratio:.2f}%",
                    (
                        f"mean: {full_numeric_stats_by_col[col]['mean']:.6g}"
                        if full_numeric_stats_by_col[col]["mean"] is not None
                        else "mean: n/a"
                    ),
                    (
                        f"var: {full_numeric_stats_by_col[col]['var']:.6g}"
                        if full_numeric_stats_by_col[col]["var"] is not None
                        else "var: n/a"
                    ),
                    (f"1std: {abs(std_scalar):.6g}" if std_scalar is not None else "1std: n/a"),
                    (f"2std: {abs(2.0 * std_scalar):.6g}" if std_scalar is not None else "2std: n/a"),
                    (f"3std: {abs(3.0 * std_scalar):.6g}" if std_scalar is not None else "3std: n/a"),
                ]
            )
            left_ax.text(
                0.008,
                0.96,
                stats_box,
                transform=left_ax.transAxes,
                va="top",
                ha="left",
                fontsize=3.6,
                family="monospace",
                color="#d7e3f2",
                bbox={"facecolor": "#0a1322", "edgecolor": "#334155", "alpha": 0.78, "pad": 2.0},
            )
        else:
            left_ax.text(
                0.02,
                0.5,
                f"feature: {col}\nno data",
                va="center",
                ha="left",
                color="#e2e8f0",
                fontsize=9.5,
                transform=left_ax.transAxes,
            )
        if idx < row_count - 1:
            left_ax.set_xticklabels([])
            right_ax.set_xticklabels([])
        right_ax.set_xlabel("value", color="#cbd5e1", fontsize=7)
        right_ax.set_yticks([])
        right_ax.tick_params(axis="x", colors="#cbd5e1", labelsize=7)
        right_ax.grid(alpha=0.18, linestyle="-", linewidth=0.6, color="#334155")

    fig.suptitle("Gold 1m Feature Profile", color="#f1f5f9", fontsize=10, fontweight="semibold")
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return str(output_path.resolve())


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for feature profile generation. Install project dependencies.") from exc
    return pl


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today_utc_date() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _sample_frame_for_plot(frame: Any) -> Any:
    if "timestamp_m1" not in frame.columns or frame.height <= MAX_PLOT_POINTS:
        return frame
    step = (frame.height - 1) / float(MAX_PLOT_POINTS - 1)
    indices = [int(round(i * step)) for i in range(MAX_PLOT_POINTS)]
    indices[0] = 0
    indices[-1] = frame.height - 1
    seen: set[int] = set()
    deduped_indices: list[int] = []
    for idx in indices:
        if idx not in seen:
            seen.add(idx)
            deduped_indices.append(idx)
    return frame[deduped_indices]


def _ordered_numeric_columns(frame: Any) -> list[str]:
    numeric_cols = [col for col, dtype in zip(frame.columns, frame.dtypes, strict=False) if dtype.is_numeric()]
    market_cols = [col for col in numeric_cols if col.startswith(("spot_", "perp_"))]
    derived_cols = [col for col in numeric_cols if col.startswith(("oi_", "funding_"))]
    l2_cols = [col for col in numeric_cols if col.startswith("l2_")]
    other_cols = [col for col in numeric_cols if col not in set(market_cols + derived_cols + l2_cols)]
    return [*market_cols, *derived_cols, *l2_cols, *other_cols]


def _normalized_series(values_all: list[object]) -> tuple[list[float], list[float], int]:
    arr = [float(v) for v in values_all if isinstance(v, (int, float))]
    missing_values = len(values_all) - len(arr)
    if not arr:
        return [], [], missing_values
    arr_min = min(arr)
    arr_max = max(arr)
    if arr_max == arr_min:
        arr_plot = [0.0 if v is not None else float("nan") for v in values_all]
    else:
        scale = arr_max - arr_min
        arr_plot = [((float(v) - arr_min) / scale) if isinstance(v, (int, float)) else float("nan") for v in values_all]
    return arr, arr_plot, missing_values


def _time_axis_style(mdates: Any, mticker: Any, ts: list[object]) -> tuple[Any, Any, Any]:
    if not ts or not isinstance(ts[0], datetime) or not isinstance(ts[-1], datetime):
        return mticker.MaxNLocator(nbins=6), None, mticker.StrMethodFormatter("{x:,.0f}")
    if len(ts) < 3 or ts[0] == ts[-1]:
        major_locator = mdates.MinuteLocator(interval=1)
        return major_locator, None, mdates.DateFormatter("%m-%d %H:%M")
    span_seconds = max((ts[-1] - ts[0]).total_seconds(), 1.0)
    span_minutes = span_seconds / 60.0
    span_hours = span_seconds / 3600.0
    span_days = span_seconds / 86400.0
    if span_seconds <= 6 * 3600:
        major_locator = mdates.MinuteLocator(interval=max(30, int(span_minutes // 8) + 1))
        minor_locator = mdates.MinuteLocator(interval=max(10, int(span_minutes // 700) + 1))
        formatter = mdates.DateFormatter("%m-%d %H:%M")
    elif span_seconds <= 2 * 24 * 3600:
        major_locator = mdates.HourLocator(interval=max(2, int(span_hours // 8) + 1))
        minor_locator = mdates.HourLocator(interval=max(1, int(span_hours // 700) + 1))
        formatter = mdates.DateFormatter("%m-%d %H:%M")
    elif span_seconds <= 14 * 24 * 3600:
        major_locator = mdates.DayLocator(interval=max(1, int(span_days // 8) + 1))
        minor_locator = mdates.HourLocator(interval=max(6, int(span_hours // 700) + 1))
        formatter = mdates.DateFormatter("%m-%d")
    else:
        major_locator = mdates.DayLocator(interval=max(2, int(span_days // 8) + 1))
        minor_locator = mdates.DayLocator(interval=max(1, int(span_days // 700) + 1))
        formatter = mdates.DateFormatter("%Y-%m-%d")
    return major_locator, minor_locator, formatter
