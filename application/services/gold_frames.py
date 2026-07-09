"""Gold dataset frame discovery, preparation, and merge-grid helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


def require_polars() -> Any:
    """Import Polars for Gold frame operations.

    Raises:
        RuntimeError: If Polars is not installed in the active environment.
    """

    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for gold-build. Install project dependencies.") from exc
    return pl


def normalize_symbol(value: str) -> str:
    """Normalize exchange symbols to the canonical base asset used by Gold datasets."""

    raw = value.strip().upper()
    normalized = raw.replace("_", "-").replace("/", "-")
    parts = [part for part in normalized.split("-") if part]
    if parts:
        return parts[0]
    for candidate in ("BTC", "ETH", "SOL"):
        if raw.startswith(candidate):
            return candidate
    return raw


def discover_symbols_for_dataset(
    *,
    silver_root: str,
    exchange: str,
    dataset_type: str,
    timeframe: str,
) -> set[str]:
    """Discover normalized symbols available for one Silver dataset and timeframe."""

    root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    symbols: set[str] = set()
    if not root.exists():
        return symbols
    for path in root.glob("symbol=*/timeframe=*"):
        if path.name != f"timeframe={timeframe}":
            continue
        parent = path.parent.name
        if not parent.startswith("symbol="):
            continue
        symbols.add(normalize_symbol(parent.split("=", 1)[1]))
    return symbols


def read_latest_l2_gold_frame(*, l2_root: str, exchange: str, symbol: str) -> tuple[Any, Path]:
    """Read the newest compatible L2 Gold parquet frame for a symbol.

    Raises:
        ValueError: If no matching L2 parquet artifact exists.
    """

    pl = require_polars()
    root = Path(l2_root)
    nested = root / "dataset_id=gold.l2.micro.m1"
    if nested.exists():
        root = nested
    candidates: list[Path] = []
    for path in root.glob("exchange=*/symbol=*/version=*/build_id=*/data.parquet"):
        exchange_segment = next((part for part in path.parts if part.startswith("exchange=")), None)
        if exchange_segment is None:
            continue
        raw_exchange = exchange_segment.split("=", 1)[1]
        if raw_exchange != exchange:
            continue
        symbol_segment = next((part for part in path.parts if part.startswith("symbol=")), None)
        if symbol_segment is None:
            continue
        raw_symbol = symbol_segment.split("=", 1)[1]
        if normalize_symbol(raw_symbol) != normalize_symbol(symbol):
            continue
        candidates.append(path)
    if not candidates:
        for path in root.glob("**/*_L2_*.parquet"):
            base = path.name.split("_L2_", 1)[0]
            if normalize_symbol(base) != normalize_symbol(symbol):
                continue
            candidates.append(path)
    candidates = sorted(candidates, key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise ValueError(f"Missing L2 parquet for symbol={symbol} under l2_root={l2_root}")
    chosen = candidates[-1]
    return pl.read_parquet(str(chosen)), chosen


def prepare_l2(pl: Any, frame: Any, symbol: str) -> Any:
    """Normalize L2 Gold columns to the Gold join contract."""

    key_cols = {"ts_minute", "exchange", "symbol"}
    if "ts_minute" not in frame.columns:
        raise ValueError("L2 parquet missing required column 'ts_minute'")
    if "exchange" not in frame.columns:
        frame = frame.with_columns(pl.lit("deribit").alias("exchange"))
    if "symbol" not in frame.columns:
        frame = frame.with_columns(pl.lit(symbol).alias("symbol"))
    renamed = []
    for col in frame.columns:
        if col in key_cols:
            continue
        renamed.append(pl.col(col).alias(f"l2_{col}"))
    return (
        frame.with_columns(
            [
                pl.col("ts_minute")
                .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                .dt.truncate("1m")
                .alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(["timestamp_m1", "exchange", "symbol", *renamed])
        .sort("timestamp_m1")
    )


def l2_invalid_mask_expr(pl: Any, columns: set[str]) -> Any:
    """Build the invalid-row predicate for L2 quality columns."""

    cond = pl.lit(False)
    if "l2_coverage_ratio" in columns:
        cond = cond | (pl.col("l2_coverage_ratio") < 0.0) | (pl.col("l2_coverage_ratio") > 1.0)
    if "l2_snapshot_count" in columns:
        cond = cond | (pl.col("l2_snapshot_count") < 0)
    if "l2_first_snapshot_ts" in columns and "l2_last_snapshot_ts" in columns:
        cond = cond | (pl.col("l2_first_snapshot_ts") > pl.col("l2_last_snapshot_ts"))
    return cond


def validate_or_filter_l2_quality(pl: Any, frame: Any, mode: str) -> tuple[Any, dict[str, int]]:
    """Validate L2 quality fields or drop invalid L2 rows in lenient mode.

    Raises:
        ValueError: If mode is unsupported, required quality columns are absent, or strict mode
            finds invalid rows.
    """

    if mode not in {"strict", "lenient"}:
        raise ValueError(f"Unsupported l2_validation_mode: {mode}")
    l2_columns = set(frame.columns)
    if "l2_coverage_ratio" not in l2_columns and "l2_snapshot_count" not in l2_columns:
        raise ValueError("L2 validation failed: no supported L2 quality columns present")
    invalid_mask = l2_invalid_mask_expr(pl, l2_columns)
    invalid_rows = frame.filter(invalid_mask).height
    if invalid_rows == 0:
        return frame, {"l2_invalid_rows_found": 0, "l2_invalid_rows_dropped": 0}
    if mode == "strict":
        raise ValueError(f"L2 validation failed: {invalid_rows} invalid rows detected")
    filtered = frame.filter(~invalid_mask)
    dropped = frame.height - filtered.height
    return filtered, {"l2_invalid_rows_found": invalid_rows, "l2_invalid_rows_dropped": dropped}


def read_dataset_frame(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    dataset_type: str,
    timeframe: str,
) -> Any:
    """Read the newest Silver parquet frame for a normalized Gold symbol.

    Raises:
        ValueError: If the required Silver dataset is missing for the symbol.
    """

    pl = require_polars()
    dataset_root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    candidate_files: list[Path] = []
    symbol_dirs = sorted(dataset_root.glob(f"symbol=*/timeframe={timeframe}"))
    for sym_dir in symbol_dirs:
        sym_segment = sym_dir.parent.name
        if not sym_segment.startswith("symbol="):
            continue
        raw_symbol = sym_segment.split("=", 1)[1]
        if normalize_symbol(raw_symbol) != symbol:
            continue
        candidate_files.extend(path for path in sorted(sym_dir.glob("**/*.parquet")) if path.is_file())
    if not candidate_files:
        raise ValueError(f"Missing silver dataset for symbol={symbol}: {dataset_type}")
    selected_file = max(candidate_files, key=lambda path: (path.stat().st_mtime, str(path)))
    return pl.read_parquet(str(selected_file))


def prepare_spot_ohlcv_or_perp(pl: Any, frame: Any, prefix: str, symbol: str) -> Any:
    """Prepare spot_ohlcv or perpetual OHLCV bars for the Gold join contract."""

    return (
        frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias(f"{prefix}_open_price"),
                pl.col("high_price").cast(pl.Float64).alias(f"{prefix}_high_price"),
                pl.col("low_price").cast(pl.Float64).alias(f"{prefix}_low_price"),
                pl.col("close_price").cast(pl.Float64).alias(f"{prefix}_close_price"),
                pl.col("volume").cast(pl.Float64).alias(f"{prefix}_volume"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_open_interest(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare open-interest features for the Gold join contract."""

    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_interest").cast(pl.Float64).alias("open_interest_open_interest"),
                pl.col("open_interest_is_observed").cast(pl.Boolean),
                pl.col("open_interest_is_ffill").cast(pl.Boolean),
                pl.col("minutes_since_open_interest_observation").cast(pl.Int64),
                pl.col("open_interest_observation_lag_sec").cast(pl.Int64),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_funding(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare funding-rate features for the Gold join contract."""

    return (
        frame.with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("funding_rate_last_known").cast(pl.Float64),
                pl.col("minutes_since_funding").cast(pl.Int64),
                pl.col("is_funding_observation_minute").cast(pl.Boolean),
                pl.col("funding_data_available").cast(pl.Boolean),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_trades(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare perpetual trade aggregate features for the Gold join contract."""

    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias("trades_open_price"),
                pl.col("high_price").cast(pl.Float64).alias("trades_high_price"),
                pl.col("low_price").cast(pl.Float64).alias("trades_low_price"),
                pl.col("close_price").cast(pl.Float64).alias("trades_close_price"),
                pl.col("volume").cast(pl.Float64).alias("trades_volume"),
                pl.col("quote_volume").cast(pl.Float64).alias("trades_quote_volume"),
                pl.col("trade_count").cast(pl.Int64).alias("trades_trade_count"),
                pl.col("buy_volume").cast(pl.Float64).alias("trades_buy_volume"),
                pl.col("sell_volume").cast(pl.Float64).alias("trades_sell_volume"),
                pl.col("buy_trade_count").cast(pl.Int64).alias("trades_buy_trade_count"),
                pl.col("sell_trade_count").cast(pl.Int64).alias("trades_sell_trade_count"),
                pl.col("buy_volume_share").cast(pl.Float64).alias("trades_buy_volume_share"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_options_trades(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare option trade aggregate features for the Gold join contract."""

    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias("options_trades_open_price"),
                pl.col("high_price").cast(pl.Float64).alias("options_trades_high_price"),
                pl.col("low_price").cast(pl.Float64).alias("options_trades_low_price"),
                pl.col("close_price").cast(pl.Float64).alias("options_trades_close_price"),
                pl.col("volume").cast(pl.Float64).alias("options_trades_volume"),
                pl.col("quote_volume").cast(pl.Float64).alias("options_trades_quote_volume"),
                pl.col("trade_count").cast(pl.Int64).alias("options_trades_trade_count"),
                pl.col("buy_volume").cast(pl.Float64).alias("options_trades_buy_volume"),
                pl.col("sell_volume").cast(pl.Float64).alias("options_trades_sell_volume"),
                pl.col("buy_trade_count").cast(pl.Int64).alias("options_trades_buy_trade_count"),
                pl.col("sell_trade_count").cast(pl.Int64).alias("options_trades_sell_trade_count"),
                pl.col("buy_volume_share").cast(pl.Float64).alias("options_trades_buy_volume_share"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_volatility_index_data(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare observed volatility-index features for the Gold join contract."""

    return (
        frame.with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("volatility_value").cast(pl.Float64).alias("volatility_index_value"),
                pl.col("volatility_open").cast(pl.Float64).alias("volatility_index_open"),
                pl.col("volatility_high").cast(pl.Float64).alias("volatility_index_high"),
                pl.col("volatility_low").cast(pl.Float64).alias("volatility_index_low"),
                pl.col("volatility_close").cast(pl.Float64).alias("volatility_index_close"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_dataset_frame(pl: Any, dataset_type: str, frame: Any, symbol: str) -> Any:
    """Dispatch preparation for a supported Gold source dataset.

    Raises:
        ValueError: If the dataset type is not part of the Gold source contract.
    """

    dataset_preparers: dict[str, Any] = {
        "spot_ohlcv": lambda: prepare_spot_ohlcv_or_perp(pl, frame, "spot_ohlcv", symbol),
        "perps_ohlcv": lambda: prepare_spot_ohlcv_or_perp(pl, frame, "perp", symbol),
        "open_interest_1m_feature": lambda: prepare_open_interest(pl, frame, symbol),
        "funding_1m_feature": lambda: prepare_funding(pl, frame, symbol),
        "perps_trades_1m_feature": lambda: prepare_trades(pl, frame, symbol),
        "options_trades_1m_feature": lambda: prepare_options_trades(pl, frame, symbol),
        "volatility_index_data_observed": lambda: prepare_volatility_index_data(pl, frame, symbol),
        "gold_l2_m1": lambda: prepare_l2(pl, frame, symbol),
    }
    preparer = dataset_preparers.get(dataset_type)
    if preparer is None:
        raise ValueError(f"Unsupported dataset_type for preparation: {dataset_type}")
    return preparer()


def build_minute_grid(pl: Any, prepared: list[Any], exchange: str, symbol: str) -> Any:
    """Build a complete minute grid covering all prepared source frames.

    Raises:
        ValueError: If no prepared frame provides timestamp coverage.
    """

    mins: list[datetime] = []
    maxs: list[datetime] = []
    for frame in prepared:
        if frame.height == 0:
            continue
        min_ts = frame.select(pl.col("timestamp_m1").min()).item()
        max_ts = frame.select(pl.col("timestamp_m1").max()).item()
        if isinstance(min_ts, datetime) and isinstance(max_ts, datetime):
            mins.append(min_ts)
            maxs.append(max_ts)
    if not mins or not maxs:
        raise ValueError("No timestamp coverage available across prepared datasets")
    start = min(mins)
    end = max(maxs)
    timestamp_grid = pl.datetime_range(start, end, interval="1m", eager=True).alias("timestamp_m1")
    return pl.DataFrame({"timestamp_m1": timestamp_grid}).with_columns(
        [pl.lit(exchange).alias("exchange"), pl.lit(symbol).alias("symbol")]
    )
