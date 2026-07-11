"""Gold dataset frame discovery, preparation, and merge-grid helpers."""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

STRATEGY_FEATURE_LOOKBACKS: dict[str, str] = {
    "strategy_momentum_log_return_1m": "1m",
    "strategy_momentum_log_return_5m": "5m",
    "strategy_momentum_log_return_15m": "15m",
    "strategy_momentum_vol_scaled_return_5m": "5m",
    "strategy_momentum_vol_scaled_return_15m": "15m",
    "strategy_trend_ema_3m": "3m",
    "strategy_trend_ema_slope_3m": "3m",
    "strategy_trend_ema_10m": "10m",
    "strategy_trend_ema_slope_10m": "10m",
    "strategy_trend_breakout_distance_5m": "5m",
    "strategy_trend_breakout_distance_15m": "15m",
    "strategy_trend_persistence_5m": "5m",
    "strategy_trend_persistence_15m": "15m",
    "strategy_reversion_price_zscore_5m": "5m",
    "strategy_reversion_price_zscore_15m": "15m",
    "strategy_reversion_vwap_distance_5m": "5m",
    "strategy_reversion_vwap_distance_15m": "15m",
    "strategy_reversion_bollinger_distance_5m": "5m",
    "strategy_reversion_bollinger_distance_15m": "15m",
    "strategy_reversion_spot_perp_spread_zscore_5m": "5m",
    "strategy_reversion_spot_perp_spread_zscore_15m": "15m",
    "strategy_reversion_half_life_5m": "5m",
    "strategy_cost_turnover_notional_1m": "1m",
    "strategy_cost_turnover_notional_5m": "5m",
    "strategy_cost_turnover_notional_15m": "15m",
    "strategy_cost_spot_perp_spread": "1m",
}

PREDICTION_TARGET_HORIZONS: dict[str, int] = {
    "1h": 60,
    "4h": 240,
    "1d": 1440,
}
PREDICTION_TARGET_DEFINITIONS: dict[str, object] = {
    "horizons": PREDICTION_TARGET_HORIZONS,
    "transaction_cost_bps": 2.0,
    "regime_shift_zscore_delta": 1.0,
    "null_rule": "future window must contain the full horizon; otherwise target and label values are null",
    "price_return": "log(perp_close_price[t+horizon] / perp_close_price[t])",
    "drawdown": "min(perp_close_price[t+1:t+horizon]) / perp_close_price[t] - 1",
    "cost_adjusted_return": (
        "forward_return - transaction_cost_bps/10000 - abs(funding_rate_last_known)*horizon_minutes/480"
    ),
    "future_rv": "realized-volatility feature value at t+horizon",
    "iv_change": "iv_rv spread feature value at t+horizon minus value at t",
    "regime_shift_label": "abs(future iv_rv_zscore_1d - current iv_rv_zscore_1d) >= regime_shift_zscore_delta",
}


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


def _silver_dataset_roots(*, silver_root: str, exchange: str, dataset_type: str) -> list[Path]:
    """Return canonical Silver dataset root plus supported legacy fallback roots."""

    dataset_types = [dataset_type]
    if dataset_type == "perps_ohlcv":
        dataset_types.append("perp")
    return [Path(silver_root) / f"dataset_type={candidate}" / f"exchange={exchange}" for candidate in dataset_types]


def discover_symbols_for_dataset(
    *,
    silver_root: str,
    exchange: str,
    dataset_type: str,
    timeframe: str,
) -> set[str]:
    """Discover normalized symbols available for one Silver dataset and timeframe."""

    symbols: set[str] = set()
    for root in _silver_dataset_roots(silver_root=silver_root, exchange=exchange, dataset_type=dataset_type):
        if not root.exists():
            continue
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
    candidate_files: list[Path] = []
    for dataset_root in _silver_dataset_roots(silver_root=silver_root, exchange=exchange, dataset_type=dataset_type):
        symbol_dirs = sorted(dataset_root.glob(f"symbol=*/timeframe={timeframe}"))
        for sym_dir in symbol_dirs:
            sym_segment = sym_dir.parent.name
            if not sym_segment.startswith("symbol="):
                continue
            raw_symbol = sym_segment.split("=", 1)[1]
            if normalize_symbol(raw_symbol) != symbol:
                continue
            candidate_files.extend(path for path in sorted(sym_dir.glob("**/*.parquet")) if path.is_file())
        if candidate_files:
            break
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


def prepare_volatility_index_feature(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare live-origin volatility-index features without changing IV feature semantics."""

    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.col("iv_source_timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("iv_open").cast(pl.Float64),
                pl.col("iv_high").cast(pl.Float64),
                pl.col("iv_low").cast(pl.Float64),
                pl.col("iv_close").cast(pl.Float64),
                pl.col("iv_range").cast(pl.Float64),
                pl.col("iv_return_1m").cast(pl.Float64),
                pl.col("iv_change_5m").cast(pl.Float64),
                pl.col("iv_change_15m").cast(pl.Float64),
                pl.col("iv_change_1h").cast(pl.Float64),
                pl.col("iv_zscore_1d").cast(pl.Float64),
                pl.col("iv_zscore_7d").cast(pl.Float64),
                pl.col("iv_percentile_30d").cast(pl.Float64),
                pl.col("iv_source_dataset").cast(pl.Utf8),
                pl.col("iv_source_timestamp"),
                pl.col("minutes_since_iv_observation").cast(pl.Int64),
                pl.col("iv_data_available").cast(pl.Boolean),
                pl.col("iv_source_timestamp").alias("as_of"),
                pl.lit(True).alias("live_snapshot_derived"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_iv_rv(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare IV/RV spread features for the Gold join contract."""

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
                pl.col("iv_minus_rv_1h").cast(pl.Float64),
                pl.col("iv_minus_rv_1d").cast(pl.Float64),
                pl.col("iv_rv_ratio_1h").cast(pl.Float64),
                pl.col("iv_rv_ratio_1d").cast(pl.Float64),
                pl.col("iv_rv_zscore_1d").cast(pl.Float64),
                pl.col("iv_rv_percentile_30d").cast(pl.Float64),
                pl.col("minutes_since_iv_observation").cast(pl.Int64),
                pl.col("minutes_since_rv_observation").cast(pl.Int64),
                pl.col("iv_available").cast(pl.Boolean),
                pl.col("rv_available").cast(pl.Boolean),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_index_price(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare index-price features for the Gold join contract."""

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
                pl.col("index_price").cast(pl.Float64),
                pl.col("index_price_is_observed").cast(pl.Boolean),
                pl.col("minutes_since_index_price_observation").cast(pl.Int64),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_futures_summary(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare futures-summary features for the Gold join contract."""

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
                pl.col("instrument_type").cast(pl.Utf8).alias("futures_summary_instrument_type"),
                pl.col("mark_price").cast(pl.Float64).alias("futures_summary_mark_price"),
                pl.col("index_price").cast(pl.Float64).alias("futures_summary_index_price"),
                pl.col("mark_index_spread").cast(pl.Float64).alias("futures_summary_mark_index_spread"),
                pl.col("mark_index_ratio").cast(pl.Float64).alias("futures_summary_mark_index_ratio"),
                pl.col("open_interest").cast(pl.Float64).alias("futures_summary_open_interest"),
                pl.col("volume").cast(pl.Float64).alias("futures_summary_volume"),
                pl.col("turnover").cast(pl.Float64).alias("futures_summary_turnover"),
                pl.col("funding_rate").cast(pl.Float64).alias("futures_summary_funding_rate"),
                pl.col("summary_is_observed").cast(pl.Boolean).alias("futures_summary_is_observed"),
                pl.col("minutes_since_summary_observation").cast(pl.Int64),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_realized_volatility(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare internally computed realized-volatility regime features."""

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
                pl.col("rv_5m").cast(pl.Float64),
                pl.col("rv_15m").cast(pl.Float64),
                pl.col("rv_1h").cast(pl.Float64),
                pl.col("rv_4h").cast(pl.Float64),
                pl.col("rv_1d").cast(pl.Float64),
                pl.col("parkinson_rv_1h").cast(pl.Float64),
                pl.col("jump_proxy").cast(pl.Float64),
                pl.col("spot_available").cast(pl.Boolean),
                pl.col("perps_available").cast(pl.Boolean),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_options_surface(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare option-surface proxies with source-specific Gold names."""

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
                pl.col("atm_iv").cast(pl.Float64).alias("options_surface_atm_iv"),
                pl.col("short_dated_iv").cast(pl.Float64).alias("options_surface_short_dated_iv"),
                pl.col("skew").cast(pl.Float64).alias("options_surface_skew"),
                pl.col("term_structure").cast(pl.Float64).alias("options_surface_term_structure"),
                pl.col("put_call_iv_spread").cast(pl.Float64).alias("options_surface_put_call_iv_spread"),
                pl.col("contract_count").cast(pl.Int64).alias("options_surface_contract_count"),
                pl.col("fresh_quote_count").cast(pl.Int64).alias("options_surface_fresh_quote_count"),
                pl.col("stale_quote_count").cast(pl.Int64).alias("options_surface_stale_quote_count"),
                pl.col("max_quote_age_seconds").cast(pl.Float64).alias("options_surface_max_quote_age_seconds"),
                pl.col("quote_coverage_ratio").cast(pl.Float64).alias("options_surface_quote_coverage_ratio"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_perps_l2_feature(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare perpetual L2 liquidity features for the regime contract."""

    numeric = (
        "best_bid_price",
        "best_ask_price",
        "mid_price",
        "spread",
        "top_bid_size",
        "top_ask_size",
        "top_of_book_imbalance",
        "bid_depth_10bps",
        "ask_depth_10bps",
        "bid_depth_50bps",
        "ask_depth_50bps",
        "quote_age_seconds",
    )
    expressions = [pl.col(column).cast(pl.Float64).alias(f"perps_l2_{column}") for column in numeric]
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
                *expressions,
                pl.col("quote_available").cast(pl.Boolean).alias("perps_l2_quote_available"),
                pl.col("stale_quote").cast(pl.Boolean).alias("perps_l2_stale_quote"),
                pl.col("minutes_since_l2_observation").cast(pl.Int64).alias("perps_l2_minutes_since_observation"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_options_l2_feature(pl: Any, frame: Any, symbol: str) -> Any:
    """Aggregate contract-level option liquidity into one regime row per minute."""

    normalized = frame.with_columns(
        [
            pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
            pl.lit(symbol).alias("symbol"),
        ]
    )
    return (
        normalized.group_by(["timestamp_m1", "exchange", "symbol"], maintain_order=True)
        .agg(
            [
                pl.col("instrument_name").n_unique().cast(pl.Int64).alias("options_l2_contract_count"),
                pl.col("quote_available").cast(pl.Float64).mean().alias("options_l2_quote_coverage_ratio"),
                pl.col("stale_quote").cast(pl.Float64).mean().alias("options_l2_stale_quote_ratio"),
                pl.col("spread").median().cast(pl.Float64).alias("options_l2_median_spread"),
                pl.col("top_bid_size").sum().cast(pl.Float64).alias("options_l2_top_bid_depth"),
                pl.col("top_ask_size").sum().cast(pl.Float64).alias("options_l2_top_ask_depth"),
                pl.col("bid_depth_10bps").sum().cast(pl.Float64).alias("options_l2_bid_depth_10bps"),
                pl.col("ask_depth_10bps").sum().cast(pl.Float64).alias("options_l2_ask_depth_10bps"),
                pl.col("bid_depth_50bps").sum().cast(pl.Float64).alias("options_l2_bid_depth_50bps"),
                pl.col("ask_depth_50bps").sum().cast(pl.Float64).alias("options_l2_ask_depth_50bps"),
                pl.col("quote_age_seconds").max().cast(pl.Float64).alias("options_l2_max_quote_age_seconds"),
            ]
        )
        .sort("timestamp_m1")
    )


def prepare_historical_volatility(pl: Any, frame: Any, symbol: str) -> Any:
    """Prepare the external historical-volatility reference without filling gaps."""

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
                pl.col("historical_volatility").cast(pl.Float64).alias("historical_volatility_reference"),
                pl.col("historical_volatility_source_timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(True).alias("historical_volatility_available"),
            ]
        )
        .sort("timestamp_m1")
    )


def optional_feature_schema(pl: Any, dataset_type: str) -> list[tuple[str, Any]]:
    """Return stable nullable Gold columns for one optional Silver source."""

    schemas: dict[str, list[tuple[str, Any]]] = {
        "perps_l2_1m_feature": [
            *[
                (f"perps_l2_{name}", pl.Float64)
                for name in (
                    "best_bid_price",
                    "best_ask_price",
                    "mid_price",
                    "spread",
                    "top_bid_size",
                    "top_ask_size",
                    "top_of_book_imbalance",
                    "bid_depth_10bps",
                    "ask_depth_10bps",
                    "bid_depth_50bps",
                    "ask_depth_50bps",
                    "quote_age_seconds",
                )
            ],
            ("perps_l2_quote_available", pl.Boolean),
            ("perps_l2_stale_quote", pl.Boolean),
            ("perps_l2_minutes_since_observation", pl.Int64),
        ],
        "options_l2_1m_feature": [
            ("options_l2_contract_count", pl.Int64),
            ("options_l2_quote_coverage_ratio", pl.Float64),
            ("options_l2_stale_quote_ratio", pl.Float64),
            ("options_l2_median_spread", pl.Float64),
            ("options_l2_top_bid_depth", pl.Float64),
            ("options_l2_top_ask_depth", pl.Float64),
            ("options_l2_bid_depth_10bps", pl.Float64),
            ("options_l2_ask_depth_10bps", pl.Float64),
            ("options_l2_bid_depth_50bps", pl.Float64),
            ("options_l2_ask_depth_50bps", pl.Float64),
            ("options_l2_max_quote_age_seconds", pl.Float64),
        ],
        "options_surface_1m_feature": [
            ("options_surface_atm_iv", pl.Float64),
            ("options_surface_short_dated_iv", pl.Float64),
            ("options_surface_skew", pl.Float64),
            ("options_surface_term_structure", pl.Float64),
            ("options_surface_put_call_iv_spread", pl.Float64),
            ("options_surface_contract_count", pl.Int64),
            ("options_surface_fresh_quote_count", pl.Int64),
            ("options_surface_stale_quote_count", pl.Int64),
            ("options_surface_max_quote_age_seconds", pl.Float64),
            ("options_surface_quote_coverage_ratio", pl.Float64),
        ],
        "index_price_1m_feature": [
            ("index_price", pl.Float64),
            ("index_price_is_observed", pl.Boolean),
            ("minutes_since_index_price_observation", pl.Int64),
        ],
        "futures_summary_1m_feature": [
            ("futures_summary_instrument_type", pl.Utf8),
            ("futures_summary_mark_price", pl.Float64),
            ("futures_summary_index_price", pl.Float64),
            ("futures_summary_mark_index_spread", pl.Float64),
            ("futures_summary_mark_index_ratio", pl.Float64),
            ("futures_summary_open_interest", pl.Float64),
            ("futures_summary_volume", pl.Float64),
            ("futures_summary_turnover", pl.Float64),
            ("futures_summary_funding_rate", pl.Float64),
            ("futures_summary_is_observed", pl.Boolean),
            ("minutes_since_summary_observation", pl.Int64),
        ],
        "historical_volatility_observed": [
            ("historical_volatility_reference", pl.Float64),
            (
                "historical_volatility_source_timestamp",
                pl.Datetime(time_unit="us", time_zone="UTC"),
            ),
            ("historical_volatility_available", pl.Boolean),
        ],
    }
    try:
        return schemas[dataset_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported optional Gold dataset_type: {dataset_type}") from exc


def strategy_feature_lookbacks() -> dict[str, str]:
    """Return declared trailing lookbacks for Gold strategy feature families."""

    return dict(STRATEGY_FEATURE_LOOKBACKS)


def prediction_target_definitions() -> dict[str, object]:
    """Return versioned definitions for forward-looking Gold prediction targets."""

    return dict(PREDICTION_TARGET_DEFINITIONS)


def add_strategy_feature_families(pl: Any, frame: Any) -> Any:
    """Add trailing strategy state features without forward-looking labels or targets."""

    group = ["exchange", "symbol"]
    sorted_frame = frame.sort("timestamp_m1")
    price = pl.col("perp_close_price")
    spot_price = pl.col("spot_ohlcv_close_price")
    volume = pl.col("perp_volume")

    enriched = sorted_frame.with_columns(
        [
            _safe_log_return(pl, "perp_close_price", 1).alias("strategy_momentum_log_return_1m"),
            _safe_log_return(pl, "perp_close_price", 5).alias("strategy_momentum_log_return_5m"),
            _safe_log_return(pl, "perp_close_price", 15).alias("strategy_momentum_log_return_15m"),
            (_safe_ratio(pl, price, spot_price) - 1.0).alias("strategy_cost_spot_perp_spread"),
            (price * volume).alias("strategy_cost_turnover_notional_1m"),
        ]
    )
    enriched = enriched.with_columns(
        [
            price.ewm_mean(span=3, adjust=False).over(group).alias("strategy_trend_ema_3m"),
            price.ewm_mean(span=10, adjust=False).over(group).alias("strategy_trend_ema_10m"),
            price.rolling_max_by("timestamp_m1", window_size="5m", min_samples=2)
            .over(group)
            .alias("_strategy_price_high_5m"),
            price.rolling_max_by("timestamp_m1", window_size="15m", min_samples=2)
            .over(group)
            .alias("_strategy_price_high_15m"),
            price.rolling_mean_by("timestamp_m1", window_size="5m", min_samples=2)
            .over(group)
            .alias("_strategy_price_mean_5m"),
            price.rolling_mean_by("timestamp_m1", window_size="15m", min_samples=2)
            .over(group)
            .alias("_strategy_price_mean_15m"),
            price.rolling_std_by("timestamp_m1", window_size="5m", min_samples=2)
            .over(group)
            .alias("_strategy_price_std_5m"),
            price.rolling_std_by("timestamp_m1", window_size="15m", min_samples=2)
            .over(group)
            .alias("_strategy_price_std_15m"),
            (price * volume)
            .rolling_sum_by("timestamp_m1", window_size="5m", min_samples=1)
            .over(group)
            .alias("_strategy_price_volume_sum_5m"),
            (price * volume)
            .rolling_sum_by("timestamp_m1", window_size="15m", min_samples=1)
            .over(group)
            .alias("_strategy_price_volume_sum_15m"),
            volume.rolling_sum_by("timestamp_m1", window_size="5m", min_samples=1)
            .over(group)
            .alias("_strategy_volume_sum_5m"),
            volume.rolling_sum_by("timestamp_m1", window_size="15m", min_samples=1)
            .over(group)
            .alias("_strategy_volume_sum_15m"),
            pl.col("strategy_cost_turnover_notional_1m")
            .rolling_sum_by("timestamp_m1", window_size="5m", min_samples=1)
            .over(group)
            .alias("strategy_cost_turnover_notional_5m"),
            pl.col("strategy_cost_turnover_notional_1m")
            .rolling_sum_by("timestamp_m1", window_size="15m", min_samples=1)
            .over(group)
            .alias("strategy_cost_turnover_notional_15m"),
            pl.col("strategy_cost_spot_perp_spread")
            .rolling_mean_by("timestamp_m1", window_size="5m", min_samples=2)
            .over(group)
            .alias("_strategy_spread_mean_5m"),
            pl.col("strategy_cost_spot_perp_spread")
            .rolling_mean_by("timestamp_m1", window_size="15m", min_samples=2)
            .over(group)
            .alias("_strategy_spread_mean_15m"),
            pl.col("strategy_cost_spot_perp_spread")
            .rolling_std_by("timestamp_m1", window_size="5m", min_samples=2)
            .over(group)
            .alias("_strategy_spread_std_5m"),
            pl.col("strategy_cost_spot_perp_spread")
            .rolling_std_by("timestamp_m1", window_size="15m", min_samples=2)
            .over(group)
            .alias("_strategy_spread_std_15m"),
        ]
    )
    enriched = enriched.with_columns(
        [
            (
                _safe_ratio(pl, pl.col("strategy_trend_ema_3m"), pl.col("strategy_trend_ema_3m").shift(1).over(group))
                - 1.0
            ).alias("strategy_trend_ema_slope_3m"),
            (
                _safe_ratio(
                    pl,
                    pl.col("strategy_trend_ema_10m"),
                    pl.col("strategy_trend_ema_10m").shift(1).over(group),
                )
                - 1.0
            ).alias("strategy_trend_ema_slope_10m"),
            (_safe_ratio(pl, price, pl.col("_strategy_price_high_5m")) - 1.0).alias(
                "strategy_trend_breakout_distance_5m"
            ),
            (_safe_ratio(pl, price, pl.col("_strategy_price_high_15m")) - 1.0).alias(
                "strategy_trend_breakout_distance_15m"
            ),
            _rolling_direction_score(pl, "strategy_momentum_log_return_1m", "5m").alias(
                "strategy_trend_persistence_5m"
            ),
            _rolling_direction_score(pl, "strategy_momentum_log_return_1m", "15m").alias(
                "strategy_trend_persistence_15m"
            ),
            _safe_ratio(pl, pl.col("strategy_momentum_log_return_5m"), pl.col("rv_1h")).alias(
                "strategy_momentum_vol_scaled_return_5m"
            ),
            _safe_ratio(pl, pl.col("strategy_momentum_log_return_15m"), pl.col("rv_1h")).alias(
                "strategy_momentum_vol_scaled_return_15m"
            ),
            _safe_ratio(pl, price - pl.col("_strategy_price_mean_5m"), pl.col("_strategy_price_std_5m")).alias(
                "strategy_reversion_price_zscore_5m"
            ),
            _safe_ratio(pl, price - pl.col("_strategy_price_mean_15m"), pl.col("_strategy_price_std_15m")).alias(
                "strategy_reversion_price_zscore_15m"
            ),
            _safe_ratio(
                pl,
                price - _safe_ratio(pl, pl.col("_strategy_price_volume_sum_5m"), pl.col("_strategy_volume_sum_5m")),
                _safe_ratio(pl, pl.col("_strategy_price_volume_sum_5m"), pl.col("_strategy_volume_sum_5m")),
            ).alias("strategy_reversion_vwap_distance_5m"),
            _safe_ratio(
                pl,
                price - _safe_ratio(pl, pl.col("_strategy_price_volume_sum_15m"), pl.col("_strategy_volume_sum_15m")),
                _safe_ratio(pl, pl.col("_strategy_price_volume_sum_15m"), pl.col("_strategy_volume_sum_15m")),
            ).alias("strategy_reversion_vwap_distance_15m"),
            _safe_ratio(pl, price - pl.col("_strategy_price_mean_5m"), pl.col("_strategy_price_std_5m") * 2.0).alias(
                "strategy_reversion_bollinger_distance_5m"
            ),
            _safe_ratio(
                pl,
                price - pl.col("_strategy_price_mean_15m"),
                pl.col("_strategy_price_std_15m") * 2.0,
            ).alias("strategy_reversion_bollinger_distance_15m"),
            _safe_ratio(
                pl,
                pl.col("strategy_cost_spot_perp_spread") - pl.col("_strategy_spread_mean_5m"),
                pl.col("_strategy_spread_std_5m"),
            ).alias("strategy_reversion_spot_perp_spread_zscore_5m"),
            _safe_ratio(
                pl,
                pl.col("strategy_cost_spot_perp_spread") - pl.col("_strategy_spread_mean_15m"),
                pl.col("_strategy_spread_std_15m"),
            ).alias("strategy_reversion_spot_perp_spread_zscore_15m"),
        ]
    )
    enriched = enriched.with_columns(
        [
            (price - pl.col("_strategy_price_mean_5m")).alias("_strategy_reversion_deviation"),
        ]
    )
    enriched = enriched.with_columns(
        [
            pl.col("_strategy_reversion_deviation").shift(1).over(group).alias("_strategy_reversion_deviation_lag1"),
        ]
    )
    enriched = enriched.with_columns(
        [
            (pl.col("_strategy_reversion_deviation") * pl.col("_strategy_reversion_deviation_lag1"))
            .rolling_sum_by("timestamp_m1", window_size="5m", min_samples=3)
            .over(group)
            .alias("_strategy_reversion_autocov_5m"),
            pl.col("_strategy_reversion_deviation_lag1")
            .pow(2)
            .rolling_sum_by("timestamp_m1", window_size="5m", min_samples=3)
            .over(group)
            .alias("_strategy_reversion_lag_var_5m"),
        ]
    )
    enriched = enriched.with_columns(
        [
            _safe_ratio(pl, pl.col("_strategy_reversion_autocov_5m"), pl.col("_strategy_reversion_lag_var_5m")).alias(
                "_strategy_reversion_phi_5m"
            ),
        ]
    )
    return (
        enriched.with_columns(
            [
                pl.when((pl.col("_strategy_reversion_phi_5m") > 0.0) & (pl.col("_strategy_reversion_phi_5m") < 1.0))
                .then(0.6931471805599453 / -pl.col("_strategy_reversion_phi_5m").log())
                .otherwise(None)
                .alias("strategy_reversion_half_life_5m"),
            ]
        )
        .drop([column for column in enriched.columns if column.startswith("_strategy_")])
        .sort("timestamp_m1")
    )


def add_prediction_target_columns(pl: Any, frame: Any) -> Any:
    """Add forward-looking targets in a dedicated target dataset only."""

    sorted_frame = frame.sort(["exchange", "symbol", "timestamp_m1"])
    rows = sorted_frame.to_dicts()
    output_rows: list[dict[str, object]] = []
    for group_rows in _group_target_rows(rows):
        output_rows.extend(_prediction_target_rows(group_rows))
    target_frame = pl.DataFrame(output_rows)
    target_columns = [
        column
        for column in target_frame.columns
        if column in {"timestamp_m1", "exchange", "symbol"} or column.startswith(("target_", "label_"))
    ]
    return target_frame.select(target_columns).sort("timestamp_m1")


def _safe_log_return(pl: Any, column: str, periods: int) -> Any:
    current = pl.col(column)
    previous = pl.col(column).shift(periods).over(["exchange", "symbol"])
    return pl.when((current > 0.0) & (previous > 0.0)).then((current / previous).log()).otherwise(None)


def _safe_ratio(pl: Any, numerator: Any, denominator: Any) -> Any:
    return pl.when(denominator.is_not_null() & (denominator != 0.0)).then(numerator / denominator).otherwise(None)


def _rolling_direction_score(pl: Any, column: str, window_size: str) -> Any:
    direction = pl.when(pl.col(column) > 0.0).then(1.0).when(pl.col(column) < 0.0).then(-1.0).otherwise(0.0)
    return direction.rolling_mean_by("timestamp_m1", window_size=window_size, min_samples=2).over(
        ["exchange", "symbol"]
    )


def _group_target_rows(rows: list[dict[str, object]]) -> list[list[dict[str, object]]]:
    groups: list[list[dict[str, object]]] = []
    current_key: tuple[object, object] | None = None
    current_rows: list[dict[str, object]] = []
    for row in rows:
        key = (row["exchange"], row["symbol"])
        if current_key is not None and key != current_key:
            groups.append(current_rows)
            current_rows = []
        current_key = key
        current_rows.append(row)
    if current_rows:
        groups.append(current_rows)
    return groups


def _prediction_target_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for index, row in enumerate(rows):
        target_row = dict(row)
        current_price = _float_or_none(row.get("perp_close_price"))
        current_spread = _float_or_none(row.get("iv_minus_rv_1h"))
        current_zscore = _float_or_none(row.get("iv_rv_zscore_1d"))
        current_funding = _float_or_none(row.get("funding_rate_last_known")) or 0.0
        for label, horizon in PREDICTION_TARGET_HORIZONS.items():
            future_index = index + horizon
            future_row = rows[future_index] if future_index < len(rows) else None
            future_price = _float_or_none(future_row.get("perp_close_price")) if future_row is not None else None
            future_spread = _float_or_none(future_row.get("iv_minus_rv_1h")) if future_row is not None else None
            future_zscore = _float_or_none(future_row.get("iv_rv_zscore_1d")) if future_row is not None else None
            future_rv = _float_or_none(future_row.get("rv_1h")) if future_row is not None else None
            # Targets intentionally read strictly after the feature timestamp; incomplete future
            # windows stay null so training code cannot silently learn from partial horizons.
            future_window = rows[index + 1 : future_index + 1] if future_row is not None else []
            future_min_price = _min_float(row.get("perp_close_price") for row in future_window)
            forward_return = _forward_log_return(current_price, future_price)
            target_row[f"target_forward_return_{label}"] = forward_return
            target_row[f"target_forward_drawdown_{label}"] = (
                (future_min_price / current_price) - 1.0
                if current_price is not None and current_price > 0.0 and future_min_price is not None
                else None
            )
            target_row[f"target_cost_adjusted_return_{label}"] = (
                forward_return - 0.0002 - (abs(current_funding) * horizon / 480.0)
                if forward_return is not None
                else None
            )
            target_row[f"target_future_rv_{label}"] = future_rv
            target_row[f"target_future_iv_spread_change_{label}"] = (
                future_spread - current_spread if future_spread is not None and current_spread is not None else None
            )
            target_row[f"label_regime_shift_{label}"] = (
                abs(future_zscore - current_zscore) >= 1.0
                if future_zscore is not None and current_zscore is not None
                else None
            )
        output.append(target_row)
    return output


def _float_or_none(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _min_float(values: Iterable[object]) -> float | None:
    finite_values = []
    for value in values:
        number = _float_or_none(value)
        if number is not None:
            finite_values.append(number)
    return min(finite_values) if finite_values else None


def _forward_log_return(current_price: float | None, future_price: float | None) -> float | None:
    if current_price is None or future_price is None or current_price <= 0.0 or future_price <= 0.0:
        return None
    return math.log(future_price / current_price)


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
        "volatility_index_1m_feature": lambda: prepare_volatility_index_feature(pl, frame, symbol),
        "iv_rv_1m_feature": lambda: prepare_iv_rv(pl, frame, symbol),
        "index_price_1m_feature": lambda: prepare_index_price(pl, frame, symbol),
        "futures_summary_1m_feature": lambda: prepare_futures_summary(pl, frame, symbol),
        "realized_volatility_1m_feature": lambda: prepare_realized_volatility(pl, frame, symbol),
        "options_surface_1m_feature": lambda: prepare_options_surface(pl, frame, symbol),
        "perps_l2_1m_feature": lambda: prepare_perps_l2_feature(pl, frame, symbol),
        "options_l2_1m_feature": lambda: prepare_options_l2_feature(pl, frame, symbol),
        "historical_volatility_observed": lambda: prepare_historical_volatility(pl, frame, symbol),
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
