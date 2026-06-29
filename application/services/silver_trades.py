"""Silver trade-family frame transformations."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

from typing import Any

from application.dataset_contracts import SILVER_TRADES_M1_FEATURE_COLUMNS, SILVER_TRADES_OBSERVED_COLUMNS


def build_trade_feature_frame(pl: Any, frame: Any, *, symbol: str) -> Any:
    """Build 1m trade-flow feature frame from observed tick trades."""

    enriched = frame.with_columns(
        [
            pl.col("trade_time").dt.truncate("1m").alias("timestamp_m1"),
            (pl.col("price") * pl.col("quantity")).alias("notional"),
            (pl.col("side") == "buy").alias("is_buy"),
            (pl.col("side") == "sell").alias("is_sell"),
        ]
    )
    return (
        enriched.group_by(["timestamp_m1", "exchange", "symbol", "instrument_type"], maintain_order=True)
        .agg(
            [
                pl.col("price").first().alias("open_price"),
                pl.col("price").max().alias("high_price"),
                pl.col("price").min().alias("low_price"),
                pl.col("price").last().alias("close_price"),
                pl.col("quantity").sum().alias("volume"),
                pl.col("notional").sum().alias("quote_volume"),
                pl.len().cast(pl.Int64).alias("trade_count"),
                pl.col("quantity").filter(pl.col("is_buy")).sum().fill_null(0.0).alias("buy_volume"),
                pl.col("quantity").filter(pl.col("is_sell")).sum().fill_null(0.0).alias("sell_volume"),
                pl.col("is_buy").cast(pl.Int64).sum().alias("buy_trade_count"),
                pl.col("is_sell").cast(pl.Int64).sum().alias("sell_trade_count"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("volume") > 0.0)
                .then(pl.col("buy_volume") / pl.col("volume"))
                .otherwise(0.0)
                .alias("buy_volume_share"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .sort("timestamp_m1")
        .select(SILVER_TRADES_M1_FEATURE_COLUMNS)
    )


def build_trade_observed_frame(pl: Any, frame: Any) -> tuple[Any, int, int]:
    """Validate/clean raw bronze trade rows and return observed ticks + quality counts."""

    typed = frame.with_columns(
        [
            pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("trade_time"),
            pl.col("price").cast(pl.Float64),
            pl.col("quantity").cast(pl.Float64),
            pl.col("trade_id").cast(pl.Utf8),
            pl.col("side").cast(pl.Utf8).str.to_lowercase(),
            pl.col("symbol").cast(pl.Utf8).alias("symbol"),
            pl.col("exchange").cast(pl.Utf8).str.to_lowercase().alias("exchange"),
            pl.col("instrument_type").cast(pl.Utf8).str.to_lowercase().alias("instrument_type"),
        ]
    )
    invalid_expr = (
        pl.col("trade_time").is_null()
        | pl.col("trade_id").is_null()
        | pl.col("price").is_null()
        | (~pl.col("price").is_finite())
        | (pl.col("price") <= 0.0)
        | pl.col("quantity").is_null()
        | (~pl.col("quantity").is_finite())
        | (pl.col("quantity") <= 0.0)
    )
    invalid_rows = int(typed.select(invalid_expr.cast(pl.Int64).sum()).item() or 0)
    cleaned = typed.filter(~invalid_expr)
    observed = (
        cleaned.sort(["trade_time", "ingested_at"])
        .unique(
            subset=["exchange", "instrument_type", "symbol", "trade_time", "trade_id"],
            keep="last",
            maintain_order=True,
        )
        .sort("trade_time")
        .select(SILVER_TRADES_OBSERVED_COLUMNS)
    )
    return observed, invalid_rows, cleaned.height
