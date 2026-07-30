"""Silver trade-family frame transformations."""
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

from typing import Any

from application.dataset_contracts import SILVER_TRADES_M1_FEATURE_COLUMNS, SILVER_TRADES_OBSERVED_COLUMNS


def _empty_trade_feature_frame(pl: Any, empty_minutes_frame: Any) -> Any:
    """Build neutral 1m feature rows for minutes confirmed empty by Bronze."""

    return (
        empty_minutes_frame.filter(pl.col("status") == "confirmed_empty")
        .with_columns(
            [
                pl.col("minute").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.col("exchange").cast(pl.Utf8).str.to_lowercase().alias("exchange"),
                pl.col("symbol").cast(pl.Utf8).alias("symbol"),
                pl.col("instrument_type").cast(pl.Utf8).str.to_lowercase().alias("instrument_type"),
            ]
        )
        .select(["timestamp_m1", "exchange", "symbol", "instrument_type"])
        .unique(maintain_order=True)
        .with_columns(
            [
                pl.lit(None).cast(pl.Float64).alias("open_price"),
                pl.lit(None).cast(pl.Float64).alias("high_price"),
                pl.lit(None).cast(pl.Float64).alias("low_price"),
                pl.lit(None).cast(pl.Float64).alias("close_price"),
                pl.lit(0.0).alias("volume"),
                pl.lit(0.0).alias("quote_volume"),
                pl.lit(0).cast(pl.Int64).alias("trade_count"),
                pl.lit(0.0).alias("buy_volume"),
                pl.lit(0.0).alias("sell_volume"),
                pl.lit(0).cast(pl.Int64).alias("buy_trade_count"),
                pl.lit(0).cast(pl.Int64).alias("sell_trade_count"),
                pl.lit(0.0).alias("buy_volume_share"),
            ]
        )
        .select(SILVER_TRADES_M1_FEATURE_COLUMNS)
    )


def _fill_empty_trade_prices_from_past_close(pl: Any, frame: Any) -> Any:
    """Forward-fill confirmed-empty price fields from prior observed close only."""

    return (
        frame.sort("timestamp_m1")
        .with_columns(pl.col("close_price").forward_fill().alias("_past_close_price"))
        .with_columns(
            [
                pl.col("open_price").fill_null(pl.col("_past_close_price")),
                pl.col("high_price").fill_null(pl.col("_past_close_price")),
                pl.col("low_price").fill_null(pl.col("_past_close_price")),
                pl.col("close_price").fill_null(pl.col("_past_close_price")),
            ]
        )
        .drop("_past_close_price")
    )


def build_trade_feature_frame(pl: Any, frame: Any, *, symbol: str, empty_minutes_frame: Any | None = None) -> Any:
    """Build 1m trade-flow feature frame from observed ticks and confirmed-empty minutes."""

    feature_frames: list[Any] = []
    if frame.height > 0:
        enriched = frame.with_columns(
            [
                pl.col("trade_time").dt.truncate("1m").alias("timestamp_m1"),
                (pl.col("price") * pl.col("quantity")).alias("notional"),
                (pl.col("side") == "buy").alias("is_buy"),
                (pl.col("side") == "sell").alias("is_sell"),
            ]
        )
        feature_frames.append(
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
            .select(SILVER_TRADES_M1_FEATURE_COLUMNS)
        )

    if empty_minutes_frame is not None and empty_minutes_frame.height > 0:
        empty_feature = _empty_trade_feature_frame(pl, empty_minutes_frame)
        if feature_frames:
            observed_minutes = feature_frames[0].select(["timestamp_m1", "exchange", "symbol", "instrument_type"])
            empty_feature = empty_feature.join(
                observed_minutes,
                on=["timestamp_m1", "exchange", "symbol", "instrument_type"],
                how="anti",
            )
        if empty_feature.height > 0:
            feature_frames.append(empty_feature)

    if not feature_frames:
        return pl.DataFrame(schema={column: pl.Null for column in SILVER_TRADES_M1_FEATURE_COLUMNS})

    feature = pl.concat(feature_frames, how="diagonal_relaxed").sort("timestamp_m1")
    return _fill_empty_trade_prices_from_past_close(pl, feature).select(SILVER_TRADES_M1_FEATURE_COLUMNS)


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
