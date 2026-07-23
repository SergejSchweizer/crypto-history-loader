# Gold Dataset Catalog

This document is the authoritative human-readable catalog for every Gold dataset produced or contracted by `crypto-history-loader`.

The machine-readable source of truth remains `application/dataset_contracts.py`. When a Gold contract, source requirement, output feature, prefix, null policy, or lineage rule changes, update this file in the same change set.

## Global Gold contract

All Gold datasets use a canonical one-minute grid unless stated otherwise.

| Field | Description |
|---|---|
| `timestamp_m1` | UTC minute timestamp used as the temporal join key. |
| `exchange` | Normalized exchange identifier, currently `deribit`. |
| `symbol` | Canonical base symbol such as `BTC`, `ETH`, or `SOL`. |

General rules:

- Historical datasets use the union of available source timestamps; they are not truncated to the intersection of all sources.
- Missing source observations remain null unless a Silver contract explicitly defines an anti-leakage backward as-of fill.
- Event-driven trade counts and volumes are never forward-filled.
- Equivalent upstream symbol variants are normalized and the newest matching artifact is selected.
- Gold retention keeps the latest three versions per `dataset_id/exchange/symbol` lineage.
- Live datasets use only live-origin features and never backfill gaps from historical datasets.
- Manifest lineage records source datasets, source time spans, availability, freshness, and origin repository.

## Available Gold dataset IDs

- `gold.market.perps_trades.m1`
- `gold.market.options_trades.m1`
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.market.iv_rv.m1`
- `gold.market.index_price.m1`
- `gold.market.futures_summary.m1`
- `gold.market.regime_features.m1`
- `gold.market.prediction_targets.m1`
- `gold.market.history_full.m1`
- `gold.live.volatility_features.m1`
- `gold.live.microstructure_features.m1`
- `gold.live.full.m1`
- `gold.market.full.m1`
- `gold.hybrid.full_l2.m1`

## Shared feature families

### OHLCV features

The `spot_` and `perp_` prefixes identify the source market. Trade-flow datasets use `perps_trades_` or `option_trades_`.

| Feature suffix | Description |
|---|---|
| `open_price` | First valid traded price in the minute. |
| `high_price` | Highest valid traded price in the minute. |
| `low_price` | Lowest valid traded price in the minute. |
| `close_price` | Last valid traded price in the minute. |
| `volume` | Executed base-asset or contract quantity in the minute. |
| `quote_volume` | Executed quote-currency notional in the minute. |
| `trade_count` | Number of executions contributing to the minute. |

### Trade-flow features

| Feature suffix | Description |
|---|---|
| `buy_volume` | Quantity executed with buy-side aggressor classification. |
| `sell_volume` | Quantity executed with sell-side aggressor classification. |
| `buy_trade_count` | Number of buy-side classified executions. |
| `sell_trade_count` | Number of sell-side classified executions. |
| `buy_volume_share` | Buy volume divided by total classified buy and sell volume; null when the denominator is unavailable or zero. |

### Funding features

| Feature | Description |
|---|---|
| `funding_rate_last_known` | Most recent funding rate known at the minute, joined backward without future leakage. |
| `funding_observed_at` | Timestamp of the funding observation supplying the current value. |
| `minutes_since_funding` | Age in minutes of the most recent funding observation. |
| `is_funding_observation_minute` | True when the minute contains a native funding observation. |
| `funding_data_available` | True when a valid funding observation is available at or before the minute. |

### Open-interest features

| Feature | Description |
|---|---|
| `open_interest` | Total outstanding perpetual exposure at the selected observation. |
| `open_interest_is_observed` | True when the minute contains a native open-interest observation. |
| `open_interest_is_ffill` | True when the value was carried from an earlier observation. |
| `minutes_since_open_interest_observation` | Age in minutes of the selected open-interest observation. |
| `open_interest_observation_lag_sec` | Difference in seconds between the minute and the source observation. |
| `open_interest_source_timestamp` | Timestamp of the source open-interest observation. |

### Implied-volatility index features

| Feature | Description |
|---|---|
| `iv_open` | First implied-volatility index value in the minute. |
| `iv_high` | Highest implied-volatility index value in the minute. |
| `iv_low` | Lowest implied-volatility index value in the minute. |
| `iv_close` | Last implied-volatility index value in the minute. |
| `iv_range` | Intraminute high-minus-low implied-volatility range. |
| `iv_return_1m` | One-minute change or return of the implied-volatility close, according to the Silver estimator contract. |
| `iv_change_5m` | Five-minute change in the implied-volatility index. |
| `iv_change_15m` | Fifteen-minute change in the implied-volatility index. |
| `iv_change_1h` | One-hour change in the implied-volatility index. |
| `iv_zscore_1d` | Rolling one-day z-score of the implied-volatility level. |
| `iv_zscore_7d` | Rolling seven-day z-score of the implied-volatility level. |
| `iv_percentile_30d` | Rolling 30-day percentile rank of the implied-volatility level. |
| `iv_30d_annualized_pct` | Explicit annualized 30-day implied volatility in percentage points. |
| `iv_source_dataset` | Silver source selected for the implied-volatility value. |
| `iv_source_timestamp` | Timestamp of the selected implied-volatility source observation. |
| `minutes_since_iv_observation` | Age in minutes of the selected implied-volatility observation. |
| `iv_data_available` | True when a valid implied-volatility observation is available. |

### Realized-volatility features

The canonical `rv_*` features use one source for the complete symbol lineage: perpetual returns when available, otherwise spot returns. `spot_*` and `perps_*` variants preserve source-specific calculations.

| Feature | Description |
|---|---|
| `canonical_rv_source` | Market source selected for canonical realized volatility. |
| `canonical_rv_source_available` | True when the selected source is available for the minute. |
| `rv_5m`, `rv_15m`, `rv_1h`, `rv_4h`, `rv_1d`, `rv_30d` | Non-annualized square-root sum of squared log returns over the named horizon. |
| `rv_5m_annualized_pct`, `rv_15m_annualized_pct`, `rv_1h_annualized_pct`, `rv_4h_annualized_pct`, `rv_1d_annualized_pct`, `rv_30d_annualized_pct` | Realized volatility annualized on a 365-day basis and expressed in percentage points. |
| `spot_log_return` | One-minute spot log return. |
| `spot_rv_5m`, `spot_rv_15m`, `spot_rv_1h`, `spot_rv_4h`, `spot_rv_1d`, `spot_rv_30d` | Spot-only realized-volatility estimates over the named horizon. |
| `spot_rv_5m_annualized_pct`, `spot_rv_15m_annualized_pct`, `spot_rv_1h_annualized_pct`, `spot_rv_4h_annualized_pct`, `spot_rv_1d_annualized_pct`, `spot_rv_30d_annualized_pct` | Annualized percentage-point versions of the spot realized-volatility estimates. |
| `perps_log_return` | One-minute perpetual log return. |
| `perps_rv_5m`, `perps_rv_15m`, `perps_rv_1h`, `perps_rv_4h`, `perps_rv_1d`, `perps_rv_30d` | Perpetual-only realized-volatility estimates over the named horizon. |
| `perps_rv_5m_annualized_pct`, `perps_rv_15m_annualized_pct`, `perps_rv_1h_annualized_pct`, `perps_rv_4h_annualized_pct`, `perps_rv_1d_annualized_pct`, `perps_rv_30d_annualized_pct` | Annualized percentage-point versions of the perpetual realized-volatility estimates. |
| `parkinson_rv_1h` | One-hour high-low Parkinson realized-volatility estimator. |
| `jump_proxy` | Difference between close-to-close and range-based variation used as a jump/stress proxy. |
| `spot_available` | True when spot inputs are available. |
| `perps_available` | True when perpetual inputs are available. |
| `spot_perps_basis_available` | True when both spot and perpetual prices required for basis calculations are available. |

### IV/RV relationship features

| Feature | Description |
|---|---|
| `iv_minus_rv_1h` | Deprecated mixed-unit implied-minus-realized volatility spread at the one-hour horizon. |
| `iv_minus_rv_1d` | Deprecated mixed-unit implied-minus-realized volatility spread at the one-day horizon. |
| `iv_rv_ratio_1h` | Deprecated mixed-unit implied-to-realized volatility ratio at the one-hour horizon. |
| `iv_rv_ratio_1d` | Deprecated mixed-unit implied-to-realized volatility ratio at the one-day horizon. |
| `iv_rv_spread_30d_pct` | Unit-safe 30-day implied volatility minus annualized 30-day realized volatility, in percentage points. |
| `iv_rv_ratio_30d` | Unit-safe ratio of 30-day implied volatility to annualized 30-day realized volatility. |
| `iv_rv_zscore_1d` | Rolling one-day z-score of the IV/RV relationship. |
| `iv_rv_percentile_30d` | Rolling 30-day percentile rank of the IV/RV relationship. |
| `minutes_since_rv_observation` | Age in minutes of the realized-volatility source state. |
| `iv_available` | True when implied-volatility input is available. |
| `rv_available` | True when realized-volatility input is available. |

### Index-price features

| Feature | Description |
|---|---|
| `index_price` | External reference/index price selected for the minute. |
| `index_price_is_observed` | True when the minute contains a native index observation. |
| `index_price_source_timestamp` | Timestamp of the selected index-price observation. |
| `minutes_since_index_price_observation` | Age in minutes of the selected index-price observation. |

### Futures-summary features

| Feature | Description |
|---|---|
| `instrument_type` | Normalized futures instrument classification. |
| `mark_price` | Exchange mark price used for valuation and liquidation controls. |
| `index_price` | External reference price associated with the summary observation. |
| `mark_index_spread` | Mark price minus index price. |
| `mark_index_ratio` | Mark price divided by index price. |
| `open_interest` | Outstanding futures exposure reported by the summary source. |
| `volume` | Reported trading volume. |
| `turnover` | Reported quote-currency notional turnover. |
| `funding_rate` | Funding state reported with the futures summary. |
| `summary_is_observed` | True when the minute contains a native summary observation. |
| `minutes_since_summary_observation` | Age in minutes of the selected summary observation. |

### Options-surface features

| Feature | Description |
|---|---|
| `atm_iv` | Implied volatility of the option nearest at-the-money. |
| `short_dated_iv` | Representative implied volatility for the short-dated expiry bucket. |
| `skew` | Cross-strike implied-volatility slope/asymmetry measure. |
| `term_structure` | Difference or slope between short- and longer-dated implied volatility. |
| `put_call_iv_spread` | Put implied volatility minus call implied volatility for comparable contracts. |
| `contract_count` | Number of contracts contributing to the surface minute. |
| `fresh_quote_count` | Number of contracts with quotes inside the freshness threshold. |
| `stale_quote_count` | Number of contracts outside the freshness threshold. |
| `max_quote_age_seconds` | Maximum quote age among contracts contributing to the minute. |
| `quote_coverage_ratio` | Fresh usable contracts divided by the eligible contract universe. |

### L2 microstructure features

Perpetual and option L2 columns use the `perps_l2_` and `options_l2_` prefixes in combined Gold datasets.

| Feature suffix | Description |
|---|---|
| `instrument_type` | Instrument class represented by the book. |
| `instrument_name` | Exchange instrument identifier. |
| `underlying` | Canonical underlying asset. |
| `expiry` | Option expiry when applicable. |
| `strike` | Option strike when applicable. |
| `option_type` | Call/put classification when applicable. |
| `best_bid_price` | Highest available bid price. |
| `best_ask_price` | Lowest available ask price. |
| `mid_price` | Arithmetic midpoint of best bid and best ask. |
| `spread` | Best ask minus best bid. |
| `top_bid_size` | Quantity available at the best bid. |
| `top_ask_size` | Quantity available at the best ask. |
| `top_of_book_imbalance` | Normalized best-level bid-versus-ask size imbalance. |
| `bid_depth_10bps` | Aggregate bid depth within 10 basis points of the reference price. |
| `ask_depth_10bps` | Aggregate ask depth within 10 basis points of the reference price. |
| `bid_depth_50bps` | Aggregate bid depth within 50 basis points of the reference price. |
| `ask_depth_50bps` | Aggregate ask depth within 50 basis points of the reference price. |
| `quote_available` | True when a usable two-sided quote exists. |
| `quote_age_seconds` | Age of the selected L2 observation in seconds. |
| `stale_quote` | True when quote age exceeds the configured freshness limit. |
| `minutes_since_l2_observation` | Age of the selected L2 observation in minutes. |

### Lineage features used by live Gold datasets

| Feature pattern | Description |
|---|---|
| `<source>_as_of` | Source timestamp used for the feature family at the Gold minute. |
| `<source>_live_snapshot_derived` | True when the feature originates from a live snapshot pipeline. |
| `origin_repository` | Repository that owns the source lineage; live Gold uses `crypto-live-loader`. |

## Dataset specifications

### `gold.market.perps_trades.m1`

Purpose: historical perpetual execution-flow modeling.

Required source: `perps_trades_1m_feature`.

Features: global keys plus `perps_trades_open_price`, `perps_trades_high_price`, `perps_trades_low_price`, `perps_trades_close_price`, `perps_trades_volume`, `perps_trades_quote_volume`, `perps_trades_trade_count`, `perps_trades_buy_volume`, `perps_trades_sell_volume`, `perps_trades_buy_trade_count`, `perps_trades_sell_trade_count`, and `perps_trades_buy_volume_share`. Each feature has the corresponding OHLCV or trade-flow meaning defined above.

### `gold.market.options_trades.m1`

Purpose: historical option execution-flow modeling.

Required source: `options_trades_1m_feature`.

Features: global keys plus `option_trades_open_price`, `option_trades_high_price`, `option_trades_low_price`, `option_trades_close_price`, `option_trades_volume`, `option_trades_quote_volume`, `option_trades_trade_count`, `option_trades_buy_volume`, `option_trades_sell_volume`, `option_trades_buy_trade_count`, `option_trades_sell_trade_count`, and `option_trades_buy_volume_share`. Each feature has the corresponding OHLCV or trade-flow meaning defined above.

### `gold.market.core.m1`

Purpose: compact historical spot/perpetual market-state dataset for forecasting, basis research, and regime detection.

Required sources: `spot_ohlcv`, `perps_ohlcv`.

Features: global keys; `spot_open_price`, `spot_high_price`, `spot_low_price`, `spot_close_price`, `spot_volume`; `perp_open_price`, `perp_high_price`, `perp_low_price`, `perp_close_price`, `perp_volume`. The prefixes identify the market and the suffixes use the OHLCV definitions above.

### `gold.market.core_funding.m1`

Purpose: core spot/perpetual state augmented with carry and crowding context.

Required sources: `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature`.

Features: every `gold.market.core.m1` feature plus `funding_rate_last_known`, `funding_observed_at`, `minutes_since_funding`, `is_funding_observation_minute`, and `funding_data_available`.

### `gold.market.iv_rv.m1`

Purpose: historical volatility-risk-premium and volatility-regime research.

Required sources: `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature`, `open_interest_1m_feature`, `realized_volatility_1m_feature`, `iv_rv_1m_feature`.

Optional source: `historical_volatility_observed`.

Features: global keys; spot/perpetual OHLCV state; all funding and open-interest features; all canonical, spot, and perpetual realized-volatility features; all IV/RV relationship features; and `historical_volatility` plus its source timestamp when the optional source is available. `historical_volatility` is the exchange-provided historical-volatility observation, not the pipeline's return-derived RV estimator.

### `gold.market.index_price.m1`

Purpose: isolated index/reference-price dataset for fair-value and dislocation analysis.

Required source: `index_price_1m_feature`.

Features: global keys plus `index_price`, `index_price_is_observed`, `index_price_source_timestamp`, and `minutes_since_index_price_observation`.

### `gold.market.futures_summary.m1`

Purpose: isolated futures summary state for mark/index, activity, leverage, and funding analysis.

Required source: `futures_summary_1m_feature`.

Features: global keys plus `instrument_type`, `mark_price`, `index_price`, `mark_index_spread`, `mark_index_ratio`, `open_interest`, `volume`, `turnover`, `funding_rate`, `summary_is_observed`, and `minutes_since_summary_observation`.

### `gold.market.regime_features.m1`

Purpose: stable-schema research table for volatility, leverage, carry, liquidity, and derivatives-regime models.

Required sources: `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature`, `open_interest_1m_feature`, `realized_volatility_1m_feature`, `iv_rv_1m_feature`.

Optional sources: `perps_l2_1m_feature`, `options_l2_1m_feature`, `options_surface_1m_feature`, `index_price_1m_feature`, `futures_summary_1m_feature`, `historical_volatility_observed`.

Features: global keys; spot/perpetual OHLCV state; all funding, open-interest, realized-volatility, and IV/RV features; prefixed perpetual/options L2 features; all options-surface, index-price, and futures-summary features; optional `historical_volatility`; and source availability/freshness lineage. Optional-source columns remain typed nulls when unavailable, so schema does not change between builds.

### `gold.market.prediction_targets.m1`

Purpose: supervised-learning label table separated from predictor features to prevent accidental target leakage.

Required sources: `perps_ohlcv`, `funding_1m_feature`, `realized_volatility_1m_feature`, `iv_rv_1m_feature`.

Features: global keys plus forward-looking target and label columns produced by the Gold target builder. Target columns represent future perpetual return, direction, volatility, funding, or IV/RV outcomes at their encoded horizons. Every target is computed strictly from observations after `timestamp_m1`; it must never be joined into an inference feature matrix without an explicit training-only boundary. The exact target column set is code-defined and must be extended here whenever the builder adds or removes a target.

### `gold.market.history_full.m1`

Purpose: canonical historical training dataset produced solely from datasets fetched by `crypto-history-loader`.

Required sources: `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature`, `open_interest_1m_feature`, `perps_trades_1m_feature`, `options_trades_1m_feature`.

Features: global keys; complete prefixed spot and perpetual OHLCV features; all funding and open-interest features; all prefixed perpetual and option trade-flow features; and Silver features calculated only from these historical sources. It excludes volatility-index, IV/RV, L2, index-price, futures-summary, options-surface, live snapshot, strategy, target, and label columns. Source gaps remain null on the continuous symbol-minute grid.

### `gold.live.volatility_features.m1`

Purpose: live implied-volatility state for inference.

Required source: `volatility_index_1m_feature`.

Features: global keys; all implied-volatility index features; `volatility_index_as_of`; and `volatility_index_live_snapshot_derived`. Missing live minutes remain null and are not backfilled from historical data.

### `gold.live.microstructure_features.m1`

Purpose: live perpetual and option order-book state for inference and execution research.

Required sources: `perps_l2_1m_feature`, `options_l2_1m_feature`.

Features: global keys; every L2 feature with `perps_l2_` prefix; aggregated option-book coverage/depth features with `options_l2_` prefix; `perps_l2_as_of`, `options_l2_as_of`; and corresponding `*_live_snapshot_derived` flags. Missing live source minutes remain null.

### `gold.live.full.m1`

Purpose: canonical live inference dataset.

Required sources: `volatility_index_1m_feature`, `iv_rv_1m_feature`, `perps_l2_1m_feature`, `options_l2_1m_feature`.

Optional sources: `index_price_1m_feature`, `futures_summary_1m_feature`, `options_surface_1m_feature`.

Features: global keys; all implied-volatility, IV/RV, prefixed L2, optional index-price, optional futures-summary, and optional options-surface features; per-source `*_as_of` timestamps; per-source `*_live_snapshot_derived` flags; and origin lineage. Optional-source columns remain null when unavailable. Historical values never fill live gaps.

### `gold.market.full.m1`

Purpose: broad compatibility dataset combining historical market, leverage, flow, and configured volatility-index inputs.

Required sources: `spot_ohlcv`, `perps_ohlcv`, `open_interest_1m_feature`, `funding_1m_feature`, `perps_trades_1m_feature`, `options_trades_1m_feature`, `volatility_index_data_observed`.

Features: global keys; complete prefixed spot/perpetual OHLCV; all open-interest and funding features; all prefixed perpetual/option trade-flow features; and observed volatility-index fields. Existing physical artifacts must not be considered IV/RV-ready until they are rebuilt with the relevant Silver features and manifests.

### `gold.hybrid.full_l2.m1`

Purpose: compatibility dataset extending `gold.market.full.m1` with order-book state.

Required sources: the complete `gold.market.full.m1` source set plus L2 inputs selected by the Gold builder.

Features: every `gold.market.full.m1` feature plus prefixed perpetual and option L2 features, quote freshness/availability fields, and L2 lineage. It uses the same newest-artifact selection policy as other Gold joins.

## Physical status

As of the repository inventory snapshot dated 2026-07-22 CEST, physical historical artifacts exist for:

- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.market.perps_trades.m1`
- `gold.market.options_trades.m1`
- `gold.market.full.m1`
- `gold.market.history_full.m1`

The remaining IDs are typed contracts or compatibility targets and may not yet be physically materialized. Use `dataset-inventory` to obtain the current physical status rather than relying on this dated snapshot.

## Build examples

Canonical historical dataset:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --maxprocesses 4 \
  --dataset-id gold.market.history_full.m1
```

Canonical live dataset:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.live.full.m1 \
  --symbols BTC ETH
```

Current physical/contract inventory:

```bash
uv run python main.py dataset-inventory \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --format markdown \
  --output docs/dataset_inventory.md \
  --no-json-output
```
