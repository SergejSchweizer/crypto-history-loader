# Gold Datasets

This file is the canonical human-readable catalog for every Gold dataset supported by
`crypto-history-loader`. It owns Gold dataset purpose, source contracts, alignment and null
semantics, exact feature membership, and feature meaning.

The executable sources of truth are:

- `application/dataset_contracts.py` for dataset IDs and required/optional source datasets;
- `application/services/gold_frames.py` for source normalization, feature construction, strategy
  features, and prediction targets;
- `application/services/gold_service.py` for final schema selection, versioning, manifests,
  retention, and writes;
- `docs/dataset_inventory.md` for the generated physical Lake snapshot.

`DATASETS.md` documents the contract. `docs/dataset_inventory.md` documents what was physically
materialized when that report was generated. A contracted dataset can therefore be absent from the
current Lake until it is built.

## Global contract

- Default grain: one row per `exchange`, normalized base `symbol`, and UTC minute.
- Primary keys: `timestamp_m1`, `exchange`, `symbol`.
- Required sources are aligned on a union minute grid; the builder does not silently reduce output
  to the timestamp intersection.
- Missing source values remain null unless an upstream Silver contract explicitly exposes a
  last-known or forward-filled state together with observation/fill metadata.
- Trade counts, volumes, and trade-price aggregates are never forward-filled because that would
  manufacture executions.
- Optional source families have stable typed nullable columns when the optional artifact is absent.
- `gold.market.regime_features.m1` contains current/trailing features only. Forward-looking values
  exist only in `gold.market.prediction_targets.m1`.
- `gold.market.history_full.m5`, `gold.market.history_full.m30`, and `gold.market.history_full.h1`
  are deterministic bucket-start resamples of `gold.market.history_full.m1`.
- Gold dataset IDs are standalone contracts. Canonical datasets use the finest trusted grain for
  their family, and any independently materialized extension gets its own dataset ID with an
  explicit grain suffix.
- Gold manifests record the dataset version, feature-set hash, source-data hash, Git commit, source
  lineage, coverage, and build metadata.
- The latest three versions are retained per dataset/exchange/symbol lineage.
- `gold.live.*` manifests use `crypto-live-loader` as origin; all other contracted IDs use
  `crypto-history-loader`.

## Contract inventory

| Dataset ID | Purpose | Required sources | Optional sources | Physical status in 2026-07-21 inventory |
|---|---|---|---|---|
| `gold.market.perps_trades.m1` | Per-minute perpetual trade-flow aggregate. | `perps_trades_1m_feature` | None | Materialized |
| `gold.market.options_trades.m1` | Per-minute option trade-flow aggregate. | `options_trades_1m_feature` | None | Materialized |
| `gold.market.core.m1` | Minimal historical spot/perpetual price and volume state. | `spot_ohlcv`, `perps_ohlcv` | None | Materialized |
| `gold.market.core_funding.m1` | Core market state enriched with funding carry state. | `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature` | None | Materialized |
| `gold.market.iv_rv.m1` | Historical state for unit-safe implied-versus-realized volatility research. | `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature`, `open_interest_1m_feature`, `realized_volatility_1m_feature`, `iv_rv_1m_feature` | `historical_volatility_observed` | Contracted, not materialized |
| `gold.market.index_price.m1` | Narrow index-price/fair-value state. | `index_price_1m_feature` | None | Contracted, not materialized |
| `gold.market.futures_summary.m1` | Narrow futures mark/index, positioning, turnover, and funding state. | `futures_summary_1m_feature` | None | Contracted, not materialized |
| `gold.market.regime_features.m1` | Trailing-only momentum, trend, reversion, volatility, liquidity, and option-surface research table. | `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature`, `open_interest_1m_feature`, `realized_volatility_1m_feature`, `iv_rv_1m_feature` | Perpetual L2, option L2, option surface, index price, futures summary, historical volatility | Contracted, not materialized |
| `gold.market.prediction_targets.m1` | Dedicated forward-looking targets and regime-shift labels. | `perps_ohlcv`, `funding_1m_feature`, `realized_volatility_1m_feature`, `iv_rv_1m_feature` | None | Contracted, not materialized |
| `gold.market.history_full.m1` | Canonical historical table restricted to history-derived market features. | `spot_ohlcv`, `perps_ohlcv`, `funding_1m_feature`, `open_interest_1m_feature`, `perps_trades_1m_feature`, `options_trades_1m_feature` | None | Materialized |
| `gold.market.history_full.m5` | Five-minute resample of the canonical historical market table. | Derived from `gold.market.history_full.m1` | None | Contracted, not materialized |
| `gold.market.history_full.m30` | Thirty-minute resample of the canonical historical market table. | Derived from `gold.market.history_full.m1` | None | Contracted, not materialized |
| `gold.market.history_full.h1` | One-hour resample of the canonical historical market table. | Derived from `gold.market.history_full.m1` | None | Contracted, not materialized |
| `gold.live.volatility_features.m1` | Live-origin implied-volatility features with freshness and lineage. | `volatility_index_1m_feature` | None | Contracted, not materialized |
| `gold.live.microstructure_features.m1` | Live-origin perpetual and option order-book state. | `perps_l2_1m_feature`, `options_l2_1m_feature` | None | Contracted, not materialized |
| `gold.live.full.m1` | Canonical live inference table combining volatility, IV/RV, and L2 state. | `volatility_index_1m_feature`, `iv_rv_1m_feature`, `perps_l2_1m_feature`, `options_l2_1m_feature` | Index price, futures summary, option surface | Materialized |
| `gold.market.full.m1` | Broad historical price, carry, positioning, trade-flow, and observed-volatility table. | `spot_ohlcv`, `perps_ohlcv`, `open_interest_1m_feature`, `funding_1m_feature`, `perps_trades_1m_feature`, `options_trades_1m_feature`, `volatility_index_data_observed` | None | Materialized |
| `gold.hybrid.full_l2.m1` | Historical full-market table augmented with the newest compatible external L2 Gold artifact. | Same Silver sources as `gold.market.full.m1` | External `gold.l2.micro.m1` artifact | Contracted, not materialized |

The status column is a dated snapshot, not a runtime guarantee. Regenerate
`docs/dataset_inventory.md` after Lake rebuilds.

## Dataset contracts and exact feature membership

Feature groups below are closed, exact lists. Every feature in a referenced group is part of the
dataset. Definitions for every feature are in [Feature dictionary](#feature-dictionary).

### gold.market.perps_trades.m1

- Origin: `crypto-history-loader`.
- Alignment: observed trade minutes on the required-source union grid; no forward fill.
- Feature groups: **Keys**, **Perpetual trade aggregates**.

### gold.market.options_trades.m1

- Origin: `crypto-history-loader`.
- Alignment: observed trade minutes on the required-source union grid; no forward fill.
- Feature groups: **Keys**, **Option trade aggregates**.

### gold.market.core.m1

- Origin: `crypto-history-loader`.
- Alignment: spot and perpetual sources on a union minute grid; source gaps remain null.
- Feature groups: **Keys**, **Spot OHLCV**, **Perpetual OHLCV**.

### gold.market.core_funding.m1

- Origin: `crypto-history-loader`.
- Alignment: core union grid plus the explicit Silver last-known funding state.
- Feature groups: **Keys**, **Spot OHLCV**, **Perpetual OHLCV**, **Funding state**.

### gold.market.iv_rv.m1

- Origin: `crypto-history-loader`.
- Alignment: required sources on a union minute grid. External historical volatility is nullable
  and does not expand the required-source grid.
- Required feature groups: **Keys**, **Spot OHLCV**, **Perpetual OHLCV**, **Funding state**,
  **Open-interest state**, **Realized volatility**, **IV/RV state**.
- Optional nullable group: **Historical-volatility reference**.

### gold.market.index_price.m1

- Origin: `crypto-history-loader`.
- Alignment: one-minute index feature grid with observation/age metadata.
- Feature groups: **Keys**, **Index-price state**.

### gold.market.futures_summary.m1

- Origin: `crypto-history-loader`.
- Alignment: one-minute futures-summary grid with observation/age metadata.
- Feature groups: **Keys**, **Futures-summary state**.

### gold.market.regime_features.m1

- Origin: `crypto-history-loader`.
- Alignment: required sources define the minute grid. Missing optional artifacts produce typed
  null columns and never remove required rows. Strategy calculations use present and past data only.
- Required feature groups: **Keys**, **Spot OHLCV**, **Perpetual OHLCV**, **Funding state**,
  **Open-interest state**, **Realized volatility**, **IV/RV state**, **Strategy state**.
- Optional nullable groups: **Perpetual L2 state**, **Option L2 state**, **Option-surface state**,
  **Index-price state**, **Futures-summary state**, **Historical-volatility reference**.

### gold.market.prediction_targets.m1

- Origin: `crypto-history-loader`.
- Alignment: output contains only keys plus target/label columns. A target is null unless its full
  future horizon exists.
- Feature groups: **Keys**, **Prediction targets**.

### gold.market.history_full.m1

- Origin: `crypto-history-loader`.
- Alignment: union of historical source minute timestamps; source gaps remain null and event-driven
  trade fields are never forward-filled.
- Feature groups: **Keys**, **Spot OHLCV**, **Perpetual OHLCV**, **Funding state**,
  **Open-interest state**, **Perpetual trade aggregates**, **Option trade aggregates**.
- Deliberately excluded: IV/RV, volatility-index, L2, live-snapshot, strategy, target, and label
  columns.

### gold.market.history_full.m5 / gold.market.history_full.m30 / gold.market.history_full.h1

- Origin: `crypto-history-loader`.
- Alignment: deterministic bucket-start resample of `gold.market.history_full.m1` using 5m, 30m, or
  1h buckets.
- Feature groups: same closed schema as `gold.market.history_full.m1`.
- Aggregation rule: OHLC fields use first/high/low/last semantics, additive fields are summed,
  minute-end state fields keep the last non-null observation in each bucket, and trade-volume shares
  are recomputed from the resampled bucket totals.

### gold.live.volatility_features.m1

- Origin: `crypto-live-loader`.
- Alignment: observed live-origin IV feature minutes; no artificial historical filling.
- Feature groups: **Keys**, **Live implied-volatility state**.

### gold.live.microstructure_features.m1

- Origin: `crypto-live-loader`.
- Alignment: observed live-origin L2 minutes. Option contracts are aggregated to one row per
  exchange/symbol/minute.
- Feature groups: **Keys**, **Perpetual L2 state**, **Option L2 state**.

### gold.live.full.m1

- Origin: `crypto-live-loader`.
- Alignment: observed live-origin required sources. Optional families remain typed nullable.
- Required feature groups: **Keys**, **Live implied-volatility state**, **IV/RV state**,
  **Perpetual L2 state**, **Option L2 state**.
- Optional nullable groups: **Index-price state**, **Futures-summary state**,
  **Option-surface state**.

### gold.market.full.m1

- Origin: `crypto-history-loader`.
- Alignment: one-minute union grid; this contract does not add strategy, prediction-target, or L2
  feature engineering.
- Feature groups: **Keys**, **Spot OHLCV**, **Perpetual OHLCV**, **Open-interest state**,
  **Funding state**, **Perpetual trade aggregates**, **Option trade aggregates**,
  **Observed volatility index**.

### gold.hybrid.full_l2.m1

- Origin: `crypto-history-loader`.
- Alignment: base schema equals `gold.market.full.m1`. Every non-key column in the selected external
  L2 artifact is copied with an `l2_` prefix.
- Fixed feature groups: **Keys**, **Spot OHLCV**, **Perpetual OHLCV**, **Open-interest state**,
  **Funding state**, **Perpetual trade aggregates**, **Option trade aggregates**,
  **Observed volatility index**.
- Dynamic extension: **External L2 passthrough**. Its closed schema is the selected source artifact
  and is recorded in the manifest; it is intentionally not hard-coded in the typed contract.

## Feature dictionary

### Keys

| Feature | Description |
|---|---|
| `timestamp_m1` | UTC timestamp truncated to the closed one-minute bucket; primary time key. |
| `exchange` | Normalized exchange identifier, currently `deribit`. |
| `symbol` | Normalized base-asset symbol used by Gold joins, for example `BTC`, `ETH`, or `SOL`. |

### Spot OHLCV

| Feature | Description |
|---|---|
| `spot_ohlcv_open_price` | First spot trade price in the minute. |
| `spot_ohlcv_high_price` | Highest spot trade price in the minute. |
| `spot_ohlcv_low_price` | Lowest spot trade price in the minute. |
| `spot_ohlcv_close_price` | Last spot trade price in the minute. |
| `spot_ohlcv_volume` | Spot base-asset volume in the minute. |
| `spot_ohlcv_quote_volume` | Spot quote-currency notional; nullable when the source candle does not provide it. |
| `spot_ohlcv_trade_count` | Source-reported spot trade count; nullable when unavailable. |

### Perpetual OHLCV

| Feature | Description |
|---|---|
| `perp_open_price` | First perpetual trade price in the minute. |
| `perp_high_price` | Highest perpetual trade price in the minute. |
| `perp_low_price` | Lowest perpetual trade price in the minute. |
| `perp_close_price` | Last perpetual trade price in the minute. |
| `perp_volume` | Perpetual base-asset volume in the minute. |
| `perp_quote_volume` | Perpetual quote-currency notional; nullable when unavailable. |
| `perp_trade_count` | Source-reported perpetual trade count; nullable when unavailable. |

### Funding state

| Feature | Description |
|---|---|
| `funding_rate_last_known` | Most recent funding rate known at the minute. |
| `funding_observed_at` | UTC timestamp of the observation supplying the current funding value. |
| `minutes_since_funding` | Whole minutes since `funding_observed_at`. |
| `is_funding_observation_minute` | True when a native funding observation occurs in this minute. |
| `funding_data_available` | True when a funding value is available for the minute. |

### Open-interest state

| Feature | Description |
|---|---|
| `open_interest_open_interest` | Open-interest value aligned to the minute; repeated naming is the stable Gold compatibility column. |
| `open_interest_is_observed` | True when open interest was directly observed in this minute. |
| `open_interest_is_ffill` | True when open interest was carried from an earlier observation. |
| `minutes_since_open_interest_observation` | Whole minutes since the last direct open-interest observation. |
| `open_interest_observation_lag_sec` | Source observation/ingest lag in seconds. |
| `open_interest_source_timestamp` | UTC timestamp of the source observation supplying the value. |

### Perpetual trade aggregates

| Feature | Description |
|---|---|
| `perps_trades_open_price` | First perpetual execution price in the minute. |
| `perps_trades_high_price` | Highest perpetual execution price in the minute. |
| `perps_trades_low_price` | Lowest perpetual execution price in the minute. |
| `perps_trades_close_price` | Last perpetual execution price in the minute. |
| `perps_trades_volume` | Total perpetual traded quantity in the minute. |
| `perps_trades_quote_volume` | Total perpetual quote-currency notional in the minute. |
| `perps_trades_trade_count` | Number of perpetual executions in the minute. |
| `perps_trades_buy_volume` | Quantity classified as buyer-initiated perpetual flow. |
| `perps_trades_sell_volume` | Quantity classified as seller-initiated perpetual flow. |
| `perps_trades_buy_trade_count` | Count of buyer-initiated perpetual executions. |
| `perps_trades_sell_trade_count` | Count of seller-initiated perpetual executions. |
| `perps_trades_buy_volume_share` | Buyer volume divided by total classified perpetual volume; null for an unavailable/zero denominator. |

### Option trade aggregates

| Feature | Description |
|---|---|
| `options_trades_open_price` | First option execution price in the minute. |
| `options_trades_high_price` | Highest option execution price in the minute. |
| `options_trades_low_price` | Lowest option execution price in the minute. |
| `options_trades_close_price` | Last option execution price in the minute. |
| `options_trades_volume` | Total option traded quantity in the minute. |
| `options_trades_quote_volume` | Total option quote-currency notional in the minute. |
| `options_trades_trade_count` | Number of option executions in the minute. |
| `options_trades_buy_volume` | Quantity classified as buyer-initiated option flow. |
| `options_trades_sell_volume` | Quantity classified as seller-initiated option flow. |
| `options_trades_buy_trade_count` | Count of buyer-initiated option executions. |
| `options_trades_sell_trade_count` | Count of seller-initiated option executions. |
| `options_trades_buy_volume_share` | Buyer volume divided by total classified option volume; null for an unavailable/zero denominator. |

### Observed volatility index

| Feature | Description |
|---|---|
| `volatility_index_value` | Canonical observed volatility-index value, equal to the source close when OHLC is available. |
| `volatility_index_open` | First observed volatility-index value in the source interval. |
| `volatility_index_high` | Highest observed volatility-index value in the source interval. |
| `volatility_index_low` | Lowest observed volatility-index value in the source interval. |
| `volatility_index_close` | Last observed volatility-index value in the source interval. |

### Live implied-volatility state

Deribit DVOL-style values are annualized 30-day implied-volatility percentage points.

| Feature | Description |
|---|---|
| `iv_open` | Opening IV index value for the minute. |
| `iv_high` | Highest IV index value for the minute. |
| `iv_low` | Lowest IV index value for the minute. |
| `iv_close` | Closing annualized 30-day IV index value in percentage points. |
| `iv_range` | `iv_high - iv_low`. |
| `iv_return_1m` | One-minute log return of positive IV closes. |
| `iv_change_5m` | Difference between the current IV close and the close five minutes earlier. |
| `iv_change_15m` | Difference between the current IV close and the close fifteen minutes earlier. |
| `iv_change_1h` | Difference between the current IV close and the close one hour earlier. |
| `iv_zscore_1d` | Trailing one-day z-score of IV close. |
| `iv_zscore_7d` | Trailing seven-day z-score of IV close. |
| `iv_percentile_30d` | Closed trailing 30-day empirical percentile of IV close. |
| `iv_30d_annualized_pct` | Explicit unit/horizon alias of `iv_close` for safe IV/RV comparison. |
| `iv_source_dataset` | Silver dataset selected as the IV source. |
| `iv_source_timestamp` | UTC timestamp of the source IV observation. |
| `minutes_since_iv_observation` | Whole minutes since the source IV observation. |
| `iv_data_available` | True when IV data is available at the minute. |
| `as_of` | Live-source freshness timestamp, equal to `iv_source_timestamp`. |
| `live_snapshot_derived` | True when the row is derived from the live snapshot path. |

### Realized volatility

Raw `rv_*` values are non-annualized `sqrt(sum(log_return^2))`. Their
`*_annualized_pct` siblings use a 365-day annualization basis and percentage-point scale.

| Feature | Description |
|---|---|
| `canonical_rv_source` | Stable RV source identity: perpetuals when the symbol has a perpetual source, otherwise spot; never switched row by row. |
| `canonical_rv_source_available` | True when the selected canonical source has a usable close at the minute. |
| `rv_5m` | Canonical non-annualized realized volatility over 5 minutes. |
| `rv_5m_annualized_pct` | Annualized percentage-point form of `rv_5m`. |
| `rv_15m` | Canonical non-annualized realized volatility over 15 minutes. |
| `rv_15m_annualized_pct` | Annualized percentage-point form of `rv_15m`. |
| `rv_1h` | Canonical non-annualized realized volatility over 1 hour. |
| `rv_1h_annualized_pct` | Annualized percentage-point form of `rv_1h`. |
| `rv_4h` | Canonical non-annualized realized volatility over 4 hours. |
| `rv_4h_annualized_pct` | Annualized percentage-point form of `rv_4h`. |
| `rv_1d` | Canonical non-annualized realized volatility over 1 day. |
| `rv_1d_annualized_pct` | Annualized percentage-point form of `rv_1d`. |
| `rv_30d` | Canonical non-annualized realized volatility over 30 days. |
| `rv_30d_annualized_pct` | Annualized percentage-point form of `rv_30d`. |
| `spot_log_return` | One-minute log return computed only from positive spot closes. |
| `spot_rv_5m` | Spot-only non-annualized realized volatility over 5 minutes. |
| `spot_rv_5m_annualized_pct` | Annualized percentage-point form of `spot_rv_5m`. |
| `spot_rv_15m` | Spot-only non-annualized realized volatility over 15 minutes. |
| `spot_rv_15m_annualized_pct` | Annualized percentage-point form of `spot_rv_15m`. |
| `spot_rv_1h` | Spot-only non-annualized realized volatility over 1 hour. |
| `spot_rv_1h_annualized_pct` | Annualized percentage-point form of `spot_rv_1h`. |
| `spot_rv_4h` | Spot-only non-annualized realized volatility over 4 hours. |
| `spot_rv_4h_annualized_pct` | Annualized percentage-point form of `spot_rv_4h`. |
| `spot_rv_1d` | Spot-only non-annualized realized volatility over 1 day. |
| `spot_rv_1d_annualized_pct` | Annualized percentage-point form of `spot_rv_1d`. |
| `spot_rv_30d` | Spot-only non-annualized realized volatility over 30 days. |
| `spot_rv_30d_annualized_pct` | Annualized percentage-point form of `spot_rv_30d`. |
| `perps_log_return` | One-minute log return computed only from positive perpetual closes. |
| `perps_rv_5m` | Perpetual-only non-annualized realized volatility over 5 minutes. |
| `perps_rv_5m_annualized_pct` | Annualized percentage-point form of `perps_rv_5m`. |
| `perps_rv_15m` | Perpetual-only non-annualized realized volatility over 15 minutes. |
| `perps_rv_15m_annualized_pct` | Annualized percentage-point form of `perps_rv_15m`. |
| `perps_rv_1h` | Perpetual-only non-annualized realized volatility over 1 hour. |
| `perps_rv_1h_annualized_pct` | Annualized percentage-point form of `perps_rv_1h`. |
| `perps_rv_4h` | Perpetual-only non-annualized realized volatility over 4 hours. |
| `perps_rv_4h_annualized_pct` | Annualized percentage-point form of `perps_rv_4h`. |
| `perps_rv_1d` | Perpetual-only non-annualized realized volatility over 1 day. |
| `perps_rv_1d_annualized_pct` | Annualized percentage-point form of `perps_rv_1d`. |
| `perps_rv_30d` | Perpetual-only non-annualized realized volatility over 30 days. |
| `perps_rv_30d_annualized_pct` | Annualized percentage-point form of `perps_rv_30d`. |
| `parkinson_rv_1h` | One-hour Parkinson range-volatility estimator using canonical-source OHLC. |
| `jump_proxy` | Absolute rolling z-score of canonical log return; a dimensionless jump/stress proxy. |
| `spot_available` | True when a usable spot observation is available. |
| `perps_available` | True when a usable perpetual observation is available. |
| `spot_perps_basis_available` | True when spot and perpetual observations are both available. |

### IV/RV state

| Feature | Description |
|---|---|
| `canonical_rv_source` | RV source identity inherited from the realized-volatility dataset. |
| `iv_minus_rv_1h` | Deprecated compatibility spread mixing annualized IV percentage points with non-annualized 1-hour RV. |
| `iv_minus_rv_1d` | Deprecated compatibility spread mixing annualized IV percentage points with non-annualized 1-day RV. |
| `iv_rv_ratio_1h` | Deprecated mixed-unit compatibility ratio for 1-hour RV. |
| `iv_rv_ratio_1d` | Deprecated mixed-unit compatibility ratio for 1-day RV. |
| `iv_rv_spread_30d_pct` | Unit- and horizon-matched `iv_30d_annualized_pct - rv_30d_annualized_pct`. |
| `iv_rv_ratio_30d` | Unit- and horizon-matched IV/RV ratio; null when RV is non-positive. |
| `iv_rv_zscore_1d` | Trailing one-day z-score of the legacy IV-minus-RV spread, retained for compatibility. |
| `iv_rv_percentile_30d` | Closed trailing 30-day percentile of the legacy IV-minus-RV spread. |
| `minutes_since_iv_observation` | Whole minutes since the IV observation. |
| `minutes_since_rv_observation` | Whole minutes since the RV observation. |
| `iv_available` | True when an IV observation is available. |
| `rv_available` | True when an RV observation is available. |

### Index-price state

| Feature | Description |
|---|---|
| `index_price` | Canonical index/fair-value price aligned to the minute. |
| `index_price_is_observed` | True when the index value was directly observed in the minute. |
| `minutes_since_index_price_observation` | Whole minutes since the direct index observation. |

### Futures-summary state

| Feature | Description |
|---|---|
| `futures_summary_instrument_type` | Instrument type supplied by the futures summary. |
| `futures_summary_mark_price` | Deribit mark price. |
| `futures_summary_index_price` | Underlying index price from the summary. |
| `futures_summary_mark_index_spread` | `mark_price - index_price`. |
| `futures_summary_mark_index_ratio` | `mark_price / index_price`; null for an unavailable/zero index. |
| `futures_summary_open_interest` | Open interest reported by the summary. |
| `futures_summary_volume` | Summary volume. |
| `futures_summary_turnover` | Summary turnover/notional. |
| `futures_summary_funding_rate` | Funding rate reported by the summary. |
| `futures_summary_is_observed` | True when the summary was directly observed in the minute. |
| `minutes_since_summary_observation` | Whole minutes since the direct summary observation. |

### Option-surface state

| Feature | Description |
|---|---|
| `options_surface_atm_iv` | At-the-money implied-volatility estimate across eligible contracts. |
| `options_surface_short_dated_iv` | Short-dated implied-volatility estimate. |
| `options_surface_skew` | Cross-strike IV skew proxy. |
| `options_surface_term_structure` | Difference/gradient proxy across option maturities. |
| `options_surface_put_call_iv_spread` | Put-minus-call IV spread proxy. |
| `options_surface_contract_count` | Number of contracts contributing to the minute surface. |
| `options_surface_fresh_quote_count` | Number of contributing contracts with fresh quotes. |
| `options_surface_stale_quote_count` | Number of contributing contracts with stale quotes. |
| `options_surface_max_quote_age_seconds` | Maximum quote age among contributing contracts. |
| `options_surface_quote_coverage_ratio` | Fresh/usable quote coverage across eligible contracts. |

### Perpetual L2 state

| Feature | Description |
|---|---|
| `perps_l2_best_bid_price` | Best perpetual bid price. |
| `perps_l2_best_ask_price` | Best perpetual ask price. |
| `perps_l2_mid_price` | Midpoint of best bid and ask. |
| `perps_l2_spread` | Best-ask minus best-bid spread. |
| `perps_l2_top_bid_size` | Size at best bid. |
| `perps_l2_top_ask_size` | Size at best ask. |
| `perps_l2_top_of_book_imbalance` | Normalized top-of-book bid/ask size imbalance. |
| `perps_l2_bid_depth_10bps` | Bid depth within 10 basis points of reference price. |
| `perps_l2_ask_depth_10bps` | Ask depth within 10 basis points of reference price. |
| `perps_l2_bid_depth_50bps` | Bid depth within 50 basis points. |
| `perps_l2_ask_depth_50bps` | Ask depth within 50 basis points. |
| `perps_l2_quote_age_seconds` | Age of the selected quote in seconds. |
| `perps_l2_quote_available` | True when a usable quote exists. |
| `perps_l2_stale_quote` | True when the quote breaches the configured freshness threshold. |
| `perps_l2_minutes_since_observation` | Whole minutes since the last L2 observation. |
| `perps_l2_as_of` | UTC timestamp represented by the L2 feature row. |
| `perps_l2_live_snapshot_derived` | True when the feature came from live snapshot data. |

### Option L2 state

| Feature | Description |
|---|---|
| `options_l2_contract_count` | Number of distinct option instruments contributing to the minute. |
| `options_l2_quote_coverage_ratio` | Mean usable-quote indicator across contributing options. |
| `options_l2_stale_quote_ratio` | Mean stale-quote indicator across contributing options. |
| `options_l2_median_spread` | Median option spread across contributing contracts. |
| `options_l2_top_bid_depth` | Sum of top bid sizes across contributing options. |
| `options_l2_top_ask_depth` | Sum of top ask sizes across contributing options. |
| `options_l2_bid_depth_10bps` | Sum of option bid depth within 10 basis points. |
| `options_l2_ask_depth_10bps` | Sum of option ask depth within 10 basis points. |
| `options_l2_bid_depth_50bps` | Sum of option bid depth within 50 basis points. |
| `options_l2_ask_depth_50bps` | Sum of option ask depth within 50 basis points. |
| `options_l2_max_quote_age_seconds` | Maximum quote age across contributing options. |
| `options_l2_as_of` | Latest source minute represented by the option L2 aggregation. |
| `options_l2_live_snapshot_derived` | True when the aggregation came from live snapshot data. |

### Historical-volatility reference

| Feature | Description |
|---|---|
| `historical_volatility_reference` | External Deribit historical-volatility value; not an internally computed `rv_*` estimator. |
| `historical_volatility_source_timestamp` | UTC timestamp of the external observation. |
| `historical_volatility_available` | True when the external reference is present. |

### Strategy state

All strategy fields are current/trailing features. They are not targets or labels.

| Feature | Description |
|---|---|
| `strategy_momentum_log_return_1m` | One-minute perpetual log return. |
| `strategy_momentum_log_return_5m` | Five-minute perpetual log return. |
| `strategy_momentum_log_return_15m` | Fifteen-minute perpetual log return. |
| `strategy_momentum_vol_scaled_return_5m` | Five-minute log return divided by one-hour realized volatility. |
| `strategy_momentum_vol_scaled_return_15m` | Fifteen-minute log return divided by one-hour realized volatility. |
| `strategy_trend_ema_3m` | Three-minute-span exponential moving average of perpetual close. |
| `strategy_trend_ema_slope_3m` | One-minute fractional change in the 3-minute EMA. |
| `strategy_trend_ema_10m` | Ten-minute-span exponential moving average of perpetual close. |
| `strategy_trend_ema_slope_10m` | One-minute fractional change in the 10-minute EMA. |
| `strategy_trend_breakout_distance_5m` | Perpetual close relative to the trailing 5-minute high minus one. |
| `strategy_trend_breakout_distance_15m` | Perpetual close relative to the trailing 15-minute high minus one. |
| `strategy_trend_persistence_5m` | Trailing 5-minute mean of return direction encoded as -1/0/+1. |
| `strategy_trend_persistence_15m` | Trailing 15-minute mean of return direction. |
| `strategy_reversion_price_zscore_5m` | Price deviation from trailing 5-minute mean divided by trailing standard deviation. |
| `strategy_reversion_price_zscore_15m` | Price deviation from trailing 15-minute mean divided by trailing standard deviation. |
| `strategy_reversion_vwap_distance_5m` | Fractional distance from trailing 5-minute volume-weighted average price. |
| `strategy_reversion_vwap_distance_15m` | Fractional distance from trailing 15-minute volume-weighted average price. |
| `strategy_reversion_bollinger_distance_5m` | Price deviation from 5-minute mean scaled by two standard deviations. |
| `strategy_reversion_bollinger_distance_15m` | Price deviation from 15-minute mean scaled by two standard deviations. |
| `strategy_reversion_spot_perp_spread_zscore_5m` | Z-score of spot/perpetual spread over 5 minutes. |
| `strategy_reversion_spot_perp_spread_zscore_15m` | Z-score of spot/perpetual spread over 15 minutes. |
| `strategy_reversion_half_life_5m` | Mean-reversion half-life proxy from trailing lag-one persistence; null outside a stable `(0,1)` coefficient. |
| `strategy_cost_turnover_notional_1m` | Perpetual close multiplied by perpetual volume for the minute. |
| `strategy_cost_turnover_notional_5m` | Trailing 5-minute sum of turnover notional. |
| `strategy_cost_turnover_notional_15m` | Trailing 15-minute sum of turnover notional. |
| `strategy_cost_spot_perp_spread` | `perp_close_price / spot_ohlcv_close_price - 1`. |

### Prediction targets

Each target is emitted for `1h`, `4h`, and `1d`. Incomplete future windows remain null. The cost
adjustment subtracts 2 bps plus the absolute current funding rate prorated by horizon minutes over
480 minutes. The regime-shift threshold is an absolute IV/RV z-score change of at least 1.0.

| Feature pattern | Concrete features | Description |
|---|---|---|
| `target_forward_return_<h>` | `target_forward_return_1h`, `target_forward_return_4h`, `target_forward_return_1d` | Forward log return from current perpetual close to the horizon close. |
| `target_forward_drawdown_<h>` | `target_forward_drawdown_1h`, `target_forward_drawdown_4h`, `target_forward_drawdown_1d` | Minimum future-window perpetual close divided by current close minus one. |
| `target_cost_adjusted_return_<h>` | `target_cost_adjusted_return_1h`, `target_cost_adjusted_return_4h`, `target_cost_adjusted_return_1d` | Forward return net of fixed transaction cost and prorated funding cost. |
| `target_future_rv_<h>` | `target_future_rv_1h`, `target_future_rv_4h`, `target_future_rv_1d` | Realized-volatility feature observed at the future horizon. |
| `target_future_iv_spread_change_<h>` | `target_future_iv_spread_change_1h`, `target_future_iv_spread_change_4h`, `target_future_iv_spread_change_1d` | Future IV/RV spread minus current spread. |
| `label_regime_shift_<h>` | `label_regime_shift_1h`, `label_regime_shift_4h`, `label_regime_shift_1d` | True when the absolute future/current IV/RV z-score difference reaches the configured threshold. |

### External L2 passthrough

| Feature pattern | Description |
|---|---|
| `l2_<source_column>` | Every non-key column from the selected external `gold.l2.micro.m1` artifact, renamed with the `l2_` prefix. The exact expanded schema is written to the build manifest. |

## Non-contract physical Gold artifacts

The generated physical inventory can also contain live-origin artifacts that are not members of
`GOLD_DATASET_CONTRACTS`. They are upstream or compatibility products, not valid `gold-build
--dataset-id` values.

| Physical artifact | Purpose |
|---|---|
| `index_price_m1_features` | Live-loader minute index-price aggregate used upstream of canonical index features. |
| `instrument_metadata_daily_summary` | Daily live-loader instrument metadata summary. |
| `l2_m1_features` | External L2 Gold feature artifact consumed by the hybrid passthrough contract. |
| `option_surface_m1` | Live-loader option-surface artifact used upstream of canonical option-surface features. |

## Build and validation

Build all supported Gold contracts:

```bash
uv run python main.py gold-build --manifest --plot --maxprocesses 4 --no-json-output
```

Build one contract:

```bash
uv run python main.py gold-build \
  --dataset-id gold.market.history_full.m1 \
  --manifest \
  --plot \
  --maxprocesses 4 \
  --no-json-output
```

Validate this catalog against the typed registry and generated inventory policy:

```bash
uv run python scripts/validate_readme_inventory.py
```

The script name remains unchanged for command compatibility, but its canonical input is now
`DATASETS.md`.
