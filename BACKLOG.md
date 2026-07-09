# Backlog

This backlog is the source of truth for stacked, atomic PRs that bring every Bronze dataset into a
contracted Silver representation suitable for IV/RV and regime-change research.

Last updated: 2026-07-09

## Policy

- Keep PRs stacked in the order listed here; each PR must be mergeable after its predecessor.
- Keep each PR atomic: one dataset family, one contract boundary, or one shared adapter change.
- Keep outputs deterministic: stable partition layout, stable sort keys, explicit dedup keys, fixed column order,
  and no wall-clock-dependent feature values except build metadata fields.
- Keep Silver dataset contracts explicit in `application/dataset_contracts.py`.
- Keep Bronze raw fields audit-friendly; Silver may clean, normalize, deduplicate, aggregate, and align, but should
  not create model labels or strategy-specific targets.
- Keep IV/RV and regime-change features as reusable market-state features. Prediction labels belong in Gold or a
  later modelling layer.
- Update this file in every PR that changes dataset naming, contracts, scope, order, or completion status.

## Current Coverage Snapshot

Bronze dataset types present locally:

```text
funding
futures_instrument_metadata_snapshot_daily
futures_summary_snapshot_1m
historical_volatility
index_price_snapshot_1m
instrument_metadata_snapshot_daily
open_interest
options_instrument_ticker_snapshot_1m
options_l2_snapshot_1m
options_ticker_snapshot_1m
options_trades
perps_l2_snapshot_1m
perps_ohlcv
perps_trades
recent_trade_snapshot_1m
spot_ohlcv
volatility_index_data
volatility_index_snapshot_1m
```

Bronze datasets already represented in Silver:

| Bronze Dataset | Current Silver Dataset(s) | Status |
|---|---|---|
| `spot_ohlcv` | `spot_ohlcv` | Exists |
| `perps_ohlcv` | `perp` | Exists, needs naming cleanup to `perps_ohlcv` |
| `funding` | `funding_observed`, `funding_1m_feature` | Exists |
| `open_interest` | `open_interest_observed`, `open_interest_1m_feature` | Exists |
| `perps_trades` | `perps_trades_observed`, `perps_trades_1m_feature` | Exists |
| `options_trades` | `options_trades_observed`, `options_trades_1m_feature` | Exists |
| `volatility_index_data` | `volatility_index_data_observed`, `volatility_index_1m_feature` | Exists |
| `volatility_index_snapshot_1m` | `volatility_index_snapshot_1m_observed`, `volatility_index_1m_feature` | Exists |

Bronze datasets not yet represented in Silver:

```text
futures_instrument_metadata_snapshot_daily
futures_summary_snapshot_1m
historical_volatility
index_price_snapshot_1m
instrument_metadata_snapshot_daily
options_instrument_ticker_snapshot_1m
options_l2_snapshot_1m
options_ticker_snapshot_1m
perps_l2_snapshot_1m
recent_trade_snapshot_1m
```

## PR Stack

### PR-01: Silver Contract Registry Baseline

Status: Done

Branch: `codex/pr01-silver-contract-registry-baseline`

Depends on: none

Goal:
Create a complete Silver contract inventory for every Bronze dataset, including the datasets that are not yet
implemented. This makes the stack deterministic before transformation code is added.

Scope:
- Add contract placeholders for every missing Silver dataset.
- Define canonical naming:
  - `perps_ohlcv` replaces legacy Silver `perp`.
  - `volatility_index_1m_observed` and `volatility_index_1m_feature` become the canonical IV index outputs.
  - Live snapshot datasets keep their source family in the name, for example `options_l2_snapshot_1m_observed`.
- Add tests that fail if a Bronze dataset has no declared Silver destination.
- Document missing-data policy, timestamp column, timestamp semantics, and output columns for each planned output.

Acceptance:
- `application/dataset_contracts.py` lists every planned Silver dataset.
- A contract test proves every local Bronze `dataset_type` has at least one Silver destination.
- No transformation behavior changes yet.

### PR-02: Silver Naming Compatibility For OHLCV

Status: Done

Branch: `codex/pr02-silver-ohlcv-naming-compat`

Depends on: PR-01

Goal:
Remove ambiguity around `perp` vs `perps_ohlcv` before adding more Silver outputs.

Scope:
- Emit `perps_ohlcv` as the canonical Silver dataset for Bronze `perps_ohlcv`.
- Keep backward-compatible discovery/read support for existing `dataset_type=perp` artifacts.
- Update Gold requirements to prefer `perps_ohlcv` and fall back to `perp` only when needed.
- Update README and tests.

Acceptance:
- New Silver builds write `dataset_type=perps_ohlcv`.
- Existing local `dataset_type=perp` Silver files remain readable.
- Gold tests cover canonical and fallback paths.

### PR-03: Volatility Index OHLC Bronze To Silver

Status: Done

Branch: `codex/pr03-volatility-index-ohlc-silver`

Depends on: PR-02

Goal:
Make Deribit volatility index usable as the primary IV index source for IV/RV regime work.

Scope:
- Parse all Deribit `get_volatility_index_data` fields: `timestamp`, `open`, `high`, `low`, `close`.
- Store OHLC fields in Bronze `volatility_index_data`.
- Convert normalized `1m` to Deribit API resolution `60`.
- Build `volatility_index_data_observed` or canonical `volatility_index_1m_observed` with:
  - `timestamp`
  - `exchange`
  - `symbol`
  - `volatility_open`
  - `volatility_high`
  - `volatility_low`
  - `volatility_close`
  - `volatility_value` as `volatility_close`
  - `volatility_source_timestamp`
  - lineage columns
- Keep old Bronze files with only `value` readable by falling back to `value`.

Acceptance:
- Unit tests prove all OHLC endpoint fields are parsed and written.
- Silver tests prove `volatility_value == close`.
- Live smoke test proves historical Deribit rows are returned for a bounded past range.

### PR-04: Live Volatility Snapshot To Canonical IV Silver

Status: Done

Branch: `codex/pr04-volatility-snapshot-silver`

Depends on: PR-03

Goal:
Integrate `volatility_index_snapshot_1m` from `crypto-live-loader` as the fresher IV index source.

Scope:
- Add Bronze reader for path layout using `currency` and `source`.
- Build `volatility_index_snapshot_1m_observed`.
- Normalize columns to the same IV semantic names as PR-03.
- Add canonical feature builder `volatility_index_1m_feature` that can source from:
  - `volatility_index_snapshot_1m_observed` first
  - `volatility_index_data_observed` as historical fallback
- Deduplicate by `exchange/symbol/timestamp`, newest `ingested_at` wins.

Acceptance:
- BTC/ETH snapshot rows build into Silver.
- Canonical feature output contains one row per observed minute.
- Source precedence and fallback are covered by tests.

### PR-05: IV Feature Layer For Regime Research

Status: Done

Branch: `codex/pr05-iv-index-feature-layer`

Depends on: PR-04

Goal:
Create reusable IV features without modelling labels.

Scope:
- Build `volatility_index_1m_feature`.
- Add deterministic features:
  - `iv_open`, `iv_high`, `iv_low`, `iv_close`
  - `iv_range`
  - `iv_return_1m`
  - `iv_change_5m`, `iv_change_15m`, `iv_change_1h`
  - `iv_zscore_1d`, `iv_zscore_7d`
  - `iv_percentile_30d`
  - source freshness fields
- Use trailing windows only; no future leakage.

Acceptance:
- Tests prove rolling features use only current and past timestamps.
- Output has stable columns and sort order.
- Gold can consume `volatility_index_1m_feature`.

### PR-06: RV Feature Layer From Spot And Perps OHLCV

Status: Done

Branch: `codex/pr06-rv-feature-layer`

Depends on: PR-05

Goal:
Build realized-volatility features needed for IV/RV spread and regime state.

Scope:
- Build `realized_volatility_1m_feature` from `spot_ohlcv` and `perps_ohlcv`.
- Add features:
  - log returns
  - rolling RV windows: `5m`, `15m`, `1h`, `4h`, `1d`
  - Parkinson/range volatility from OHLC
  - jump proxy from absolute return z-scores
  - source flags for spot/perps availability
- Keep symbol normalization deterministic.

Acceptance:
- Tests prove RV windows do not leak future data.
- Feature output is available for BTC/ETH/SOL where OHLCV exists.
- Gold can join RV with IV.

### PR-07: IV/RV Spread Feature Dataset

Status: Planned

Branch: `codex/pr07-iv-rv-spread-feature`

Depends on: PR-06

Goal:
Create the direct IV/RV state inputs for regime-change prediction.

Scope:
- Build `iv_rv_1m_feature` from `volatility_index_1m_feature` and `realized_volatility_1m_feature`.
- Add features:
  - `iv_minus_rv_1h`
  - `iv_minus_rv_1d`
  - `iv_rv_ratio_1h`
  - `iv_rv_ratio_1d`
  - z-scores and percentile ranks
  - availability/freshness columns
- Do not create target labels.

Acceptance:
- Missing IV for SOL is represented explicitly, not silently filled from BTC/ETH.
- Tests cover partial data availability.
- Gold includes this dataset only when requirements are satisfied or explicitly optional.

### PR-08: Index Price Snapshot Silver

Status: Planned

Branch: `codex/pr08-index-price-snapshot-silver`

Depends on: PR-07

Goal:
Bring `index_price_snapshot_1m` into Silver for mark/index dislocation and fair-value context.

Scope:
- Build `index_price_snapshot_1m_observed`.
- Build optional `index_price_1m_feature`.
- Normalize `currency/symbol`, timestamp, index price, source, freshness.
- Deduplicate by `exchange/symbol/timestamp`.

Acceptance:
- Silver output covers all available index-price currencies.
- Tests cover schema drift and duplicate snapshots.
- Gold can join index price into regime features.

### PR-09: Futures Summary Snapshot Silver

Status: Planned

Branch: `codex/pr09-futures-summary-silver`

Depends on: PR-08

Goal:
Bring `futures_summary_snapshot_1m` into Silver as derivatives market-state context.

Scope:
- Build `futures_summary_snapshot_1m_observed`.
- Normalize per-instrument summary fields.
- Build selected 1m features for:
  - mark/index relationship
  - open interest if present
  - volume/turnover if present
  - funding-related summary fields if present
- Keep feature set conservative and schema-driven.

Acceptance:
- Tests cover missing optional fields.
- Output is deterministic by `exchange/instrument/timestamp`.
- No duplicated semantics with existing funding/open-interest features unless explicitly named.

### PR-10: Options Ticker Snapshot Silver

Status: Planned

Branch: `codex/pr10-options-ticker-silver`

Depends on: PR-09

Goal:
Bring option tickers into Silver for options-implied-volatility and skew context.

Scope:
- Build `options_ticker_snapshot_1m_observed`.
- Normalize contract metadata:
  - underlying
  - expiry
  - strike
  - option type
  - timestamp
- Preserve implied volatility, mark price, bid/ask, greeks, open interest, volume fields when present.
- Add deterministic dedup and validation.

Acceptance:
- Tests cover call/put parsing, invalid strike/expiry, and missing greeks.
- Output can support surface aggregation in later PRs.

### PR-11: Options Instrument Ticker Snapshot Silver

Status: Planned

Branch: `codex/pr11-options-instrument-ticker-silver`

Depends on: PR-10

Goal:
Bring `options_instrument_ticker_snapshot_1m` into Silver and reconcile it with option ticker snapshots.

Scope:
- Build `options_instrument_ticker_snapshot_1m_observed`.
- Use same contract metadata normalization as PR-10.
- Define precedence when both ticker snapshot families overlap.
- Add a reconciliation report for overlapping fields.

Acceptance:
- Tests prove overlapping records resolve deterministically.
- Silver output preserves source lineage.

### PR-12: Option Surface 1m Feature

Status: Planned

Branch: `codex/pr12-option-surface-feature`

Depends on: PR-11

Goal:
Create an option-surface feature dataset for IV regime and skew research.

Scope:
- Build `options_surface_1m_feature`.
- Aggregate observed option tickers by underlying/time/expiry/moneyness buckets.
- Add features:
  - ATM IV proxy
  - short-dated IV proxy
  - skew proxy
  - term-structure proxy
  - put/call IV spread
  - quote freshness and contract coverage counts
- Use deterministic bucket rules.

Acceptance:
- Tests cover bucket assignment and no future leakage.
- Output supports BTC/ETH where option tickers exist.

### PR-13: Perps L2 Snapshot Silver

Status: Planned

Branch: `codex/pr13-perps-l2-silver`

Depends on: PR-12

Goal:
Bring `perps_l2_snapshot_1m` into Silver for liquidity and microstructure regime context.

Scope:
- Build `perps_l2_snapshot_1m_observed`.
- Build `perps_l2_1m_feature`.
- Add features:
  - best bid/ask
  - mid price
  - spread
  - top-of-book depth
  - imbalance
  - depth within configured bps bands if available
- Validate non-negative prices/sizes and sorted books.

Acceptance:
- Tests cover malformed book snapshots.
- Feature output is one row per symbol/minute where observed.

### PR-14: Options L2 Snapshot Silver

Status: Planned

Branch: `codex/pr14-options-l2-silver`

Depends on: PR-13

Goal:
Bring `options_l2_snapshot_1m` into Silver for option liquidity and surface-quality filters.

Scope:
- Build `options_l2_snapshot_1m_observed`.
- Build optional `options_l2_1m_feature`.
- Normalize option contract metadata.
- Add liquidity features:
  - spread
  - mid
  - top depth
  - quote availability
  - stale quote flags

Acceptance:
- Tests cover contract parsing and empty-book snapshots.
- Surface features can filter contracts by liquidity quality.

### PR-15: Recent Trade Snapshot Silver

Status: Planned

Branch: `codex/pr15-recent-trade-snapshot-silver`

Depends on: PR-14

Goal:
Bring `recent_trade_snapshot_1m` into Silver and reconcile with `perps_trades`/`options_trades`.

Scope:
- Build `recent_trade_snapshot_1m_observed`.
- Normalize trade fields and instrument metadata.
- Add deterministic dedup by source trade ID or fallback composite key.
- Add reconciliation checks against historical trade datasets where overlap exists.

Acceptance:
- Tests cover missing trade IDs and overlapping historical trades.
- Output is clearly marked as snapshot-derived, not full historical tick coverage.

### PR-16: Instrument Metadata Silver

Status: Planned

Branch: `codex/pr16-instrument-metadata-silver`

Depends on: PR-15

Goal:
Bring instrument metadata snapshots into Silver for joins, contract parsing, and universe filters.

Scope:
- Build `instrument_metadata_snapshot_daily_observed`.
- Build `futures_instrument_metadata_snapshot_daily_observed`.
- Normalize:
  - instrument name
  - base/quote/settlement currencies
  - instrument kind
  - expiry
  - strike
  - option type
  - tick size
  - contract size
  - active/listed state
- Produce latest-valid metadata views per day.

Acceptance:
- Tests cover futures and options metadata.
- Other Silver builders can join metadata without ad hoc parsing.

### PR-17: Historical Volatility Silver

Status: Planned

Branch: `codex/pr17-historical-volatility-silver`

Depends on: PR-16

Goal:
Bring `historical_volatility` into Silver as an auxiliary RV/volatility reference source.

Scope:
- Build `historical_volatility_observed`.
- Preserve source value and timestamp.
- Validate non-negative finite volatility values.
- Clearly distinguish this source from internally computed RV in PR-06.

Acceptance:
- Tests prove naming does not collide with `realized_volatility_1m_feature`.
- Gold can include it as an optional reference feature.

### PR-18: Gold Contract Update For Regime Feature Set

Status: Planned

Branch: `codex/pr18-gold-regime-feature-contract`

Depends on: PR-17

Goal:
Expose the new Silver datasets through a coherent Gold dataset contract for IV/RV and regime-change research.

Scope:
- Add `gold.market.regime_features.m1`.
- Required sources:
  - `spot_ohlcv`
  - `perps_ohlcv`
  - `funding_1m_feature`
  - `open_interest_1m_feature`
  - `realized_volatility_1m_feature`
  - `iv_rv_1m_feature`
- Optional sources:
  - L2 features
  - option surface features
  - index price features
  - historical volatility reference
- Add manifest fields for optional source availability.

Acceptance:
- Gold build is deterministic with and without optional sources.
- Manifest exposes source coverage and freshness.
- No predictive labels are added.

## Completion Definition

The stack is complete when:

- Every local Bronze `dataset_type` has at least one explicit Silver destination or an explicit archived/deprecated
  decision in this file.
- Every Silver output has a tested contract.
- Every Silver builder has deterministic sorting, deduplication, and missing-data semantics.
- IV/RV and regime-change research can consume:
  - IV index features
  - RV features
  - IV/RV spread features
  - funding/open-interest context
  - trade-flow context
  - optional L2 and option-surface context
- Gold has a dedicated `gold.market.regime_features.m1` contract.
