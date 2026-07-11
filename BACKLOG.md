# Backlog

This backlog is the source of truth for stacked, atomic PRs that bring every Bronze dataset into a
contracted Silver representation suitable for IV/RV and regime-change research.

Last updated: 2026-07-10

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

## Status Semantics And Working-Tree Policy

`Done` on PR-01 through PR-18 means that the contract, transformation code, and focused tests are merged.
It does not mean that a complete local Lake artifact exists. The 2026-07-10 inventory found ten physical
Silver dataset types and nine physical Gold families; all other outputs require materialization or rebuild.

Every stacked PR must begin with:

```bash
git status --short
git branch --show-current
git fetch origin --prune
git checkout main
git pull --ff-only origin main
git checkout -b codex/<pr-name>
```

Before commit and after validation, `git status --short` must contain only the intended tracked source,
test, documentation, or configuration files. Ignored Lake outputs must never be committed. An unexpected
untracked or modified file is a stop condition; do not use `git add -A`, `git stash`, reset, or cleanup
commands to hide it. Each PR must include its exact status output in the handoff or PR notes.

Intermediate stacked PRs run only their smallest reliable related tests. The final PR before the squash
merge into `main` runs the complete configured quality suite and a coverage report.

The cron-friendly medallion script must remain a complete layer scheduler:

- Bronze step: runs every historical Bronze dataset that this repository can fetch directly.
- Silver step: runs every Bronze-backed and live-origin Silver dataset family supported by `silver-build`.
- Gold step: omits `--dataset-id` intentionally so `gold-build` builds every supported Gold contract.

Current complete-run commands:

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
uv run python main.py silver-build --dataset spot_ohlcv perps_ohlcv open_interest funding perps_trades options_trades volatility_index_data volatility_index_snapshot_1m realized_volatility iv_rv index_price_snapshot_1m futures_summary_snapshot_1m options_ticker_snapshot_1m options_instrument_ticker_snapshot_1m options_surface_1m_feature perps_l2_snapshot_1m options_l2_snapshot_1m recent_trade_snapshot_1m instrument_metadata_snapshot_daily futures_instrument_metadata_snapshot_daily historical_volatility --manifest --plot --maxprocesses 4 --no-json-output
uv run python main.py gold-build --manifest --plot --maxprocesses 4 --no-json-output
```

PRs that add a new supported Silver or Gold dataset must update `config.yaml`, this command list, and
the parser/config compatibility tests in the same change set.

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

Status: Done

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

## Physical Materialization And Gold Stack

The following stack closes the gap between the merged code contracts and the physical Lake snapshot.
Each PR is atomic and must not regenerate unrelated historical artifacts. Builds must be deterministic:
fixed input manifest, stable sort, explicit deduplication, fixed columns, explicit timestamp semantics,
and a manifest containing source paths, source hashes, row counts, date spans, missing days, and builder
commit. The live-origin datasets are the snapshot families from `crypto-live-loader`; historical-origin
datasets are fetched and transformed by this repository.

### PR-19: Silver Materialization Audit And Build Manifest

Status: Merge-ready - PR #79: https://github.com/SergejSchweizer/crypto-history-loader/pull/79

Branch: `codex/pr19-silver-materialization-audit`

Depends on: PR-18

Goal:
Create one deterministic audit command that compares Bronze inventory, Silver contracts, physical Silver
files, and per-series missing-day statistics.

Scope:
- Add a read-only inventory report; do not write Lake data during audit.
- Add `dataset-inventory` as the deterministic read-only CLI entrypoint for the report.
- Validate that `config.yaml` schedules every `silver-build` dataset family and that the Gold medallion
  step still builds every supported Gold dataset by omitting `--dataset-id`.
- Report `dataset_type`, origin repository, schema columns, file count, row count, start/end, observed days,
  missing days, and contract status.
- Fail when a Bronze dataset has neither a physical Silver output nor an explicitly documented exception.
- Add fixture tests for partition dates, mixed series lifetimes, and legacy `perp` compatibility.

Acceptance:
- The report reproduces the README inventory without manual edits.
- `dataset-inventory` runs without writing Lake files and supports Markdown and JSON rendering.
- A config compatibility test fails if any Silver or Gold dataset is omitted from the complete medallion run.
- `git status --short` remains clean after the audit.
- Targeted tests cover only inventory, contract, and report formatting behavior.

### PR-20: Volatility Index Silver Materialization

Status: Merge-ready - PR #80: https://github.com/SergejSchweizer/crypto-history-loader/pull/80

Branch: `codex/pr20-volatility-silver-materialization`

Depends on: PR-19

Goal:
Materialize historical and live IV observations into one canonical, freshness-aware Silver feature.

Scope:
- Build `volatility_index_data_observed` from historical `value` Bronze rows.
- Build `volatility_index_snapshot_1m_observed` from live `open/high/low/close` snapshots.
- Build `volatility_index_1m_feature` with live snapshot precedence and historical fallback.
- Preserve source lineage and represent missing SOL live data explicitly.

Acceptance:
- Exact output variables are the contract columns in `application/dataset_contracts.py`.
- Dedup key is `exchange/symbol/timestamp`; newest `ingested_at` wins deterministically.
- Targeted tests cover historical fallback, live precedence, duplicate rows, and missing IV.

### PR-21: Realized Volatility And IV/RV Silver Materialization

Status: Merge-ready - PR #81: https://github.com/SergejSchweizer/crypto-history-loader/pull/81

Branch: `codex/pr21-rv-iv-rv-silver-materialization`

Depends on: PR-20

Goal:
Materialize leakage-safe RV and IV/RV state features for BTC, ETH, and SOL.

Scope:
- Build `realized_volatility_1m_feature` from spot/perpetual OHLCV.
- Build `iv_rv_1m_feature` from the canonical IV feature and realized-volatility feature.
- Keep trailing windows only: 5m, 15m, 1h, 4h, and 1d.
- Preserve availability, freshness, and source timestamps; never cross-fill symbols.

Acceptance:
- Contract variables include RV windows, Parkinson RV, jump proxy, IV-RV spreads/ratios, z-score,
  percentile, and availability flags.
- Tests prove no future timestamps enter a rolling value and partial source availability remains explicit.
- Targeted tests cover RV windows, IV/RV joins, and timestamp alignment.

### PR-22: Index Price And Futures Summary Silver Materialization

Status: Merge-ready - PR #83: https://github.com/SergejSchweizer/crypto-history-loader/pull/83

Branch: `codex/pr22-index-futures-silver-materialization`

Depends on: PR-21

Goal:
Materialize index/mark dislocation, derivatives summary, and freshness context.

Scope:
- Build `index_price_snapshot_1m_observed` and `index_price_1m_feature`.
- Build `futures_summary_snapshot_1m_observed` and `futures_summary_1m_feature`.
- Normalize currency/index names to canonical symbols and preserve optional-field availability.

Acceptance:
- Dedup keys and one-minute ordering are deterministic.
- Tests cover missing optional summary fields and symbol normalization.
- No duplicate funding or open-interest semantics are silently introduced.

### PR-23: Options Ticker Silver Materialization

Status: Merge-ready - PR #84: https://github.com/SergejSchweizer/crypto-history-loader/pull/84

Branch: `codex/pr23-options-ticker-silver-materialization`

Depends on: PR-22

Goal:
Materialize option ticker and instrument-ticker observations needed for surface reconstruction.

Scope:
- Build `options_ticker_snapshot_1m_observed`.
- Build `options_instrument_ticker_snapshot_1m_observed`.
- Normalize instrument name, underlying, expiry, strike, option type, IV, greeks, quotes, OI, and volume.
- Keep source-family lineage so overlapping ticker records can be reconciled later.

Acceptance:
- Tests cover calls/puts, invalid expiry/strike, missing greeks, and duplicate snapshots.
- Output columns match the Silver contract exactly.
- Only the related options ticker tests run on this intermediate PR.

### PR-24: Options Surface Silver Materialization

Status: Merge-ready - PR #94: https://github.com/SergejSchweizer/crypto-history-loader/pull/94

Branch: `codex/pr24-options-surface-silver-materialization`

Depends on: PR-23

Goal:
Build deterministic option-surface features for IV level, skew, smile, and term structure.

Scope:
- Build `options_surface_1m_feature` using fixed expiry and moneyness bucket rules.
- Calculate ATM IV, short-dated IV, skew, term structure, put/call IV spread, coverage, and quote age.
- Exclude stale or invalid quotes using explicit quality rules.

Acceptance:
- Bucket boundaries and tie-breaking are tested and documented.
- No future contract observation enters a minute feature.
- Surface coverage and missing days are present in the build manifest.

### PR-25: Perpetual L2 Silver Materialization

Status: Merge-ready - PR #95: https://github.com/SergejSchweizer/crypto-history-loader/pull/95

Branch: `codex/pr25-perps-l2-silver-materialization`

Depends on: PR-24

Goal:
Materialize perpetual order-book features for liquidity and microstructure regime context.

Scope:
- Build `perps_l2_snapshot_1m_observed` and `perps_l2_1m_feature`.
- Produce mid, spread, top depth, imbalance, depth-at-10/50bps, quote age, and stale flags.
- Validate non-negative prices/sizes and ordered bid/ask levels.

Acceptance:
- One deterministic row per symbol/minute where observations exist.
- Malformed/empty books are classified, not silently coerced into valid liquidity.
- Targeted L2 tests cover strict and lenient quality modes.

### PR-26: Options L2 Silver Materialization

Status: Merge-ready - PR #96: https://github.com/SergejSchweizer/crypto-history-loader/pull/96

Branch: `codex/pr26-options-l2-silver-materialization`

Depends on: PR-25

Goal:
Materialize option order-book features and liquidity filters for surface quality.

Scope:
- Build `options_l2_snapshot_1m_observed` and `options_l2_1m_feature`.
- Normalize contract metadata and quote fields.
- Add empty-book, stale-quote, spread, mid, and depth indicators.

Acceptance:
- Tests cover missing contract metadata, empty books, and invalid quote ordering.
- Surface consumers can filter by explicit liquidity-quality variables.

### PR-27: Recent Trade Snapshot Silver Materialization

Status: Merge-ready - PR #97: https://github.com/SergejSchweizer/crypto-history-loader/pull/97

Branch: `codex/pr27-recent-trades-silver-materialization`

Depends on: PR-26

Goal:
Materialize live recent trades without treating snapshots as a replacement for historical tick data.

Scope:
- Build `recent_trade_snapshot_1m_observed`.
- Deduplicate by source trade ID or a documented composite fallback key.
- Preserve snapshot timestamp, exchange timestamp, direction, liquidation, block-trade, and notional fields.

Acceptance:
- Output is explicitly marked `snapshot_derived`.
- Overlap with historical trades produces a deterministic reconciliation report.
- Tests cover missing IDs, duplicate snapshots, and timestamp ordering.

### PR-28: Instrument Metadata Silver Materialization

Status: Merge-ready - PR #98: https://github.com/SergejSchweizer/crypto-history-loader/pull/98

Branch: `codex/pr28-instrument-metadata-silver-materialization`

Depends on: PR-27

Goal:
Materialize the daily instrument universe required for contract parsing and valid option-surface joins.

Scope:
- Build `instrument_metadata_snapshot_daily_observed`.
- Build `futures_instrument_metadata_snapshot_daily_observed`.
- Normalize currency, kind, expiry, strike, option type, tick/contract size, listing, and active state.
- Select the latest valid metadata snapshot per instrument/day deterministically.

Acceptance:
- Tests cover option and future metadata, expiry transitions, and inactive instruments.
- Metadata joins never infer contract identity from an unvalidated string.

### PR-29: Historical Silver Backfill And Reconciliation

Status: In progress - PR #99: https://github.com/SergejSchweizer/crypto-history-loader/pull/99

Branch: `codex/pr29-silver-backfill-reconciliation`

Depends on: PR-28

Goal:
Run the complete Silver build over all available Bronze history and live snapshot windows.

Scope:
- Materialize all missing outputs from PR-20 through PR-28.
- Rebuild stale `perp` outputs as canonical `perps_ohlcv` while preserving read compatibility.
- Emit one inventory manifest per dataset/series with rows, columns, start/end, observed/missing days,
  source hash, builder commit, and quality counters.
- Do not commit Parquet data; commit only reproducible reports or generated metadata explicitly allowed by policy.

Acceptance:
- Every Bronze dataset has a physical Silver destination or a documented exception.
- All outputs are sorted, deduplicated, schema-valid, and restart-safe.
- Full Silver build report is reproducible from the same input manifest.

### PR-30: Historical Gold IV/RV Feature Dataset

Status: In progress - PR #100: https://github.com/SergejSchweizer/crypto-history-loader/pull/100

Branch: `codex/pr30-historical-gold-iv-rv`

Depends on: PR-29

Goal:
Create the historical Gold dataset used for IV/RV prediction without forward-looking values.

Scope:
- Materialize `gold.market.iv_rv.m1` from `iv_rv_1m_feature`.
- Extend the canonical historical state join with spot/perps returns, RV horizons, IV level/change,
  IV-RV spread/ratio, funding, OI, and data-quality fields.
- Keep `historical_volatility_observed` as a named external reference, never as a substitute for computed RV.

Acceptance:
- Manifest reports source coverage/freshness and feature-set hash.
- Point-in-time tests prove all features are known at `timestamp_m1`.
- Intermediate PR runs only Gold IV/RV tests.

### PR-31: Historical Gold Regime Features

Status: In progress - PR #101: https://github.com/SergejSchweizer/crypto-history-loader/pull/101; PR #103: https://github.com/SergejSchweizer/crypto-history-loader/pull/103

Branch: `codex/pr31-historical-gold-regime-features`

Depends on: PR-30

Goal:
Materialize `gold.market.regime_features.m1` as the reusable state representation for regime-change models.

Scope:
- Require spot/perps OHLCV, funding, OI, RV, and IV/RV features.
- Include optional index, futures summary, option surface, L2, and external historical-volatility features
  as typed nullable columns with availability flags.
- Keep all transformations trailing and deterministic.

Acceptance:
- Required-source gaps fail loudly; optional-source gaps do not change schema or minute grid.
- No regime labels or future returns are present in this feature dataset.

### PR-32: Historical Strategy Feature Families

Status: In progress - PR #104: https://github.com/SergejSchweizer/crypto-history-loader/pull/104; PR #105: https://github.com/SergejSchweizer/crypto-history-loader/pull/105

Branch: `codex/pr32-historical-strategy-features`

Depends on: PR-31

Goal:
Add reusable feature families for momentum, trend following, and mean reversion.

Scope:
- Momentum/trend: multi-horizon returns, EMA slopes, breakout distance, trend persistence, and volatility-scaled direction.
- Mean reversion: rolling z-scores, VWAP/EMA distance, Bollinger distance, spread reversion, and estimated half-life.
- Add turnover, spread, and volatility normalization needed for realistic optimization.

Acceptance:
- Each feature has a declared lookback and no future dependency.
- Features are state variables only; strategy targets remain separate.
- Tests cover warm-up periods, constant-price series, zero-volume, and numerical stability.

### PR-33: Historical Prediction Targets And Regime Labels

Status: Planned

Branch: `codex/pr33-historical-targets-labels`

Depends on: PR-32

Goal:
Create explicitly forward-looking training targets without contaminating inference features.

Scope:
- Future RV and IV-change targets at 1h, 4h, and 1d.
- Forward return, drawdown, and cost-adjusted return targets for momentum/trend/mean-reversion evaluation.
- Regime-change labels with fixed transition and horizon definitions.

Acceptance:
- Targets live in a separate dataset and are never joined into live feature outputs.
- Label definitions, horizons, transaction-cost assumptions, and null rules are versioned.
- Leakage tests fail if a target column appears in a feature contract.

### PR-34: Live-Origin Gold Feature Contract

Status: Planned

Branch: `codex/pr34-live-gold-feature-contract`

Depends on: PR-31

Goal:
Expose live-loader index, surface, L2, trade, and metadata data through feature schemas compatible with
historical inference inputs.

Scope:
- Add `gold.live.volatility_features.m1`, `gold.live.microstructure_features.m1`,
  `gold.live.regime_features.m1`, and `gold.live.instrument_universe.d1`.
- Preserve live source lineage, `as_of`, freshness, coverage, and availability flags.
- Do not backfill live data with historical values inside a live dataset.

Acceptance:
- Historical/live overlapping features have identical names, units, timestamp semantics, and null rules.
- Live artifacts remain clearly marked as snapshot-derived and source-repository-specific.

### PR-35: Gold Inventory Documentation And Release Gate

Status: Planned

Branch: `codex/pr35-gold-silver-documentation-gate`

Depends on: PR-29, PR-33, PR-34

Goal:
Make README and backlog status reproducible and release-blocking.

Scope:
- Generate the Bronze/Silver/Gold inventory report from the audit command.
- Document every dataset's variables, origin, series, start/end, observed days, missing days, and physical/contract status.
- Add CI validation that README inventory date and schema lists match the generated report.
- Record the exact `git status --short` and validation commands in the PR handoff.

Acceptance:
- No dataset is described as present when only its contract exists.
- Final stack run executes the full quality suite, `coverage run -m pytest`, and `coverage report`.
- `main` is clean after merge and all stacked branches are deleted only after their commits are reachable from `main`.

## Completion Definition

The stack is complete when:

- Every local Bronze `dataset_type` has at least one explicit Silver destination or an explicit archived/deprecated
  decision in this file.
- Every Bronze dataset with available local files has a materialized Silver output, with per-series start/end,
  observed days, missing days, row count, schema, origin, and source lineage in the inventory report.
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
- Historical and live-origin Gold features use compatible point-in-time schemas and are explicitly separated.
- Forward-looking prediction targets and regime labels are stored separately from feature datasets.
- README and the generated inventory report agree on every dataset variable and coverage statistic.
- The final squash PR passes the complete quality and coverage suite; no stacked intermediate PR is required to
  run the full suite.
