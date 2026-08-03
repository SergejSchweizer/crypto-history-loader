# Backlog

This backlog is the source of truth for stacked, atomic PRs that bring every Bronze dataset into a
contracted Silver representation suitable for IV/RV and regime-change research.

Last updated: 2026-08-02

## Policy

- Use this backlog as a simple ticket system: one `PR-XX` entry is one ticket and one logical pull request.
- Every ticket must contain separate `Status`, `Updated`, `PR`, `Branch`, and `Depends on` fields. `Updated` uses
  `YYYY-MM-DD`; `Branch` remains in the merged ticket as historical traceability after branch deletion.
- The only valid statuses are `Planned`, `In Progress`, `Blocked`, `Ready`, and `Merged`. `Merged` is the only
  completed status. Terms such as `Done`, `Implemented`, `Complete`, and `Finished` are not valid statuses.
- Every ticket must contain numbered `Description` requirements (`R1`, `R2`, ...) and numbered `Acceptance`
  checks (`A1`, `A2`, ...) with exactly the same IDs. `A1` verifies only `R1`, `A2` verifies only `R2`, and so on.
  No description requirement or acceptance check may exist without its matching counterpart.
- A ticket may move to `Ready` only when every acceptance check passes. A ticket may move to `Merged` only after
  the PR is merged into `main` and the merge commit is reachable from `origin/main`.
- After reachability from `origin/main` is verified, delete both the local and remote feature branches. Record
  `Status: Merged`, the final `Updated` date, and the final PR URL before deleting them.
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
- Keep merged and superseded ticket entries in this backlog as an audit trail; never delete their recorded branch,
  date, PR URL, description, or acceptance evidence.

## Ticket Template

```markdown
### PR-XX: Short Ticket Title

Status: In Progress

Updated: YYYY-MM-DD

PR: TBD

Branch: `codex/prxx-short-ticket-title`

Depends on: none

Description:
- R1: State one required behavior or deliverable.
- R2: State one required behavior or deliverable.

Acceptance:
- A1 (verifies R1): State the observable condition proving R1.
- A2 (verifies R2): State the observable condition proving R2.
```

The number and IDs of `Description` and `Acceptance` items must match exactly. Broad `Goal`, `Scope`, or prose
sections may provide context, but they never replace the paired requirements and checks.

## Status Semantics And Working-Tree Policy

`Merged` on PR-01 through PR-18 means that the contract, transformation code, and focused tests are in `main`.
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

Status: Merged

Updated: 2026-07-25

PR: PR #58: https://github.com/SergejSchweizer/crypto-history-loader/pull/58

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

Status: Merged

Updated: 2026-07-25

PR: PR #59: https://github.com/SergejSchweizer/crypto-history-loader/pull/59

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

Status: Merged

Updated: 2026-07-25

PR: PR #60: https://github.com/SergejSchweizer/crypto-history-loader/pull/60

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

Status: Merged

Updated: 2026-07-25

PR: PR #61: https://github.com/SergejSchweizer/crypto-history-loader/pull/61

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

Status: Merged

Updated: 2026-07-25

PR: PR #62: https://github.com/SergejSchweizer/crypto-history-loader/pull/62

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

Status: Merged

Updated: 2026-07-25

PR: PR #63: https://github.com/SergejSchweizer/crypto-history-loader/pull/63

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

Status: Merged

Updated: 2026-07-25

PR: PR #64: https://github.com/SergejSchweizer/crypto-history-loader/pull/64

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

Status: Merged

Updated: 2026-07-25

PR: PR #65: https://github.com/SergejSchweizer/crypto-history-loader/pull/65

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

Status: Merged

Updated: 2026-07-25

PR: PR #66: https://github.com/SergejSchweizer/crypto-history-loader/pull/66

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

Status: Merged

Updated: 2026-07-25

PR: PR #67: https://github.com/SergejSchweizer/crypto-history-loader/pull/67

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

Status: Merged

Updated: 2026-07-25

PR: PR #68: https://github.com/SergejSchweizer/crypto-history-loader/pull/68

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

Status: Merged

Updated: 2026-07-25

PR: PR #69: https://github.com/SergejSchweizer/crypto-history-loader/pull/69

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

Status: Merged

Updated: 2026-07-25

PR: PR #70: https://github.com/SergejSchweizer/crypto-history-loader/pull/70

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

Status: Merged

Updated: 2026-07-25

PR: PR #71: https://github.com/SergejSchweizer/crypto-history-loader/pull/71

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

Status: Merged

Updated: 2026-07-25

PR: PR #72: https://github.com/SergejSchweizer/crypto-history-loader/pull/72

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

Status: Merged

Updated: 2026-07-25

PR: PR #73: https://github.com/SergejSchweizer/crypto-history-loader/pull/73

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

Status: Merged

Updated: 2026-07-25

PR: PR #74: https://github.com/SergejSchweizer/crypto-history-loader/pull/74

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

Status: Merged

Updated: 2026-07-25

PR: PR #77: https://github.com/SergejSchweizer/crypto-history-loader/pull/77

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

Status: Merged

Updated: 2026-07-25

PR: PR #79: https://github.com/SergejSchweizer/crypto-history-loader/pull/79

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

Status: Merged

Updated: 2026-07-25

PR: PR #80: https://github.com/SergejSchweizer/crypto-history-loader/pull/80

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

Status: Merged

Updated: 2026-07-25

PR: PR #81: https://github.com/SergejSchweizer/crypto-history-loader/pull/81

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

Status: Merged

Updated: 2026-07-25

PR: PR #83: https://github.com/SergejSchweizer/crypto-history-loader/pull/83; PR #92: https://github.com/SergejSchweizer/crypto-history-loader/pull/92

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

Status: Merged

Updated: 2026-07-25

PR: PR #84: https://github.com/SergejSchweizer/crypto-history-loader/pull/84; PR #93: https://github.com/SergejSchweizer/crypto-history-loader/pull/93

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

Status: Merged

Updated: 2026-07-25

PR: PR #85: https://github.com/SergejSchweizer/crypto-history-loader/pull/85; PR #94: https://github.com/SergejSchweizer/crypto-history-loader/pull/94

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

Status: Merged

Updated: 2026-07-25

PR: PR #86: https://github.com/SergejSchweizer/crypto-history-loader/pull/86; PR #95: https://github.com/SergejSchweizer/crypto-history-loader/pull/95

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

Status: Merged

Updated: 2026-07-25

PR: PR #87: https://github.com/SergejSchweizer/crypto-history-loader/pull/87; PR #96: https://github.com/SergejSchweizer/crypto-history-loader/pull/96

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

Status: Merged

Updated: 2026-07-25

PR: PR #88: https://github.com/SergejSchweizer/crypto-history-loader/pull/88; PR #97: https://github.com/SergejSchweizer/crypto-history-loader/pull/97

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

Status: Merged

Updated: 2026-07-25

PR: PR #98: https://github.com/SergejSchweizer/crypto-history-loader/pull/98

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

Status: Merged

Updated: 2026-07-25

PR: PR #99: https://github.com/SergejSchweizer/crypto-history-loader/pull/99

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

Status: Merged

Updated: 2026-07-25

PR: PR #100: https://github.com/SergejSchweizer/crypto-history-loader/pull/100

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

Status: Merged

Updated: 2026-07-25

PR: PR #101: https://github.com/SergejSchweizer/crypto-history-loader/pull/101; PR #103: https://github.com/SergejSchweizer/crypto-history-loader/pull/103

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

Status: Merged

Updated: 2026-07-25

PR: PR #104: https://github.com/SergejSchweizer/crypto-history-loader/pull/104; PR #105: https://github.com/SergejSchweizer/crypto-history-loader/pull/105

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

Status: Merged

Updated: 2026-07-25

PR: PR #106: https://github.com/SergejSchweizer/crypto-history-loader/pull/106

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

Status: Merged

Updated: 2026-07-25

PR: PR #107: https://github.com/SergejSchweizer/crypto-history-loader/pull/107; PR #108: https://github.com/SergejSchweizer/crypto-history-loader/pull/108

Branch: `codex/pr34-live-gold-feature-contract`

Depends on: PR-31

Goal:
Expose live-loader index, surface, L2, trade, and metadata data through feature schemas compatible with
historical inference inputs.

Scope:
- Add `gold.live.volatility_features.m1` and `gold.live.microstructure_features.m1` as explicit live-origin
  feature contracts.
- Keep future live regime and instrument-universe additions as extensions of the canonical
  `gold.live.full.m1` endpoint rather than separate primary Gold endpoints.
- Preserve live source lineage, `as_of`, freshness, coverage, and availability flags.
- Do not backfill live data with historical values inside a live dataset.

Acceptance:
- Historical/live overlapping features have identical names, units, timestamp semantics, and null rules.
- Live artifacts remain clearly marked as snapshot-derived and source-repository-specific.

### PR-35: Historical Full Gold Dataset

Status: Merged

Updated: 2026-07-25

PR: PR #110: https://github.com/SergejSchweizer/crypto-history-loader/pull/110

Branch: `codex/pr35-historical-full-gold-dataset`

Depends on: PR-33

Goal:
Create one historical Gold dataset that joins every raw historical dataset family fetched by
`crypto-history-loader` into Bronze. Research-derived IV/RV, volatility, L2, index, futures-summary,
option-surface, strategy, target, and label features stay in narrower Gold contracts.

Scope:
- Add `gold.market.history_full.m1` as the complete raw-history join.
- Required historical sources:
  - `spot_ohlcv`
  - `perps_ohlcv`
  - `funding_1m_feature`
  - `open_interest_1m_feature`
  - `perps_trades_1m_feature`
  - `options_trades_1m_feature`
- Build the Gold minute grid from the union of those historical source timestamps.
- Keep missing source values nullable; do not shrink the dataset to a date intersection.
- Keep realized-volatility, IV/RV, volatility-index, L2, index, futures-summary, option-surface,
  strategy, target, and label columns out of this dataset.
- Update complete-run commands, parser compatibility tests, and README inventory docs.

Acceptance:
- One deterministic row per `exchange/symbol/timestamp_m1` on the historical minute grid.
- Every joined source column has explicit prefixing, timestamp semantics, null policy, and availability flag.
- Leakage tests prove forward-looking targets are absent from the inference-safe output.
- Manifest reports row count, source coverage, start/end, observed days, missing days, source hashes, and
  builder commit.
- Intermediate PR runs only the focused historical full-Gold tests.

### PR-36: Live Full Gold Dataset

Status: Merged

Updated: 2026-07-25

PR: PR #112: https://github.com/SergejSchweizer/crypto-history-loader/pull/112

Branch: `codex/pr36-live-full-gold-dataset`

Depends on: PR-34, PR-35

Goal:
Create one live-origin Gold dataset that joins every live-loader-derived Silver/Gold feature family into a
single inference table compatible with the historical full dataset where semantics overlap.

Scope:
- Add `gold.live.full.m1` as the complete live feature join.
- Required live sources:
  - `volatility_index_1m_feature`
  - `iv_rv_1m_feature`
  - `perps_l2_1m_feature`
  - `options_l2_1m_feature`
- Optional live Silver sources remain nullable with explicit availability/freshness fields:
  - `index_price_1m_feature`
  - `futures_summary_1m_feature`
  - `options_surface_1m_feature`
- Keep future live regime and instrument-universe additions as extensions of `gold.live.full.m1`, not as
  separate primary Gold endpoints.
- Do not backfill live gaps from historical datasets; live missing minutes stay null and are represented by
  coverage, freshness, and source-availability fields.
- Align overlapping column names, units, and null semantics with `gold.market.history_full.m1`.
- Update complete-run commands, parser compatibility tests, and README inventory docs.

Acceptance:
- One deterministic row per live `exchange/symbol/timestamp_m1` where the live minute grid exists.
- Existing live-loader-derived feature families are represented directly or through documented availability flags.
- Historical/live schema compatibility tests pass for overlapping feature families.
- Manifest reports origin repository, source coverage, freshness, start/end, observed days, missing days,
  source hashes, and builder commit.
- Intermediate PR runs only focused live full-Gold tests.

### PR-37: Gold Inventory Documentation And Release Gate

Status: Merged

Updated: 2026-07-25

PR: PR #114: https://github.com/SergejSchweizer/crypto-history-loader/pull/114

Branch: `codex/pr37-gold-inventory-contract-gate`

Depends on: PR-29, PR-33, PR-34, PR-35, PR-36

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

### PR-38: Fixed Gold Dataset Retention

Status: Merged

Updated: 2026-07-25

PR: PR #113: https://github.com/SergejSchweizer/crypto-history-loader/pull/113

Branch: `codex/gold-retention-three-versions`

Depends on: PR-36

Goal:
Keep Gold storage bounded and deterministic by retaining exactly the latest three artifact versions for each
`dataset_id/exchange/symbol` lineage.

Scope:
- Enforce a fixed Gold retention window of three versions in the Gold build service.
- Keep the legacy `--retention-keep-versions` CLI argument parseable, but reject any value other than `3`.
- Update README retention policy and focused regression tests.

Acceptance:
- Gold pruning always runs with a retention window of `3`.
- CLI/config/direct service callers fail clearly when they request any other retention window.
- Backlog and README describe fixed Gold retention.

## Refactor Hardening Stack

The 2026-07-12 repository rescan found five refactor issues that create the largest future maintenance risk:
handwritten Silver command routing, duplicated Silver monthly IO/report plumbing, a broad Gold frame-preparation
module, repeated dataset/config/CLI lists, and oversized monkeypatch-heavy tests. The following stack is atomic,
idempotent, and behavior-preserving; each PR must keep existing public commands and dataset contracts compatible.

### PR-39: Silver Build Registry Extraction

Status: Merged

Updated: 2026-07-25

PR: PR #120: https://github.com/SergejSchweizer/crypto-history-loader/pull/120

Branch: `codex/pr39-silver-build-registry`

Depends on: PR-38

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
clean
```

Goal:
Replace the handwritten `silver-build` handler/discovery cascade in `api/commands/silver.py` with a typed dataset
build registry that keeps dataset choices, discovery, builder functions, output dataset names, and sidecar reporting
in one inspectable contract.

Scope:
- Add a typed `SilverBuildSpec` registry in the application layer or a command-adjacent module with no new storage side effects.
- Move per-dataset discovery selection and handler wiring out of the long `run_silver_build` branch cascade.
- Keep current CLI arguments, JSON output shape, logging fields, and sidecar behavior unchanged.
- Add focused route tests that compare every existing dataset choice against exactly one registry entry.

Acceptance:
- `silver-build --dataset ...` schedules the same jobs and reports as before for every current dataset.
- Adding a Silver dataset requires one registry entry plus tests, not edits to multiple `elif` chains.
- Re-running the same command with unchanged inputs is idempotent and produces the same target paths and report metadata.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### PR-40: Shared Silver Monthly IO And Report Kernel

Status: Merged

Updated: 2026-07-25

PR: PR #121: https://github.com/SergejSchweizer/crypto-history-loader/pull/121

Branch: `codex/pr40-silver-monthly-io-kernel`

Depends on: PR-39

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
clean
```

Goal:
Extract the repeated Silver monthly read, deterministic write, timestamp-span, duplicate-count, and
`SilverBuildReport` aggregation patterns from `application/services/silver_service.py` and the `silver_*` builders
into one reusable monthly build kernel.

Scope:
- Define a typed monthly build result object that owns rows in/out, duplicate counts, invalid counts, timestamp span,
  months processed, output columns, and target path.
- Move shared parquet path creation, month iteration, report aggregation, and UTC formatting behind explicit helpers.
- Migrate OHLCV and one existing observed/feature pair first; leave adapters for the remaining builders to preserve behavior.
- Keep all existing dataset paths, partition names, row ordering, and deduplication keys stable.

Acceptance:
- Focused Silver service tests prove migrated datasets write byte-equivalent schemas and identical report fields.
- Existing public builder functions remain import-compatible for CLI and tests.
- The new kernel has no wall-clock dependency except caller-supplied cutoffs already present in existing builders.
- Re-running the same monthly build is idempotent and rewrites only the same deterministic target files.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### PR-41: Gold Frame Preparation Registry

Status: Merged

Updated: 2026-07-25

PR: PR #122: https://github.com/SergejSchweizer/crypto-history-loader/pull/122

Branch: `codex/pr41-gold-frame-preparation-registry`

Depends on: PR-40

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
clean
```

Goal:
Split `application/services/gold_frames.py` into a registry-driven frame-preparation layer so dataset-specific
select/cast/prefix rules, optional schemas, live lineage fields, strategy feature lookbacks, and prediction target
definitions are explicit and independently testable.

Scope:
- Introduce typed preparation specs mapping Silver dataset types to prepare functions, required columns, output columns,
  optional nullable schema, and source lineage semantics.
- Move optional feature schema definitions next to their corresponding prepare specs.
- Keep `prepare_dataset_frame`, strategy feature, and prediction-target public entrypoints compatible during migration.
- Add tests that every Gold contract requirement has a registered preparation path or documented explicit exception.

Acceptance:
- Gold builds emit the same column order for `gold.market.history_full.m1`, `gold.market.regime_features.m1`,
  `gold.live.volatility_features.m1`, `gold.live.microstructure_features.m1`, and `gold.live.full.m1`.
- Optional source gaps still produce stable nullable columns and do not expand required grids.
- The registry makes unsupported dataset types fail with one deterministic error message.
- Re-running a Gold build with the same Silver inputs remains idempotent, including manifest hashes and version pruning.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### PR-42: Contract-Driven Dataset Lists And Command Choices

Status: Merged

Updated: 2026-07-25

PR: PR #123: https://github.com/SergejSchweizer/crypto-history-loader/pull/123

Branch: `codex/pr42-contract-driven-dataset-lists`

Depends on: PR-41

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
clean
```

Goal:
Remove dataset-list drift by deriving CLI choices, complete-run command validation, inventory expectations, and docs
checks from the typed dataset contracts and build registries instead of maintaining repeated literal lists.

Scope:
- Add contract helpers for supported Bronze-backed Silver build IDs, live-origin Silver build IDs, and supported Gold IDs.
- Make `silver-build` and `gold-build` parser choices consume these helpers without changing accepted command values.
- Update parser/config compatibility tests to assert config lists are subsets of contract-derived supported IDs.
- Update README/backlog inventory validators to rely on the same canonical helper where practical.

Acceptance:
- A new dataset cannot be added to contracts without either appearing in command choices or being explicitly marked as
  non-buildable with a test-covered reason.
- Complete medallion command validation fails on missing or stale dataset IDs without duplicating the full list in tests.
- Existing `config.yaml` and documented complete-run commands remain valid.
- The refactor is idempotent: canonical helpers return sorted stable sequences and do not read local lake state.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### PR-43: Typed Test Fixture And Command Harness Consolidation

Status: Merged

Updated: 2026-07-25

PR: PR #124: https://github.com/SergejSchweizer/crypto-history-loader/pull/124

Branch: `codex/pr43-typed-test-command-harness`

Depends on: PR-42

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
clean
```

Goal:
Reduce regression risk in the largest monkeypatch-heavy test modules by introducing typed fixtures and command harnesses
for Silver routing, Gold frame builds, fetch services, and parquet fixture construction.

Scope:
- Add reusable typed fixture builders for Silver reports, Gold parquet inputs, Bronze parquet partitions, and command args.
- Replace repeated `# type: ignore[no-untyped-def]` monkeypatch patterns in `tests/test_silver_command.py`,
  `tests/test_gold_service.py`, and the largest fetch-service tests with typed local helpers.
- Keep test behavior and assertions equivalent while reducing dependency on private compatibility wrappers.
- Document fixture ownership in test module docstrings or a small test helper README if needed.

Acceptance:
- Targeted Silver command, Gold service, and fetch-service tests pass with fewer untyped test ignores.
- Fixture helpers are deterministic, do not touch network resources, and write only under pytest `tmp_path` roots.
- Command tests assert behavior through public CLI/service surfaces wherever practical.
- The final stacked PR runs the complete configured quality suite plus coverage before merge readiness.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

## Quantitative Correctness Priority Stack

This stack was merged from `docs/backlog/quant-correctness-priority-stack.md` (recorded during the 2026-07-17
repository review). Data correctness takes precedence over structural cleanup.

Priority order:

1. QC-01: Normalize implied- and realized-volatility semantics.
2. QC-02: Preserve rolling state across monthly partitions.
3. QC-03: Prevent row-wise spot/perpetual source switching in realized volatility.
4. QC-04: Add quantitative semantics to dataset contracts.
5. QC-05: Validate documented CLI commands as executable contracts.
6. QC-06: Align documented and enforced quality gates.

### QC-01: Normalize IV/RV Units, Horizons, And Annualization

Priority: P0 - data correctness blocker

Status: Merged

Updated: 2026-07-25

PR: PR #140: https://github.com/SergejSchweizer/crypto-history-loader/pull/140

Branch: `codex/qc01-normalize-iv-rv-semantics`

Depends on: none

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
 M ARCHITECTURE.md
 M BACKLOG.md
 M README.md
 M application/dataset_contracts.py
 M application/services/gold_frames.py
 M application/services/silver_iv_rv.py
 M application/services/silver_realized_volatility.py
 M application/services/silver_volatility.py
 M ingestion/feature_metadata.py
 M tests/test_gold_live_full.py
 M tests/test_gold_live_volatility.py
 M tests/test_gold_regime_features.py
 M tests/test_gold_service.py
 M tests/test_silver_iv_rv.py
 M tests/test_silver_realized_volatility.py
```

Problem:
`volatility_index_1m_feature.iv_close` represents an annualized implied-volatility index in percentage points,
while `realized_volatility_1m_feature.rv_1h` and `rv_1d` currently represent non-annualized square-root sums of
squared decimal log returns. Direct subtraction and division therefore mix incompatible units and horizons in
expressions such as `iv_minus_rv_1h = iv_close - rv_1h` and `iv_rv_ratio_1h = iv_close / rv_1h`.

Goal:
Make every IV/RV comparison financially interpretable and encode the convention in contracts, manifests, tests,
and column names.

Scope:
- Define the canonical IV unit as annualized volatility percentage points.
- Define the canonical annualization basis explicitly, with crypto calendar-time defaulting to 365 days unless a
  contract states otherwise.
- Add annualized realized-volatility fields with explicit names, for example `rv_1h_annualized_pct`,
  `rv_1d_annualized_pct`, `rv_30d_annualized_pct`.
- Prefer a horizon-compatible 30-day realized-volatility comparison for the volatility index:
  `rv_30d_annualized_pct = sqrt(sum(last_30d_log_returns^2)) * sqrt(365 / 30) * 100`,
  `iv_rv_spread_30d_pct = iv_30d_annualized_pct - rv_30d_annualized_pct`,
  `iv_rv_ratio_30d = iv_30d_annualized_pct / rv_30d_annualized_pct`.
- Either remove ambiguous `iv_minus_rv_1h`, `iv_minus_rv_1d`, `iv_rv_ratio_1h`, and `iv_rv_ratio_1d` fields or
  version them with precisely documented semantics.
- Update Silver and Gold contracts, manifests, README tables, architecture documentation, and rebuild notes.
- Add a migration or compatibility policy for existing materialized artifacts.

Out of scope:
- Do not tune trading thresholds or model hyperparameters.
- Do not create forward-looking labels.
- Do not silently reinterpret existing persisted columns without a schema/version change.

Acceptance:
- Every IV and RV output declares unit, horizon, annualization status, annualization basis, and estimator.
- Unit tests use realistic decimal-return and volatility-index values rather than treating values such as `10.0`
  as a one-hour decimal RV.
- A deterministic reference test verifies annualization formulas against hand-calculated results.
- Division-by-zero and insufficient-history policies are explicit and tested.
- Gold feature-only datasets expose no mixed-unit IV/RV subtraction or ratio.
- A rebuild note identifies every affected Silver and Gold dataset.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### QC-02: Preserve Rolling State Across Monthly Partitions

Priority: P0 - data correctness blocker

Status: Merged

Updated: 2026-07-25

PR: PR #141: https://github.com/SergejSchweizer/crypto-history-loader/pull/141

Branch: `codex/qc02-cross-month-rolling-state`

Depends on: QC-01

Problem:
Rolling returns, realized volatility, z-scores, changes, and percentiles are calculated independently inside
monthly processing loops. At the beginning of each month, the calculation loses the previous close and all
required trailing observations. Storage partition boundaries therefore alter feature values. Affected feature
families include at least `realized_volatility_1m_feature`, `volatility_index_1m_feature`, and
`iv_rv_1m_feature`; potentially affected calculations include the first return of each month, `5m`/`15m`/`1h`/
`4h`/`1d` RV windows, `1d`/`7d` z-scores, `30d` percentiles, IV change windows, jump proxies, and any downstream
Gold rolling features.

Goal:
Make feature values invariant to monthly storage partitioning.

Scope:
- Declare the maximum required lookback for each builder or feature family.
- Load sufficient prior-partition context before calculating a target month.
- Calculate on the buffered frame, then trim output back to the requested target month.
- Preserve the previous valid close across month and year boundaries.
- Centralize buffered monthly reads in a reusable helper rather than implementing one-off overlap logic in each
  builder.
- Keep writes monthly and deterministic.
- Add manifest metadata recording the calculation lookback used.

Recommended processing pattern:
`calculation_start = target_month_start - required_lookback`; calculate on
`[calculation_start, output_end]`; write only `[output_start, output_end]`.

Out of scope:
- Do not change partition layout.
- Do not add future observations or centered windows.
- Do not use forward fills that cross source-availability rules.

Acceptance:
- Regression tests cover January 31 to February 1 and December 31 to January 1.
- Building one long unpartitioned fixture and building the same fixture month-by-month produce equal values for
  all target-month rows.
- The first valid minute of a month uses the final valid prior-month close when the contract permits it.
- A `30d` percentile on the first day of a month includes eligible prior-month observations.
- Re-running a month with unchanged inputs produces byte-stable or value-stable deterministic output according
  to the existing storage contract.
- Affected historical artifacts are explicitly marked for rebuild.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### QC-03: Separate Spot And Perpetual RV Sources

Priority: P0 - data correctness blocker

Status: Merged

Updated: 2026-07-25

PR: PR #141: https://github.com/SergejSchweizer/crypto-history-loader/pull/141

Branch: `codex/qc03-separate-spot-perp-rv`

Depends on: QC-02

Problem:
The current realized-volatility builder creates one synthetic price stream by coalescing perpetual OHLCV over
spot OHLCV row by row. If a perpetual minute is absent, the stream can switch to spot and then switch back. The
spot-perpetual basis is then misclassified as a price return, contaminating RV and jump features.

Goal:
Prevent source switching from producing artificial returns while retaining explicit source availability.

Scope:
- Calculate spot and perpetual returns and RV features as separate source families, for example `spot_rv_1h`,
  `spot_rv_1d`, `spot_rv_30d_annualized_pct`, `perps_rv_1h`, `perps_rv_1d`, `perps_rv_30d_annualized_pct`.
- Define one canonical IV/RV comparison source, preferably through an explicit contract or configuration rather
  than row-wise fallback.
- If a stitched canonical series remains necessary, require an explicit basis-adjusted stitching method and emit
  source-transition flags.
- Preserve `spot_available` and `perps_available`, and add source identity fields where a canonical RV is
  published.
- Add data-quality counters for source gaps and attempted source transitions.

Out of scope:
- Do not hide missing perpetual data by silently substituting spot.
- Do not treat the spot-perpetual basis as ordinary underlying return.
- Do not remove either source family from research outputs.

Acceptance:
- A regression fixture with alternating spot/perpetual availability produces no artificial basis jump in either
  source-specific return series.
- Source-specific RV values match independent hand calculations.
- Canonical IV/RV features declare which RV source is used.
- Missing canonical-source observations remain explicitly unavailable unless a documented stitching policy
  applies.
- Gold manifests report source availability and the canonical RV source policy.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### QC-04: Extend Dataset Contracts With Quantitative Semantics

Priority: P1 - contract integrity

Status: Merged

Updated: 2026-07-25

PR: PR #141: https://github.com/SergejSchweizer/crypto-history-loader/pull/141

Branch: `codex/qc04-quant-semantic-contracts`

Depends on: QC-01 through QC-03

Goal:
Extend typed dataset contracts beyond column shape so economically meaningful fields cannot be combined without
explicit semantic metadata.

Scope:
- For quantitative feature fields or feature families, add typed metadata for unit (decimal, percentage points,
  price, quantity, notional, count, or dimensionless), horizon or tenor, annualized flag, annualization basis,
  estimator or construction method, required lookback, source-selection policy, and null/insufficient-history
  policy.
- Add contract tests proving IV/RV comparisons use compatible semantics.
- Emit relevant metadata into Silver and Gold manifests.

Acceptance:
- A contract test fails when an IV/RV spread attempts to combine incompatible units or horizons.
- All volatility feature families declare their estimator, horizon, unit, and annualization convention.
- Required lookbacks are machine-readable and used by buffered partition reads.
- Documentation is generated from or validated against the canonical contracts where practical.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### QC-05: Validate Documentation Commands As Executable Contracts

Priority: P1 - operational correctness

Status: Merged

Updated: 2026-07-25

PR: PR #141: https://github.com/SergejSchweizer/crypto-history-loader/pull/141

Branch: `codex/qc05-executable-doc-commands`

Depends on: none

Problem:
README examples can drift from the actual parser surface. A documented Bronze command may use a stale argument
name while the parser exposes a different canonical flag.

Scope:
- Correct stale README command examples.
- Extract or represent canonical example argument vectors in a testable form.
- Add parser-level tests proving documented commands are accepted.
- Keep examples synchronized with dataset registry choices and configuration aliases.

Acceptance:
- Every canonical README command parses successfully without network or lake access.
- CI fails when a documented flag or dataset choice is removed without updating documentation.
- README, `config.yaml`, and parser choices use the same canonical vocabulary.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### QC-06: Align Documented And Enforced Quality Gates

Priority: P2 - governance consistency

Status: Merged

Updated: 2026-07-25

PR: PR #141: https://github.com/SergejSchweizer/crypto-history-loader/pull/141

Branch: `codex/qc06-align-quality-gates`

Depends on: none

Problem:
`AGENTS.md`, architecture documentation, pre-commit configuration, and GitHub Actions do not currently describe
exactly the same quality-gate suite. In particular, documented docstring tools must either be enforced or
removed from the mandatory policy.

Scope:
- Inventory mandatory checks declared by `AGENTS.md`, `ARCHITECTURE.md`, `.pre-commit-config.yaml`, `Makefile`,
  and `.github/workflows/ci.yml`.
- Choose one canonical enforced suite.
- Add missing tools such as `interrogate` and `pydoclint` only if they are intentionally mandatory.
- Otherwise revise policy text so it matches the enforced suite.
- Add a lightweight consistency test for named mandatory checks where practical.

Acceptance:
- Local `make check`, pre-commit, and CI perform the same logical mandatory checks.
- No tool is described as mandatory without being enforced.
- CI remains the final merge-readiness authority.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### Required Rebuild And Release Gate (QC-01 Through QC-03)

QC-01 through QC-03 change historical feature values and require a controlled rebuild. Before downstream
research or model training treats the outputs as corrected: rebuild affected Silver volatility datasets;
rebuild dependent Gold datasets; publish schema or feature-set version changes; compare old and corrected
distributions; document expected discontinuities; verify no reusable feature dataset contains forward-looking
labels; record the effective corrected-data start/version in manifests.

QC-01 rebuild note: QC-01 adds new columns (`iv_30d_annualized_pct`; `rv_5m_annualized_pct`,
`rv_15m_annualized_pct`, `rv_1h_annualized_pct`, `rv_4h_annualized_pct`, `rv_1d_annualized_pct`, `rv_30d`,
`rv_30d_annualized_pct`; `iv_rv_spread_30d_pct`, `iv_rv_ratio_30d`) without deleting or recomputing any
existing column, so previously materialized `volatility_index_1m_feature`, `realized_volatility_1m_feature`,
and `iv_rv_1m_feature` Silver artifacts must be rebuilt to backfill the new columns before Gold contracts that
select those columns (`gold.market.regime_features.m1`, `gold.live.volatility_features.m1`) can expose them;
existing legacy columns (`iv_minus_rv_1h`, `iv_minus_rv_1d`, `iv_rv_ratio_1h`, `iv_rv_ratio_1d`) are unchanged
and require no rebuild for consumers that only read those fields.

## Refactor Architecture Stack

This stack captures the next three highest-value refactor topics after rereading the repository on 2026-07-16:

1. Bronze loader boundary cleanup.
2. Silver builder registry and monthly IO consolidation.
3. Gold frame preparation split with stronger contracts and typing.

The stack is intentionally ordered from operational boundary risk to downstream feature correctness. Each PR must
preserve existing CLI behavior, public dataset names, partition layouts, report fields, manifest hashes, and
backward-compatible reads unless a later PR explicitly documents a migration.

### PR-44: Bronze Build Request And Result Contracts

Status: Merged

Updated: 2026-07-25

PR: PR #134: https://github.com/SergejSchweizer/crypto-history-loader/pull/134

Branch: `codex/pr44-bronze-build-contracts`

Depends on: PR-43

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
?? application/bronze_contracts.py
?? tests/test_bronze_contracts.py
```

Goal:
Introduce typed Bronze build input and output contracts so the CLI, compatibility wrappers, runtime services,
checkpoint handling, and fetch execution no longer exchange implicit mutable module state.

Scope:
- Add `BronzeBuildRequest`, `BronzeRuntimeContext`, `BronzeDatasetSelection`, and `BronzeBuildResult` types in the
  application layer.
- Keep existing command-line flags, default values, debug behavior, JSON output shape, lock paths, checkpoint keys,
  and Bronze write locations unchanged.
- Add conversion helpers from parsed CLI args to `BronzeBuildRequest` without moving execution logic yet.
- Add focused tests proving current CLI argument combinations produce deterministic request objects.
- Document every field that is wall-clock-sensitive, config-derived, environment-derived, or dataset-derived.

Out of scope:
- Do not remove compatibility wrappers yet.
- Do not change fetch execution order, retry behavior, checkpoint writes, or lake writes.
- Do not change public command names or aliases.

Acceptance:
- The same current Bronze CLI invocations build equivalent requests across repeated runs when config and args match.
- Tests cover at least default loader execution, explicit dataset selection, explicit time bounds, symbol filters,
  debug behavior, dry-run/report-only behavior where supported, and config/env override precedence.
- New contracts are fully typed and do not import `api`.
- `api` depends on the contracts; application contracts do not depend on CLI parser internals.
- Existing Bronze tests continue to pass without updating lake fixtures.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### PR-45: Bronze Runtime Adapter Without Module-Global Mutation

Status: Merged

Updated: 2026-07-25

PR: PR #135: https://github.com/SergejSchweizer/crypto-history-loader/pull/135

Branch: `codex/pr45-bronze-runtime-adapter`

Depends on: PR-44

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
 M api/cli.py
 M api/commands/loader.py
 M application/services/bronze_runtime_service.py
 M tests/test_bronze_runtime_service.py
 M tests/test_loader_command.py
```

Goal:
Replace CLI-to-loader module-global synchronization with an explicit runtime adapter while preserving the old
test-facing compatibility surface during the transition.

Scope:
- Introduce a `BronzeRuntimeAdapter` or equivalent typed dependency object that carries runtime bounds, fetch hooks,
  clock hooks, config aliases, lock policy, and checkpoint policy.
- Route `api/cli.py` and `api/commands/loader.py` through the adapter instead of mutating imported module globals.
- Keep existing private compatibility functions importable for tests, but make them delegate to the adapter.
- Remove or shrink direct synchronization helpers that copy values between CLI and loader modules.
- Add regression tests that monkeypatch the old compatibility surface and prove behavior is still preserved.

Out of scope:
- Do not split the main build workflow yet.
- Do not change task planning, checkpoint semantics, or persistence behavior.
- Do not change logs except for adding deterministic adapter-identification fields where useful.

Acceptance:
- There is one explicit runtime object per command invocation.
- Parallel or repeated command invocations in the same Python process do not share mutable runtime overrides except
  through explicitly passed dependencies.
- Existing CLI and loader compatibility tests pass.
- New tests prove old monkeypatch entrypoints still route to the new adapter.
- No application-layer module imports `api`.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

### PR-46: Bronze Workflow Stage Split

Status: Merged

Updated: 2026-07-25

PR: PR #137: https://github.com/SergejSchweizer/crypto-history-loader/pull/137

Branch: `codex/pr46-bronze-workflow-stages`

Depends on: PR-45

Planning `git status --short`:

```text
clean
```

Publication `git status --short`:

```text
M  api/commands/loader.py
M  api/commands/loader_execution.py
M  api/commands/loader_workflow.py
M  tests/test_loader_command.py
M  tests/test_loader_execution.py
```

Scope note: the Bronze build workflow was already substantially stage-split before this PR
(`loader_planning.py`, `loader_checkpoint.py`, `loader_bounds.py`, `loader_execution.py`,
`loader_output.py`, each with dedicated no-network stage-level tests, coordinated by
`loader_workflow.py::run_bronze_build` via `BronzeWorkflowDependencies`). The remaining concrete
gap this PR closes: `loader_execution.py::fetch_all_task_groups` returned a
`FetchAllResult8 | FetchAllResult10` tuple union, disambiguated by the coordinator via a fragile
`len(fetch_results) == 8` arity check (the 8-tuple branch was dead in production; only exercised by
test doubles). Replaced with a single explicit, stably-ordered `FetchAllTaskGroupsResult` dataclass
(10 named fields) always returned from the execution stage, removing the arity-sniffing branch from
`run_bronze_build` entirely.

Goal:
Split the Bronze build workflow into deterministic stages with explicit inputs and outputs so checkpointing,
locking, planning, execution, persistence, and reporting can be tested independently.

Scope:
- Extract stage functions or small services for:
  - request validation and normalization
  - lock acquisition plan
  - checkpoint hydration
  - fetch task planning
  - task execution
  - incremental persistence
  - final checkpoint/report construction
- Define stage result types with stable ordering and explicit side-effect ownership.
- Keep the existing top-level command workflow as a thin coordinator.
- Add tests for each stage using deterministic in-memory or `tmp_path` fixtures.
- Preserve all existing Bronze report fields and JSON output.

Out of scope:
- Do not introduce a new scheduler.
- Do not change concurrency defaults.
- Do not change lake partition layout, dedup keys, checkpoint keys, or raw record schema.

Acceptance:
- A no-network stage-level test can validate task planning from a fixed request/config pair.
- A no-network stage-level test can validate checkpoint decisions from fixed checkpoint inputs.
- The full Bronze command still writes the same deterministic target paths for the same inputs.
- Failure handling remains observable and does not silently swallow fetch, checkpoint, or persistence errors.
- The top-level workflow becomes a coordinator over named stage contracts rather than a monolithic implementation.
- `git status --short` and the PR URL are recorded in this backlog entry before handoff.

## Performance Delivery Rules

PR-54 through PR-61 are a separate performance stack. They must be implemented in order and must not be
combined into one broad optimization PR. Every ticket in this stack has the following mandatory properties:

- **Idempotent:** rerunning with identical source fingerprints produces no changed data files, no duplicate rows,
  and no version churn. A changed input produces exactly one replacement for the affected output partition.
- **Atomic:** write to a uniquely named temporary path in the same filesystem, flush and close it, validate the
  artifact, then publish with an atomic rename. A failed build must leave the last valid artifact and its manifest
  untouched. Temporary paths must be cleaned only after the failure is logged.
- **Deterministic:** use explicit source fingerprints, stable partition keys, stable sort keys, explicit deduplication
  keys, fixed schemas, and UTC timestamps. Wall-clock time may appear only in operational metadata.
- **Bounded:** preserve the repository-wide maximum of four Polars threads and four application workers. No ticket
  may add nested unbounded executors or silently increase `maxprocesses`.
- **Observable:** log one structured event for `planned`, `skipped_unchanged`, `built`, `published`, and `failed`
  with layer, dataset, exchange, symbol, partition, source fingerprint, row count, and elapsed milliseconds.
- **Backward compatible:** old artifacts without a performance manifest remain readable and are treated as needing
  one rebuild; no existing canonical dataset ID or column contract is removed.
- **Rollback-safe:** publication must be recoverable by retaining the previous valid manifest until the new manifest
  is validated. No ticket may delete lake data as part of a normal optimization run.

Each PR must include a focused benchmark fixture and report before/after wall time, rows processed, bytes read,
bytes written, peak memory where available, and the number of skipped partitions. A speedup without correctness,
idempotency, and atomic-publication evidence is not an acceptable result.

## Performance PR Stack

### PR-54: Medallion Performance Benchmark And Stage Telemetry

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/159

Branch: `codex/pr54-medallion-performance-benchmark`

Depends on: none

Description:
- R1: Add a deterministic, read-only benchmark command or test fixture covering representative Bronze, Silver, and Gold symbol/month inputs without touching the production lake.
- R2: Measure stage, dataset, symbol, and partition timings together with rows in/out, bytes read/written, worker count, and Polars thread count.
- R3: Add structured performance log events for planned, skipped, built, published, and failed work without changing dataset contents.
- R4: Record baseline measurements for the current full and representative incremental workloads in a versioned benchmark report.
- R5: Preserve the existing CLI contracts, log root, and four-core limit.
- R6: Record the exact planning and publication `git status --short` output and PR URL in this ticket.

Out of scope:
- No incremental processing or cache behavior.
- No schema, partition-layout, or dataset-ID changes.
- No production-lake writes from the benchmark.

Acceptance:
- A1 (verifies R1): The benchmark runs twice against the same fixture and leaves the production lake unchanged.
- A2 (verifies R2): The report contains all R2 fields and distinguishes Bronze, Silver, and Gold timings.
- A3 (verifies R3): Log tests assert all five event types and required context fields.
- A4 (verifies R4): A checked-in baseline report contains reproducible commands, fixture size, and measured values.
- A5 (verifies R5): CLI compatibility tests pass and telemetry reports no more than four Polars threads and workers.
- A6 (verifies R6): The ticket contains the exact clean status output and final PR URL before merge.

### PR-55: Silver Source Fingerprint Manifests And No-Op Detection

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/160

Branch: `codex/pr55-silver-source-fingerprint-manifests`

Depends on: PR-54

Description:
- R1: Define a versioned Silver input fingerprint over the exact Bronze files, file metadata/content identity, source schema, exchange, symbol, timeframe, and builder contract version.
- R2: Write one manifest per Silver output partition containing the input fingerprint, output fingerprint, schema signature, row count, sort/dedup contract, and build status.
- R3: Skip an unchanged Silver partition before loading its data and emit `skipped_unchanged` telemetry.
- R4: Treat missing, malformed, incompatible, or legacy manifests as cache misses and rebuild the affected partition.
- R5: Publish data and manifest atomically so a crash cannot expose a new manifest with an old or missing parquet artifact.
- R6: Record publication evidence in this ticket before handoff.

Out of scope:
- No change to Silver feature math or canonical dataset names.
- No cross-partition incremental windowing yet; that is PR-56.
- No deletion of legacy Silver artifacts.

Acceptance:
- A1 (verifies R1): Identical Bronze inputs produce identical fingerprints across two independent processes.
- A2 (verifies R2): Manifest contract tests validate every required field and reject unknown status values.
- A3 (verifies R3): A second unchanged build reads zero source rows for the skipped partition and preserves file hashes.
- A4 (verifies R4): Tests prove legacy and corrupt manifests trigger exactly one rebuild rather than silent skipping.
- A5 (verifies R5): Failure-injection tests prove the previous valid artifact remains readable after data or manifest publication failure.
- A6 (verifies R6): The ticket contains the exact clean status output and final PR URL before merge.

Publication evidence:
- Pull request: https://github.com/SergejSchweizer/crypto-history-loader/pull/160
- `git status --short`: *(empty)*
- GitHub required checks: `pr-lint-quality`, `pr-typing-quality`, all four unit shards, all four integration shards,
  `pr-coverage-95`, and `pr-quality` passed on 2026-08-02.

### PR-56: Silver Incremental Monthly Partitions And Lookback Windows

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/161

Branch: `codex/pr56-silver-incremental-monthly-builds`

Depends on: PR-55

Description:
- R1: Plan Silver work at the smallest safe partition boundary, using changed Bronze months and a configurable feature lookback window rather than rescanning the complete history.
- R2: Recompute the changed partition plus the minimum preceding lookback required by each rolling, resampling, forward-fill, or gap-tracking operation.
- R3: Replace affected Silver partitions atomically and preserve unaffected partition hashes and manifests.
- R4: Keep minute-gap and zero-minute tracking semantics correct across partition boundaries, including the first and last minute of each rebuilt partition.
- R5: Make retries and interrupted runs restart-safe: a retry must converge to the same output as one successful run.
- R6: Record publication evidence in this ticket before handoff.

Out of scope:
- No Gold changes.
- No reduction of the configured lookback below the documented feature dependency.
- No automatic deletion or compaction of historical partitions.

Acceptance:
- A1 (verifies R1): A fixture with one changed Bronze month plans only that month and documented dependency months.
- A2 (verifies R2): Boundary tests prove rolling windows, resampling, forward-fill, and zero-minute tracking match a full rebuild.
- A3 (verifies R3): Unaffected Silver partition hashes and manifests remain byte-identical after an incremental build.
- A4 (verifies R4): Tests cover month transitions, empty Deribit minutes, and perps/options trade zero-minute rows.
- A5 (verifies R5): Injected interruption followed by retry produces the same partition set, rows, schemas, and manifests as a clean run.
- A6 (verifies R6): The ticket contains the exact clean status output and final PR URL before merge.

### PR-57: Silver Shared Source Scan And Dependency Planner

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/162

Branch: `codex/pr57-silver-shared-source-planner`

Depends on: PR-56

Description:
- R1: Build a typed Silver dependency graph separating source-backed, derived, and sidecar work.
- R2: Reuse one bounded lazy Bronze scan or normalized intermediate frame when multiple Silver outputs consume the same symbol/month source.
- R3: Schedule independent work with at most four application workers and execute derived datasets only after their declared inputs are published.
- R4: Preserve memory bounds by evicting intermediates at partition boundaries and never retaining the complete historical lake in a global cache.
- R5: Keep output bytes, schemas, sort keys, dedup keys, manifests, and lineage identical to the pre-planner implementation.
- R6: Record publication evidence in this ticket before handoff.

Out of scope:
- No new Silver dataset families.
- No Gold source-cache implementation.
- No global process pool or unbounded in-memory cache.

Acceptance:
- A1 (verifies R1): A graph test rejects missing dependencies and cycles and lists a deterministic execution order.
- A2 (verifies R2): Instrumented tests show duplicate source scans are eliminated for the selected shared-input families.
- A3 (verifies R3): Scheduler tests prove dependency ordering and a maximum of four workers.
- A4 (verifies R4): Stress fixtures demonstrate bounded intermediate lifetime and no complete-history cache.
- A5 (verifies R5): Golden-file tests prove unchanged outputs and lineage against the pre-planner fixture.
- A6 (verifies R6): The ticket contains the exact clean status output and final PR URL before merge.

### PR-58: Gold Input Fingerprints And Incremental M1 Publication

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/163

Branch: `codex/pr58-gold-incremental-m1-publication`

Depends on: PR-57

Description:
- R1: Define Gold source fingerprints over all required and optional Silver inputs, source manifests, Gold contract version, and feature configuration.
- R2: Plan Gold `m1` work by changed Silver partitions and the minimum feature lookback needed by each Gold feature family.
- R3: Rebuild only affected symbol/partition outputs and atomically publish the new canonical and extended `m1` artifacts.
- R4: Preserve canonical-versus-extended dataset separation and all existing Gold column selection, target, and leakage contracts.
- R5: Make missing optional sources explicit in the fingerprint and keep their nullable output columns stable.
- R6: Record publication evidence in this ticket before handoff.

Out of scope:
- No Gold `m5`, `m30`, or `h1` fan-out yet; that is PR-59.
- No removal of existing Gold versions or manifests.
- No change to target lookahead semantics.

Acceptance:
- A1 (verifies R1): Identical Silver inputs and configuration yield identical Gold source fingerprints.
- A2 (verifies R2): A fixture with one changed Silver month plans only that month plus declared feature lookback partitions.
- A3 (verifies R3): Unchanged Gold partitions retain hashes; changed partitions are published exactly once after validation.
- A4 (verifies R4): Canonical/extended schema, dataset-ID, and leakage regression tests pass unchanged.
- A5 (verifies R5): Optional-source availability changes are detected and produce deterministic nullable columns without grid expansion.
- A6 (verifies R6): The ticket contains the exact clean status output and final PR URL before merge.

### PR-59: Gold Shared M1 Preparation And Multi-Timeframe Fan-Out

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/165

Branch: `codex/pr59-gold-shared-timeframe-fanout`

Depends on: PR-58

Handoff status: `git status --short` produced no output after merge verification on 2026-08-02.

Description:
- R1: Prepare each symbol's Gold `m1` source frame and common joins once per build transaction.
- R2: Derive `m5`, `m30`, and `h1` from the validated `m1` frame in one deterministic fan-out while preserving each dataset contract.
- R3: Publish all sibling timeframe artifacts only after their source `m1` artifact and all derived frames validate successfully.
- R4: Ensure a partial fan-out failure leaves every previously valid timeframe readable and marks the failed transaction for retry.
- R5: Preserve dependency ordering so no derived timeframe is attempted before its source dataset is available.
- R6: Record publication evidence in this ticket before handoff.

Out of scope:
- No change to timeframe aggregation rules or bucket labels.
- No cross-symbol shared cache.
- No dataset-ID renaming.

Acceptance:
- A1 (verifies R1): Instrumentation proves one common `m1` preparation per symbol for a multi-timeframe build.
- A2 (verifies R2): Golden fixtures prove identical rows, columns, timestamps, and aggregates for all three timeframes.
- A3 (verifies R3): Transaction tests prove no child artifact is published before validated `m1` input.
- A4 (verifies R4): Failure injection proves old artifacts remain available and retry publishes a complete sibling set.
- A5 (verifies R5): Scheduler tests prove `m1 -> m5/m30/h1` ordering for history and live families.
- A6 (verifies R6): The ticket contains the exact clean status output and final PR URL before merge.

### PR-60: Gold Optional Artifact And Plot Decoupling

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/167

Branch: `codex/pr60-gold-optional-artifact-decoupling`

Depends on: PR-59

Handoff status: merged into `main` as `d136ce7` on 2026-08-02; feature branch deleted by GitHub.

Description:
- R1: Make Gold parquet and manifest publication the required production path and move plots to an explicit audit operation.
- R2: Preserve backward-compatible CLI flags while making plot generation opt-in or separately runnable.
- R3: Ensure plot failures cannot invalidate an otherwise valid parquet and manifest transaction.
- R4: Emit plot status and paths as separate operational metadata without changing Gold data schemas.
- R5: Record publication evidence in this ticket before handoff.

Out of scope:
- No feature, target, schema, or dataset-ID changes.
- No deletion of existing plot artifacts.
- No reduction of manifest validation.

Acceptance:
- A1 (verifies R1): A production Gold build writes valid parquet and manifest artifacts without invoking plotting.
- A2 (verifies R2): CLI compatibility tests cover existing flags and the explicit audit invocation.
- A3 (verifies R3): Plot failure injection leaves the validated data transaction published and retryable plot status recorded.
- A4 (verifies R4): Logs and audit reports distinguish data publication from plot publication.
- A5 (verifies R5): The ticket contains the exact clean status output and final PR URL before merge.

### PR-61: Incremental Medallion Orchestrator And Freshness Audit

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/168

Branch: `codex/pr61-incremental-medallion-orchestrator`

Depends on: PR-60

Description:
- R1: Add a dependency-aware medallion plan that runs only stale Bronze, Silver, and Gold partitions while preserving the complete-run mode.
- R2: Propagate source fingerprints and publication states across layers so downstream work starts only after upstream artifacts are valid.
- R3: Make the daily run resumable from the last successful atomic publication without duplicating rows or skipping changed inputs.
- R4: Add a dry-run plan showing stale, unchanged, blocked, and scheduled partitions before any write occurs.
- R5: Add a freshness audit that identifies stale or missing canonical and extended Gold timeframes.
- R6: Record publication evidence in this ticket before handoff.

Out of scope:
- No change to cron timing or external loader ownership.
- No silent repair of corrupt artifacts; the audit must report them for an explicit rebuild.
- No increase above the repository-wide four-core limit.

Acceptance:
- A1 (verifies R1): A fixture with unchanged and changed inputs schedules only the expected stale partitions and leaves complete-run behavior available.
- A2 (verifies R2): Integration tests block downstream publication when an upstream manifest is missing, invalid, or failed.
- A3 (verifies R3): Kill-and-retry tests converge to the same lake state as a clean run with no duplicate keys.
- A4 (verifies R4): Dry-run output is deterministic and contains every planned status without writing lake files.
- A5 (verifies R5): Freshness audit tests detect missing `m1`, `m5`, `m30`, or `h1` artifacts and report their source lineage.
- A6 (verifies R6): The ticket contains the exact clean status output and final PR URL before merge.

### PR-62: Backlog PR Branch Naming Policy

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/169

Branch: `codex/pr62-enforce-pr-branch-names`

Depends on: None

Description:
- R1: Require every new working branch to include its Backlog PR number in a deterministic lowercase form.

Acceptance:
- A1 (verifies R1): `AGENTS.md` defines the `codex/pr<backlog-number>-...` convention and examples follow it.

### PR-63: Backlog PR Commit Identifier Policy

Status: Merged

Updated: 2026-08-02

PR: https://github.com/SergejSchweizer/crypto-history-loader/pull/170

Branch: `codex/pr63-require-pr-identifiers`

Depends on: PR-62

Description:
- R1: Require each working-branch commit and squash-merge title to include its Backlog PR identifier.

Acceptance:
- A1 (verifies R1): `AGENTS.md` defines the `PR-<backlog-number>` Conventional Commit subject format with an example.

Handoff status: `git status --short` produced no output before this ticket update.

### PR-64: Backlog Cleanup For Superseded Refactoring Tickets

Status: In Progress

Updated: 2026-08-03

PR: TBD

Branch: `codex/pr64-backlog-cleanup`

Depends on: PR-63

Description:
- R1: Remove the superseded planned PR-47 through PR-53 ticket entries.
- R2: Synchronize PR-61 through PR-63 ticket statuses with their merged GitHub pull requests.

Acceptance:
- A1 (verifies R1): Backlog searches contain no PR-47 through PR-53 ticket headings.
- A2 (verifies R2): PR-61 through PR-63 each have `Status: Merged`.

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
