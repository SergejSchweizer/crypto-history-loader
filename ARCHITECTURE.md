# Architecture

This document is the durable architecture contract for `crypto-history-loader`. Keep it aligned
with code changes that alter package boundaries, dataset contracts, medallion paths, runtime
configuration, side effects, or quality gates.

## System Shape

`crypto-history-loader` is a medallion data platform for deterministic crypto market-history
loading and feature generation.

```text
              config.yaml
                  |
                  v
main.py -> api/cli.py -> api/commands/*
                  |
                  v
        application services and DTOs
                  |
        +---------+---------+
        |                   |
        v                   v
 ingestion exchange     ingestion lake
 adapters and parsers   adapters and sidecars
        |                   |
        +---------+---------+
                  |
                  v
        lake/bronze -> lake/silver -> lake/gold
```

The package dependency direction is intentionally narrow:

```text
api ---------------> application ---------------> ingestion-facing DTOs/contracts
 |                         |
 |                         v
 |                 runtime policies and services
 |
 +----X direct lake persistence internals

ingestion ---------X api
ingestion ---------X application
application -------X api
```

Architecture boundaries are enforced by `.importlinter` and `tests/test_import_linter.py`.

## Layer Ownership

| Layer | Owns | Must not own |
|---|---|---|
| `api/` | CLI parser registration, command wiring, command output shape | Exchange calls, lake persistence internals, durable business contracts |
| `application/` | Use-case orchestration, DTOs, runtime policy, dataset contracts, restart behavior | CLI parsing, HTTP adapter details, parquet layout details |
| `ingestion/` | Exchange adapters, source parsing, lake layout, parquet IO, sidecars, plotting adapters | CLI behavior, application service orchestration |
| `scripts/` | Operational entrypoints and documentation/history generators | Hidden business rules that bypass application contracts |
| `tests/` | Regression, architecture, contract, CLI, and quality-gate validation | Production behavior |

When a change crosses layers, define or update the typed contract first, then update the adapter or
orchestrator. Compatibility facades are acceptable while refactoring, but the owning layer must stay
clear in tests and docs.

## Medallion Data Flow

```text
Bronze
  - source-shaped, normalized records
  - deterministic partition paths
  - idempotent writes
  - restart-safe checkpoints

Silver
  - dataset-specific contracts
  - observed datasets and 1m feature datasets
  - timestamp normalization and missing-data policy
  - monthly parquet outputs plus optional sidecars

Gold
  - declared source requirements
  - model-ready joins
  - feature profiling, versioning, manifests, and audit metadata
```

Current Bronze dataset identities are registered in `application/datasets.py`:

```text
spot_ohlcv
perps_ohlcv
open_interest
funding
perps_trades
options_trades
volatility_index_data
```

`volatility_index_data` stores Deribit volatility-index OHLC observations in Bronze as
`value`, `open`, `high`, `low`, and `close`. Silver exposes these as `volatility_value`,
`volatility_open`, `volatility_high`, `volatility_low`, and `volatility_close`; Gold exposes
them as `volatility_index_value`, `volatility_index_open`, `volatility_index_high`,
`volatility_index_low`, and `volatility_index_close`.

Silver and Gold contracts live in `application/dataset_contracts.py`. If a dataset is renamed,
added, or removed, update all of the following in the same change set:

- `application/datasets.py`
- `application/dataset_contracts.py`
- Bronze/Silver/Gold command choices
- lake path helpers and sidecar defaults
- README dataset tables and examples
- this file
- focused tests for planning, contracts, storage, Silver, Gold, and CLI parsing

Instrument-level option ticker snapshots are normalized into
`options_instrument_ticker_snapshot_1m_observed`. They use the same option-contract columns as
currency-level ticker snapshots, retain `source_endpoint`, and write a monthly reconciliation
sidecar against `options_ticker_snapshot_1m_observed`. Instrument-level observations take
precedence when the same exchange, instrument, and timestamp occur in both families.

`options_surface_1m_feature` combines both observed ticker families at closed-minute boundaries.
Instrument observations retain precedence. ATM contracts satisfy
`abs(log(strike / underlying_price)) <= 0.05`; short-dated contracts expire within seven days,
long-dated contracts expire after 30 days, and skew compares the 85-95% put wing with the
105-115% call wing. The builder never carries a later observation backward and reports quote
coverage plus fresh/stale counts using a 60-second ingest-latency threshold.

`perps_l2_snapshot_1m_observed` retains validated, source-shaped bid and ask levels. Prices must
be positive and finite, sizes non-negative and finite, bids descending, asks ascending, and
two-sided books uncrossed. Empty or one-sided books remain explicit observations. The last
snapshot in each closed minute feeds `perps_l2_1m_feature`, including spread, top imbalance, and
bid/ask depth within fixed 10- and 50-bps bands around the mid price.

`options_l2_snapshot_1m_observed` applies the same book invariants while retaining normalized
option contract identity. `options_l2_1m_feature` remains one row per contract and closed minute;
`quote_available`, `quote_age_seconds`, and `stale_quote` make liquidity filtering joinable to
option surfaces on `exchange`, `instrument_name`, and `timestamp_m1`. A quote is stale when its
non-negative ingest latency exceeds 60 seconds.

`recent_trade_snapshot_1m_observed` is an explicitly snapshot-derived trade view, not a complete
historical tick source. Real source trade IDs are the primary deduplication key; missing IDs use a
deterministic composite of exchange, instrument, event time, price, quantity, and side. Monthly
reconciliation sidecars compare source IDs with `perps_trades_observed` and
`options_trades_observed`, while contract metadata and snapshot lineage remain in every row.

`instrument_metadata_snapshot_daily_observed` and
`futures_instrument_metadata_snapshot_daily_observed` share one daily contract keyed by
`snapshot_date`, `exchange`, and `instrument_name`. Source `kind` becomes the market-facing
`option`, `future`, or `perp` instrument type; option type is normalized to `C/P`. The latest
valid ingest per day wins, while active state, listing interval, currencies, tick size, and
contract size remain explicit for joins and universe filters.

`historical_volatility_observed` preserves Deribit's external historical-volatility value and
source timestamp as an auxiliary reference. It accepts only finite, non-negative values and does
not resample or emit any internally computed `rv_*` field; `realized_volatility_1m_feature`
remains the separately owned realized-volatility calculation.

`volatility_index_1m_feature.iv_close` is Deribit's DVOL-style implied-volatility index: an
annualized, 30-day-horizon measure already expressed in percentage points, aliased explicitly as
`iv_30d_annualized_pct`. `realized_volatility_1m_feature`'s raw `rv_*` windows are non-annualized
`sqrt(sum(log_return^2))` estimates over their window; each has an annualized-percentage-point
sibling (for example `rv_30d_annualized_pct`) computed against a shared 365-day annualization
basis. Only annualized, horizon-matched fields (`iv_30d_annualized_pct` against
`rv_30d_annualized_pct`) are financially comparable; `iv_rv_1m_feature` exposes this as
`iv_rv_spread_30d_pct` and `iv_rv_ratio_30d`. The legacy `iv_minus_rv_1h`, `iv_minus_rv_1d`,
`iv_rv_ratio_1h`, and `iv_rv_ratio_1d` columns mix an annualized IV index with non-annualized,
sub-30-day RV and remain only for backward compatibility with existing persisted artifacts; new
consumers should prefer the annualized 30-day comparison.

The Gold layer has two canonical model-ready endpoints: `gold.market.history_full.m1` for
historical data produced by this repository, and `gold.live.full.m1` for live-origin data produced
from `crypto-live-loader` inputs. Narrower Gold contracts remain available as internal building
blocks and compatibility outputs, but downstream training and inference code should target the two
canonical full datasets.

`gold.market.history_full.m1` is the full historical dataset for data this repository actually
fetches into Bronze. It joins only spot OHLCV, perpetual OHLCV, open interest, funding,
perpetual-trade, and option-trade families through their Silver representations on the historical
minute grid. The grid covers the union of those historical source timestamps; missing source values
stay null. Realized-volatility, IV/RV, volatility-index, L2, index, futures-summary,
option-surface, strategy, target, and label columns belong to narrower research-facing Gold
contracts, not to `gold.market.history_full.m1`.

`gold.market.regime_features.m1` owns the research-facing IV/RV regime contract. Its minute grid
is determined only by required spot, perpetual, funding, open-interest, realized-volatility, and
IV/RV sources. Perpetual L2, options L2, option surface, index price, and external historical
volatility, and futures summary are optional left joins with stable nullable columns; their
presence never expands the grid or changes column order. Manifests record availability, covered
grid minutes, coverage ratio, source time range, and freshness at the required-grid end for every
optional source. The contract contains market state only and does not create predictive labels.
Strategy feature families in the regime contract are derived only from the joined minute state with
trailing windows. Their declared lookbacks are emitted in the Gold manifest, and target or label
columns remain reserved for separate forward-looking datasets.

`gold.market.prediction_targets.m1` is the separate forward-looking training-target contract. It
emits only timestamp keys plus `target_*` and `label_*` columns, with horizon definitions,
transaction-cost assumptions, regime-shift thresholds, and null rules recorded in the manifest.
These columns must never be joined back into live or historical feature outputs.

`gold.live.volatility_features.m1` is the live-origin volatility-index Gold contract. It uses
`volatility_index_1m_feature` as its only required source, preserves the overlapping historical
`iv_*` feature names, units, minute timestamp semantics, and null rules, and adds `as_of` plus
`live_snapshot_derived` lineage columns. Missing live minutes remain null inside the Gold grid;
the contract does not backfill from historical datasets.

`gold.live.microstructure_features.m1` exposes live L2 microstructure state from
`perps_l2_1m_feature` and `options_l2_1m_feature`. Perpetual L2 fields keep the `perps_l2_`
prefix, option-book aggregates keep the `options_l2_` prefix, and each source carries its own
`*_as_of` and `*_live_snapshot_derived` lineage. Quote availability, staleness, quote age, depth,
and option quote coverage remain explicit so live consumers can filter stale or incomplete rows
without hidden fills.

`gold.live.full.m1` is the canonical live full dataset. It combines live volatility-index features,
live IV/RV features, perpetual L2 features, and option L2 aggregates into one inference table,
marks the manifest origin as `crypto-live-loader`, and keeps optional live index, futures-summary,
and option-surface features nullable. Missing live minutes stay null and are never backfilled from
historical data.

## Runtime And Side Effects

`config.yaml` is the canonical durable runtime configuration source. Runtime values should be
validated through `application/services/config_validation.py` and resolved through policy modules
instead of ad hoc command code.

Side effects are isolated by boundary:

```text
HTTP/network       -> ingestion/exchanges/* and ingestion/http_client.py
Bronze lake writes -> ingestion/lake_bronze_writes.py and ingestion/lake_records.py
Lake reads         -> ingestion/lake_reads.py, ingestion/lake_dataframe.py, ingestion/lake_queries.py
Silver/Gold builds -> application/services/silver_*.py and application/services/gold_*.py
CLI output         -> api/commands/*_output*.py
```

Long-running Bronze work must remain idempotent and restart-safe. Checkpoint keys are derived from
dataset task identity, not from incidental tuple ordering.

## Quality Gates

The local and CI quality gates are intentionally aligned:

```text
ruff check .
ruff format --check .
mypy .
pyright --level error
ty check
lint-imports --config .importlinter
python scripts/validate_config_with_pydantic.py --config config.yaml
python scripts/update_project_history_docs.py --check
pytest
```

`Makefile` exposes the same logical sequence through `make check`. Coverage enforcement is
configured in `pyproject.toml`.

GitHub repository gates are managed through `scripts/github/apply_quality_gates.sh`, which uses
`gh api` to configure server-side repository settings. The script is the durable configuration
entrypoint for merge policy, `main` branch protection, and the default-branch merge-queue ruleset;
the GitHub web UI should be treated as an inspection surface. Pull requests require the
`pr-quality` CI job before merge. Pushes to `main` and merge-queue candidates run the full
`main-quality` job, including coverage. If GitHub rejects API-based merge-queue setup for the
repository plan or account, the script leaves branch protection active and reports the required
manual UI step. The protected `main` branch also requires up-to-date branches before merge, linear
history, resolved PR conversations, no force pushes, and no branch deletions. Repository merge
settings are squash-only with automatic branch deletion after merge.

## Update Protocol

Update `ARCHITECTURE.md` in the same PR when a change does any of the following:

- changes package dependency direction or import-linter contracts
- changes a dataset name, path, schema, timestamp semantics, or missing-data policy
- changes Bronze/Silver/Gold orchestration or restart behavior
- changes side-effect ownership for HTTP, lake IO, plotting, manifests, or CLI output
- changes runtime configuration, quality gates, or documented validation commands

If the architecture does not change, no edit is required. If behavior changes but this file does not,
the PR should explain why the existing architecture contract still applies.
