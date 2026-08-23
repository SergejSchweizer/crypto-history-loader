# Architecture

This document is the durable architecture contract for `crypto-loader`. Keep it aligned
with code changes that alter package boundaries, dataset contracts, medallion paths, runtime
configuration, side effects, or quality gates.

## System Shape

`crypto-loader` is a medallion data platform for deterministic crypto market-history
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
  - negative coverage sidecars for confirmed empty trade minutes
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

`perps_trades` and `options_trades` use minute-level Bronze coverage because no-trade minutes are
valid market states, not necessarily missing data. Successful zero-row Deribit responses are stored
as `empty_minutes.parquet` sidecars in the corresponding Bronze date partition with
`status=confirmed_empty`. These sidecars are negative coverage only; Bronze never writes synthetic
trade rows. Silver trade 1m feature builders consume observed trade ticks plus confirmed-empty
minutes. Empty minutes become zero-flow feature rows, and price fields are filled only from prior
observed trade closes, including across month boundaries. Future trade observations must never be
used to backfill empty-minute prices.

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

Rolling IV/RV builders calculate each monthly output on a buffered frame that includes 30 days of
prior calendar context, then trim writes back to the target month. This keeps previous-close,
rolling RV, z-score, and percentile values invariant to monthly storage partition boundaries.

`realized_volatility_1m_feature` computes Spot and Perpetual returns and RV windows as separate
feature families (`spot_*` and `perps_*`). The legacy canonical `rv_*` columns mirror one explicit
source for the whole symbol: Perpetuals when any Perpetual source exists, otherwise Spot. The
builder does not row-wise coalesce Spot and Perpetual prices, so Spot/Perp basis changes cannot be
misclassified as ordinary returns. The selected source is exposed as `canonical_rv_source`, and
missing selected-source observations remain unavailable through `canonical_rv_source_available`.
Silver manifests include `quantitative_feature_semantics` from typed contracts so units,
horizons, annualization basis, lookback, source policy, and null policy are auditable.

The supported Gold layer has four minute-level source contracts and their deterministic timeframe
derivatives:

- `gold.history.full.m1`: canonical historical market data owned by this repository;
- `gold.history.extended.m1`: historical canonical data plus trailing prediction features;
- `gold.live.full.m1`: canonical live-loader snapshot data;
- `gold.live.extended.m1`: live full data plus causal live-derived features.

Each source contract has `m5`, `m30`, and `h1` derivatives where registered. The historical
`gold.history.extended_full.m1` compatibility contract keeps the extended historical schema. Gold
dataset IDs are standalone contracts, and the canonical/extended distinction is part of the public
dataset interface rather than a column-level option.

Historical Gold joins the Spot OHLCV, perpetual OHLCV, open interest, funding, perpetual-trade,
option-trade, and optional historical-prediction Silver families on a historical minute grid.
Live Gold joins the live-loader snapshot families on the live minute grid. Missing source values
remain null according to the contract; historical and live datasets are never silently mixed.

The minute source datasets are built first. Their `m5`, `m30`, and `h1` children are then built by
deterministic bucket-start resampling. In a multi-dataset build, dataset dependencies are
serialized while symbols remain parallel within each dataset, bounded by the configured process
limit. This prevents a derived build from racing its minute source artifact.

The live extended minute dataset is built directly from live snapshot Silver sources and adds only
causal, same-row or trailing live-derived features. It does not read back from a materialized
`gold.live.full.m1` artifact.

The older `gold.market.*`, `gold.hybrid.*`, and narrow `gold.live.*` contracts are compatibility
outputs retained in the executable registry. They are not part of the supported Gold build surface
and must not be treated as additional canonical or extended datasets.

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
python scripts/validate_readme_inventory.py
pytest
```

`Makefile` exposes the same logical sequence through `make check`. GitHub Actions runs lint/docs/
config checks, typing checks, unit-test shards, and integration-test shards as independent parallel
gates, then keeps the branch-protection contexts stable with final `pr-quality` and `main-quality`
aggregator jobs. Unit and integration suites each use four deterministic test-file shards. Coverage
enforcement is configured in `pyproject.toml`; main and merge-queue runs combine coverage data from
all unit and integration shards before reporting.

## Update Protocol

Update `ARCHITECTURE.md` when a change does any of the following:

- changes package dependency direction or import-linter contracts
- changes a dataset name, path, schema, timestamp semantics, or missing-data policy
- changes Bronze/Silver/Gold orchestration or restart behavior
- changes side-effect ownership for HTTP, lake IO, plotting, manifests, or CLI output
- changes runtime configuration, quality gates, or documented validation commands

If the architecture does not change, no edit is required. If behavior changes but this file does not,
the PR should explain why the existing architecture contract still applies.
