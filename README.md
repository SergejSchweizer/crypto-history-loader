# CRYPTO-HISTORY-LOADER

Production-grade cryptocurrency market data ingestion, normalization, feature engineering, and dataset generation framework for quantitative research and systematic trading.

Author: Sergej Schweizer

---

# Table Of Contents

- [CRYPTO-HISTORY-LOADER](#crypto-history-loader)
- [Table Of Contents](#table-of-contents)
- [1. System Overview](#1-system-overview)
  - [1.1 Core Design Principles](#11-core-design-principles)
  - [1.2 Medallion Architecture](#12-medallion-architecture)
  - [1.3 Supported Data Domains](#13-supported-data-domains)
- [2. Repository Structure](#2-repository-structure)
- [3. Installation](#3-installation)
  - [3.1 System prerequisites](#31-system-prerequisites)
  - [3.2 Python environment setup](#32-python-environment-setup)
- [4. Raw Datasets](#4-raw-datasets)
  - [4.1 Spot (`dataset_type=spot`)](#41-spot-dataset_typespot)
  - [4.2 Perpetual (`dataset_type=perp`)](#42-perpetual-dataset_typeperp)
  - [4.3 Open Interest (`dataset_type=oi`)](#43-open-interest-dataset_typeoi)
  - [4.4 Funding (`dataset_type=funding`)](#44-funding-dataset_typefunding)
  - [4.5 Perpetual Trades (`dataset_type=perp_trades`)](#45-perpetual-trades-dataset_typeperp_trades)
  - [4.6 Option Trades (`dataset_type=option_trades`)](#46-option-trades-dataset_typeoption_trades)
- [5. Example Commands](#5-example-commands)
  - [5.1 End-to-End Pipeline](#51-end-to-end-pipeline)
  - [5.2 Layer Commands](#52-layer-commands)
  - [5.3 Operational Notes](#53-operational-notes)
  - [5.4 Quality Checks](#54-quality-checks)
- [7. Roadmap](#7-roadmap)

---

# 1. System Overview

## 1.1 Core Design Principles

The repository follows the engineering principles defined in `AGENTS.md`:

- maintainability
- modularity
- reproducibility
- deterministic processing
- idempotent ingestion
- explicit interfaces
- production-grade architecture

## 1.2 Medallion Architecture

The system uses a medallion pipeline in which exchange API data is first persisted in Bronze as
normalized, append-oriented raw records, then transformed in Silver into canonical time-aligned
feature datasets, and finally published in Gold as versioned, model-ready joins with deterministic
processing, explicit contracts, and restart-safe execution guarantees.

## 1.3 Supported Data Domains

Supported ingest domains are defined by `DATASET_REGISTRY` in `application/datasets.py`.

### Domain Groups

OHLCV:

| CLI Domain | Bronze `dataset_type` | Instrument Type | Task Kind | Default Timeframe | Symbol Source | Description |
|---|---|---|---|---|---|---|
| `spot` | `spot` | `spot` | `ohlcv` | `1m` | `--symbols` | Physical spot OHLCV candles |
| `perp` | `perp` | `perp` | `ohlcv` | `1m` | `--symbols` | Perpetual futures OHLCV candles |

Interval State:

| CLI Domain | Bronze `dataset_type` | Instrument Type | Task Kind | Default Timeframe | Symbol Source | Description |
|---|---|---|---|---|---|---|
| `oi` | `oi` | `perp` | `open_interest` | `1m` | `--symbols` | Open-interest observations |
| `funding` | `funding` | `perp` | `funding` | `1m`* | `--symbols` | Funding-rate observations (stored at native cadence) |

Trade Ticks:

| CLI Domain | Bronze `dataset_type` | Instrument Type | Task Kind | Default Timeframe | Symbol Source | Description |
|---|---|---|---|---|---|---|
| `perp_trades` | `perp_trades` | `perp` | `trade` | `tick` | `--symbols` | Historical perpetual trade ticks |
| `option_trades` | `option_trades` | `option` | `trade` | `tick` | `--symbols` | Historical option trade ticks |

\* Funding input accepts `1m`/`m1` aliases but normalizes to Deribit-native `8h` events.

### CLI Contract

- `bronze-build --dataset` choices: `spot perp oi funding perp_trades option_trades`
- `--symbols` applies to all selected datasets (`spot`, `perp`, `oi`, `funding`, `perp_trades`, `option_trades`)

Current exchange support:

- Deribit

Primary symbols:

- BTC
- ETH
- SOL

---

# 2. Repository Structure

```text
api/
application/
ingestion/
scripts/
lake/
docs/
tests/
agents/
config.yaml
pyproject.toml
main.py
README.md
AGENTS.md
```

| Path | Responsibility |
|---|---|
| `api/` | CLI entrypoints |
| `application/` | Pipeline orchestration and service-layer business logic |
| `ingestion/` | Exchange adapters, parsing, storage IO, and source-facing contracts |
| `scripts/` | Operational scripts (pipeline runner, validation, maintenance helpers) |
| `lake/` | Local medallion storage roots (for example `lake/bronze`, `lake/silver`, `lake/gold`) |
| `tests/` | Validation and regression tests |
| `docs/` | Documentation assets (figures, tables, reference materials) |
| `agents/` | Agent-policy source fragments and synchronization helpers |
| `config.yaml` | Canonical runtime configuration |
| `pyproject.toml` | Project metadata and Python tooling configuration |
| `main.py` | Python entrypoint wrapper for CLI execution |
| `AGENTS.md` | Generated repository operating policy (do not edit directly) |

Dataset metadata is centralized in `application/datasets.py`. New Bronze datasets should start with a
`DatasetSpec` entry that defines the CLI name, storage dataset type, instrument type, symbol group,
task kind, and default timeframe. Bronze planning derives fetch tuples from these specs, so
new datasets can share symbol validation, deterministic scheduling, checkpoint fingerprints, and
reporting behavior instead of duplicating one-off planner logic.

---

# 3. Installation

## 3.1 System prerequisites

Because this repository is used heavily with GitHub workflows, install both `git` and the GitHub CLI
(`gh`) on every development machine (Linux and Windows) before running project setup.

Linux (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y git gh
```

Windows (PowerShell + winget):

```powershell
winget install --id Git.Git -e
winget install --id GitHub.cli -e
```

Verify installs:

```bash
git --version
gh --version
```

## 3.2 Python environment setup

```bash
uv sync --extra dev
```

The `dev` extra installs the local quality-gate tools used by pre-commit and CI-style checks:
Ruff, Mypy, Pyright, ty, import-linter, pytest, pytest-cov, and pre-commit.

Runtime configuration uses:

```text
config.yaml
```

Recommended permissions:

```bash
chmod 600 config.yaml
```

---

# 4. Raw Datasets

Raw ingests are defined by `application/datasets.py` and persisted by Bronze writers in
`ingestion/lake.py`. The repository currently ingests six raw dataset types:
`spot`, `perp`, `oi`, `funding`, `perp_trades`, and `option_trades`.

All datasets share structural metadata columns:
`schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`,
`ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`.

Coverage reference for missing statistics in this section:
- Start: first observed day per dataset series
- End: `2026-05-25` (inclusive)
- Missing %: missing calendar days / expected calendar days

## 4.1 Spot (`dataset_type=spot`)

Market role: physical spot-market state, baseline for directional and volatility context.
Relationship: joins with `perp` by symbol and minute to compute basis; anchors Gold core joins.
Raw ingestion granularity: `1m` candles.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `open_price` | quote/base price | First trade price in interval; opening equilibrium. | Used with `close_price` for returns; compared with `perp.close_price` for basis state. |
| `high_price` | quote/base price | Maximum traded price; upside excursion. | Paired with `low_price` for range-volatility features. |
| `low_price` | quote/base price | Minimum traded price; downside excursion. | Combined with `high_price` for intrabar stress/range diagnostics. |
| `close_price` | quote/base price | Last traded price; end-of-interval mark. | Primary aligned price in Silver/Gold joins. |
| `volume` | base-asset units | Traded base quantity; participation intensity. | Compared with perp/trades flow volumes for regime analysis. |
| `quote_volume` | quote-currency units | Traded notional volume. | Complements `volume` for average execution/notional flow inference. |
| `trade_count` | count | Number of executions in interval. | Coarse activity proxy compared with tick-level trade datasets. |
| `origin_payload` | JSON/object | Full source-shaped raw record for audit/replay. | Backstop for reconciliation and schema-drift debugging. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC_USDC` | `1m` | `2023-04-24` | `2026-05-25` | 0 | 0.00% |
| `deribit` | `ETH_USDC` | `1m` | `2023-04-24` | `2026-05-25` | 0 | 0.00% |
| `deribit` | `SOL_USDC` | `1m` | `2024-02-27` | `2026-05-25` | 0 | 0.00% |

## 4.2 Perpetual (`dataset_type=perp`)

Market role: leveraged perpetual-futures state with faster leverage-driven price discovery.
Relationship: interpreted with `funding` and `oi` for crowding/leverage regimes; joined with `spot`
for basis and premium state.
Raw ingestion granularity: `1m` candles.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `open_price` | USD (or quote/base) | Opening perpetual mark for interval. | Used against spot prices to infer carry and dislocation. |
| `high_price` | USD (or quote/base) | Intrabar maximum price. | Coupled with OI/funding shifts to detect squeeze conditions. |
| `low_price` | USD (or quote/base) | Intrabar minimum price. | Combined with OI drawdowns for liquidation diagnostics. |
| `close_price` | USD (or quote/base) | End-of-interval perpetual mark. | Canonical join key with funding/OI minute features. |
| `volume` | contracts/base units | Leveraged venue traded size. | Compared with spot volume and tick-flow aggregates for speculation intensity. |
| `quote_volume` | quote-currency units | Perpetual notional turnover. | Used for cross-market notional participation diagnostics. |
| `trade_count` | count | Number of perp executions. | Coarse complement to `perp_trades` microstructure rows. |
| `origin_payload` | JSON/object | Full source-shaped raw record for audit/replay. | Reconciliation source for derived Silver features. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC-PERPETUAL` | `1m` | `2018-08-14` | `2026-05-25` | 27 | 0.95% |
| `deribit` | `ETH-PERPETUAL` | `1m` | `2019-03-14` | `2026-05-25` | 12 | 0.46% |
| `deribit` | `SOL-PERPETUAL` | `1m` | `2022-04-29` | `2026-05-25` | 11 | 0.74% |

## 4.3 Open Interest (`dataset_type=oi`)

Market role: outstanding leveraged exposure stock.
Relationship: interpreted jointly with `perp` returns and `funding` to classify leverage build-up,
covering, and liquidation regimes.
Raw ingestion granularity: `1m` observations.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `open_interest` | contracts | Total open positions at timestamp. | Combined with price direction from `perp` to classify position flow regime. |
| `open_interest_value` | quote-currency notional | Monetary exposure form of OI. | Scales raw OI for cross-period comparability and risk sizing. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC-PERPETUAL` | `1m` | `2018-08-15` | `2026-05-25` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `1m` | `2019-03-15` | `2026-05-25` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `1m` | `2022-03-16` | `2026-05-25` | 0 | 0.00% |

## 4.4 Funding (`dataset_type=funding`)

Market role: periodic long-short transfer/carry state.
Relationship: enriches perp/spot state with crowding and carry; interpreted with OI for leverage
imbalance.
Raw ingestion granularity: native `8h` funding events.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `funding_rate` | fraction per `8h` event | Funding transfer rate between longs and shorts. | Combined with OI/perp moves for crowding and squeeze diagnostics. |
| `index_price` | USD | External fair-value index around funding event. | Baseline for mark/index dislocation and premium state. |
| `mark_price` | USD | Exchange mark reference around funding timestamp. | Compared with index/perp close for premium and stress features. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC-PERPETUAL` | `8h` | `2023-04-24` | `2026-05-25` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `8h` | `2023-04-24` | `2026-05-25` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `8h` | `2024-02-27` | `2026-05-25` | 0 | 0.00% |

## 4.5 Perpetual Trades (`dataset_type=perp_trades`)

Market role: tick-level perpetual execution flow and aggressor pressure.
Relationship: aggregated into `perp_trades_1m_feature` and joined with `spot/perp/oi/funding` in
Gold (`gold.market.perp_trades.m1`, `gold.market.full.m1`).
Raw ingestion granularity: `tick` (per trade execution).

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `trade_id` | identifier | Unique trade execution id. | Deduplication and idempotent replay key. |
| `price` | USD (or quote/base) | Executed trade price. | Aggregated into 1m OHLC path for flow features. |
| `quantity` | contracts/base units | Executed size per trade. | Aggregates into directional and intensity flow metrics. |
| `side` | category (`buy`/`sell`/`unknown`) | Aggressor side proxy. | Basis for buy/sell imbalance and participation skew. |
| `is_maker` | boolean | Maker-side indicator proxy. | Liquidity-provision vs taker-pressure diagnostics. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC-PERPETUAL` | `tick` | `2023-01-01` | `2026-05-24` | 1093 | 88.07% |
| `deribit` | `ETH-PERPETUAL` | `tick` | `2023-04-25` | `2026-05-24` | 1093 | 96.98% |

## 4.6 Option Trades (`dataset_type=option_trades`)

Market role: tick-level options execution flow with contract metadata.
Relationship: aggregated into `option_trades_1m_feature`; joins at underlying level (`BTC`, `ETH`)
with spot/perp state in Gold (`gold.market.option_trades.m1`, `gold.market.full.m1`).
Raw ingestion granularity: `tick` (per trade execution).

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `trade_id` | identifier | Unique option trade id. | Deduplication/replay identity. |
| `price` | option premium (quote units) | Executed option premium. | Aggregated into option-flow pressure features. |
| `quantity` | contracts | Number of option contracts traded. | Volume and participation proxy for options activity. |
| `side` | category (`buy`/`sell`/`unknown`) | Aggressor side proxy. | Supports directional option-flow imbalance features. |
| `is_maker` | boolean | Maker-side indicator proxy. | Liquidity-taking vs provision context. |
| `instrument_name` | contract code | Full exchange contract identifier. | Parent for `expiry`, `strike`, `option_type` extraction. |
| `expiry` | contract expiry code | Option maturity bucket. | Used with timestamp for term-structure activity mapping. |
| `strike` | strike price (USD) | Contract strike level. | Combined with underlying spot/perp for moneyness context. |
| `option_type` | category (`call`/`put`/`unknown`) | Contract payoff side. | Enables call/put activity skew features. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC` | `tick` | `2018-08-14` | `2026-05-24` | 1171 | 41.20% |
| `deribit` | `ETH` | `tick` | `2023-04-25` | `2026-05-24` | 882 | 78.26% |

---

# 5. Example Commands

## 5.1 End-to-End Pipeline

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

Runs `bronze-build -> silver-build -> gold-build` using `medallion-pipeline` settings from
`config.yaml`, enforces single-run locking via `.run/full-pipeline.lock`, and writes a shared
append-only pipeline log.

## 5.2 Layer Commands

Bronze:

```bash
uv run python main.py bronze-build \
  --exchange deribit \
  --dataset spot perp oi funding perp_trades option_trades \
  --symbols BTC ETH SOL
```

Silver:

```bash
uv run python main.py silver-build \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --exchange deribit \
  --dataset spot perp oi funding perp_trades option_trades \
  --timeframe 1m
```

Gold:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.market.full.m1
```

## 5.3 Operational Notes

Symbol-group controls for Bronze:

- `--symbols` applies to all selected datasets (`spot`, `perp`, `oi`, `funding`, `perp_trades`, `option_trades`)
- default symbols are `BTC ETH SOL`

Bronze checkpoint path:

```text
.run/checkpoints/bronze-build.json
```

Checkpoint behavior:

- completed tasks are recorded incrementally
- reruns with the same effective plan skip completed tasks
- successful runs delete the checkpoint automatically

Manual reset:

```bash
rm -f .run/checkpoints/bronze-build.json
```

Perp-trades storage path: `dataset_type=perp_trades`.

Gold source selection:

- for each required upstream dataset, equivalent symbol variants are normalized and the newest
  parquet artifact is selected
- `gold.hybrid.full_l2.m1` applies the same newest-artifact policy for L2 input

Gold retention policy:

- keep latest `N` versions per `dataset_id/exchange/symbol` lineage (default `N=3`)
- configure via `gold-build.retention_keep_versions` in `config.yaml` or override with
  `--retention-keep-versions`.

Available Gold dataset IDs:

- `gold.market.perp_trades.m1` (perp-trade-flow only)
- `gold.market.option_trades.m1` (option-trade-flow only)
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.hybrid.full_l2.m1`

## 5.4 Quality Checks

Run this sequence before pushing changes:

```bash
uv run ruff check .
uv run mypy .
uv run pyright --level error
uv run ty check
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.yaml
uv run pytest
```

| Check | Scope | Gate Objective | Failure Signal |
|---|---|---|---|
| `uv run ruff check .` | Lint and static quality rules | Keep code quality and prevent obvious correctness pitfalls before runtime. | Style/correctness violations such as unused imports, invalid patterns, or rule breaches. |
| `uv run mypy .` | Static typing | Enforce typed contracts across DTOs, services, and module boundaries. | Type mismatches, invalid `None` handling, incompatible signatures. |
| `uv run pyright --level error` | Static typing (strict) | Provide complementary type analysis and stricter narrowing checks. | Type errors not caught by mypy or stricter incompatibility findings. |
| `uv run ty check` | Additional typing gate | Maintain policy-level typing consistency across the codebase. | Unresolved typing gaps and annotation inconsistencies. |
| `uv run lint-imports --config .importlinter` | Architecture boundaries | Enforce dependency direction and import-layer contracts. | Boundary violations (for example domain importing infrastructure internals). |
| `uv run python scripts/validate_config_with_pydantic.py --config config.yaml` | Runtime config schema | Reject invalid runtime configuration before pipeline execution. | Missing/invalid config fields or schema/type constraint failures. |
| `uv run pytest` | Behavioral + regression tests | Validate functional behavior and enforce coverage thresholds. | Test failures, behavioral regressions, or coverage below configured threshold. |

Operational notes:

- `pytest` coverage defaults are configured in `pyproject.toml`.
- Pre-commit enforces the same logical quality-gate path used in CI.

---

# 7. Roadmap

Objective: build complete, reproducible historical quote coverage for `BTC`, `ETH`, and `SOL`.

| Phase | Priority | Focus | Deliverables |
|---|---|---|---|
| 1. Bronze Completeness | High | Close historical quote gaps and keep daily continuity. | Backfill missing quote days, enforce daily completeness checks, and alert on new gaps. |
| 2. Data Quality Controls | High | Improve trust in raw quote integrity. | Automated checks for outliers, stale intervals, duplicate windows, and symbol-normalization drift. |
| 3. Multi-Exchange Coverage | High | Reduce single-venue bias and improve robustness. | Add at least one additional exchange for `BTC`/`ETH`/`SOL`, plus cross-exchange reconciliation metrics. |
| 4. Silver/Gold Contract Hardening | Medium | Stabilize research-facing quote features. | Explicit alignment/merge contracts, regression tests for joins/resampling, and versioned feature expectations. |
| 5. Quote Readiness Reporting | Medium | Make model readiness measurable. | Recurring coverage, freshness, and quality reports with clear pass/fail thresholds. |

Near-term execution order:

1. Complete phase 1 for all three symbols.
2. Implement phase 2 checks in CI and pre-commit-compatible local runs.
3. Start phase 3 with one exchange and expand after reconciliation is stable.
