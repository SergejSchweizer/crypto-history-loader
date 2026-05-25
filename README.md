# crypto-market-loader

Production-grade cryptocurrency market data ingestion, normalization, feature engineering, and dataset generation framework for quantitative research and systematic trading.

---

## Table of Contents

- [1. Executive Summary](#1-executive-summary)
- [2. Project Goals and Scope](#2-project-goals-and-scope)
- [3. System Architecture](#3-system-architecture)
- [4. Data Coverage](#4-data-coverage)
- [5. Dataset Definitions](#5-dataset-definitions)
- [6. Storage Layout](#6-storage-layout)
- [7. Repository Structure](#7-repository-structure)
- [8. Installation and Environment](#8-installation-and-environment)
- [9. Pipeline Execution](#9-pipeline-execution)
- [10. Quality Gates](#10-quality-gates)
- [11. Quant Research Usage](#11-quant-research-usage)
- [12. Engineering Standards](#12-engineering-standards)
- [13. Extensions and Roadmap](#13-extensions-and-roadmap)

---

## 1. Executive Summary

`crypto-market-loader` provides a deterministic medallion pipeline (Bronze -> Silver -> Gold) for crypto market data.
It is designed for repeatable research workflows, stable schema evolution, and ML-ready dataset generation.

Primary outcomes:

- deterministic ingestion and processing
- schema-stable parquet datasets
- reproducible feature engineering
- explicit dataset contracts and lineage
- restart-safe historical backfills

---

## 2. Project Goals and Scope

The platform targets:

- systematic trading research
- market-neutral strategy research
- volatility forecasting
- hidden Markov model regime detection
- reinforcement learning state construction
- derivatives analytics and cross-market diagnostics

Core design principles (aligned with `AGENTS.md`):

- maintainability and modularity
- deterministic processing and idempotency
- explicit interfaces and side-effect ownership
- production-grade testability and reproducibility

---

## 3. System Architecture

### 3.1 Medallion Layers

```text
Exchange APIs
      |
      v
+----------------+
| Bronze Layer   |
| Raw normalized |
+----------------+
      |
      v
+----------------+
| Silver Layer   |
| Feature tables |
+----------------+
      |
      v
+----------------+
| Gold Layer     |
| ML datasets    |
+----------------+
```

### 3.2 Layer Responsibilities

Bronze:

- append-oriented raw normalized data
- minimal transformation and source fidelity preservation
- deterministic, audit-friendly persistence

Silver:

- feature derivation and canonical resampling
- trade-tick to 1-minute aggregation
- rolling statistics and volatility transforms
- funding and open-interest transformations

Gold:

- canonical 1-minute alignment across feature families
- latest-source selection for equivalent upstream variants
- versioned final datasets for modeling
- manifest and provenance outputs

---

## 4. Data Coverage

### 4.1 Supported Domains

| Dataset | Description |
|---|---|
| Spot OHLCV | Physical spot market |
| Perpetual OHLCV | Leveraged perpetual futures |
| Funding | Long/short positioning pressure |
| Open Interest | Aggregate leveraged exposure |
| Tick Trades | Historical trade-by-trade prints (REST backfill) |
| Option Tick Trades | Historical option trade prints (REST backfill) |

### 4.2 Current Exchange and Symbols

Current exchange support:

- Deribit

Primary symbols:

- BTC
- ETH
- SOL

---

## 5. Dataset Definitions

### 5.1 Bronze/Silver Domain Semantics

#### Spot OHLCV

Represents the underlying physical market.

| Field | Meaning |
|---|---|
| open | First traded price |
| high | Highest traded price |
| low | Lowest traded price |
| close | Last traded price |
| volume | Base asset turnover |

Quantitative relevance:

- baseline market direction
- volatility estimation
- trend structure
- lead/lag modeling
- spot/perpetual basis analysis

#### Perpetual OHLCV

Represents leveraged perpetual futures trading.

Perpetuals frequently lead spot during:

- liquidations
- leverage expansions
- speculative squeezes
- volatility shocks

| Feature | Interpretation |
|---|---|
| perp returns | Leveraged directional pressure |
| perp volume | Speculative participation |
| basis vs spot | Carry and leverage state |
| volatility | Market stress |

#### Open Interest

Open interest measures total leveraged exposure.

| Concept | Meaning |
|---|---|
| observed OI | Native exchange observation |
| OI 1m feature | Forward-filled modeling feature |

| Price | OI | Meaning |
|---|---|---|
| Up | Up | New longs entering |
| Down | Up | New shorts entering |
| Up | Down | Short covering |
| Down | Down | Long liquidation |

Primary uses:

- leverage regime detection
- squeeze prediction
- volatility forecasting
- systemic stress estimation

#### Funding Rate

Funding transfers capital between longs and shorts.

| Funding State | Market Meaning |
|---|---|
| Positive funding | Long crowding |
| Negative funding | Short crowding |
| Neutral funding | Balanced positioning |

Primary uses:

- carry strategies
- market-neutral signals
- crowding analysis
- mean reversion systems
- regime detection

#### Tick Trades and Option Tick Trades

Tick trades represent per-execution market prints.

Silver builds `perp_trades_1m_feature` from perpetual trade ticks and `option_trades_1m_feature` from option ticks.

| Feature | Meaning |
|---|---|
| open/high/low/close | Minute-level trade-price path |
| volume / quote_volume | Executed flow intensity |
| trade_count | Activity and participation |
| buy/sell volume + counts | Directional aggressor pressure proxy |
| buy_volume_share | Buy-side flow dominance |

### 5.2 Quantitative Feature Interpretation

Price features describe:

- trend
- momentum
- volatility clustering
- regime shifts

Volume features describe:

- participation intensity
- speculative activity
- stress conditions
- liquidity conditions

Trade-flow features describe:

- execution-level pressure
- buy/sell imbalance
- participation bursts
- short-horizon microstructure regime shifts

Funding features describe:

- directional crowding
- leverage imbalance
- carry state
- sentiment extremes

Open-interest features describe:

- leverage expansion
- leverage unwind
- liquidation risk
- structural market stress

Cross-market interactions are often highest signal:

| Combination | Interpretation |
|---|---|
| spot/perp spread | Futures premium |
| funding + OI | Crowded leverage |
| OI + volatility | Fragile market state |
| volume + funding | Speculative frenzy |

### 5.3 Gold Dataset Catalog

#### `gold.market.perp_trades.m1`

Contains perpetual tick-to-1m flow features.

Use cases:

- flow-only modeling
- execution pressure analysis
- trade-activity regime signals

#### `gold.market.option_trades.m1`

Contains option tick-to-1m flow features.

Use cases:

- option flow regime modeling
- options activity pressure analysis
- option/perpetual flow comparison

#### `gold.market.core.m1`

Contains spot and perpetual feature families.

Use cases:

- forecasting
- volatility models
- regime detection

#### `gold.market.core_funding.m1`

Extends core with funding features.

Use cases:

- carry modeling
- crowding analysis
- market-neutral systems

#### `gold.market.full.m1`

Adds open interest, funding, perpetual trade flow, and option trade flow.

Use cases:

- advanced ML datasets
- systemic risk modeling
- leverage-state and flow-aware modeling

#### `gold.hybrid.full_l2.m1`

Extends full market datasets with L2 order book features.

Potential L2 features:

| Feature | Meaning |
|---|---|
| bid/ask imbalance | Liquidity pressure |
| spread | Market quality |
| order flow imbalance | Aggressive flow |
| microprice | Near-term directional bias |

---

## 6. Storage Layout

### 6.1 Bronze

```text
dataset_type=spot|perp|oi|funding|perp_trades|option_trades/
  exchange=<exchange>/
  instrument_type=<spot|perp>/
  symbol=<symbol>/
  timeframe=<interval|tick>/
  year=<YYYY>/
  month=<YYYY-MM>/
  date=<YYYY-MM-DD>/
  data.parquet
```

### 6.2 Silver

```text
dataset_type=<dataset>/
  exchange=<exchange>/
  symbol=<symbol>/
  timeframe=<interval>/
  year=<YYYY>/
  month=<YYYY-MM>/
  <SYMBOL>-<YYYY-MM>.parquet
```

### 6.3 Gold

```text
lake/gold/
  dataset_id=<dataset_id>/
  feature_set_version=<version>/
  exchange=<exchange>/
  symbol=<symbol>/
```

---

## 7. Repository Structure

```text
api/
application/
ingestion/
docs/
tests/
README.md
REPORT.md
AGENTS.md
```

| Directory | Responsibility |
|---|---|
| `api/` | CLI entrypoints |
| `application/` | Pipeline orchestration |
| `ingestion/` | Exchange connectors |
| `tests/` | Validation and regression tests |
| `docs/` | Figures and documentation |

Dataset metadata is centralized in `application/datasets.py`.
New Bronze datasets should start with a `DatasetSpec` entry defining CLI name, storage dataset type,
instrument type, symbol group, task kind, and default timeframe.

This allows Bronze planning to derive legacy fetch tuples while sharing:

- symbol validation
- deterministic scheduling
- checkpoint fingerprints
- run reporting behavior

---

## 8. Installation and Environment

### 8.1 System Prerequisites (Linux and Windows)

Because the repository is integrated tightly with GitHub workflows, install both `git` and GitHub CLI (`gh`) on all development machines.

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

Verify installation:

```bash
git --version
gh --version
```

### 8.2 Python Environment

```bash
uv sync --extra dev
```

The `dev` extra installs local quality-gate tooling used by pre-commit and CI-style checks:

- Ruff
- Mypy
- Pyright
- ty
- import-linter
- pytest
- pytest-cov
- pre-commit

Runtime configuration source:

```text
config.yaml
```

Recommended local permission:

```bash
chmod 600 config.yaml
```

---

## 9. Pipeline Execution

### 9.1 Full Medallion Pipeline

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

This executes all layers in sequence (`bronze-build` -> `silver-build` -> `gold-build`) using
`medallion-pipeline` settings from `config.yaml`.

Operational behavior:

- non-blocking single-run lock via `.run/full-pipeline.lock`
- shared append-only pipeline log output

### 9.2 Bronze Build

```bash
uv run python main.py bronze-build \
  --exchange deribit \
  --market spot perp oi funding perp_trades option_trades \
  --symbols BTC ETH SOL
```

Trade dataset symbol controls:

- `--symbols` for `spot`, `perp`, `oi`, `funding`
- `--perp-trade-symbols` for `perp_trades` (default: `BTC ETH SOL`)
- `--option-trade-symbols` for `option_trades` (default: `BTC ETH SOL`)

Bronze checkpoint path:

```text
.run/checkpoints/bronze-build.json
```

Checkpoint behavior:

- completed tasks are recorded incrementally
- reruns with the same effective plan skip completed tasks
- successful completion removes checkpoint automatically

Manual reset:

```bash
rm -f .run/checkpoints/bronze-build.json
```

Perpetual trade dataset migration:

- canonical dataset path is `dataset_type=perp_trades`
- legacy path `dataset_type=trades` must be migrated or backfilled before Silver

Example path rename:

```bash
mv lake/bronze/dataset_type=trades lake/bronze/dataset_type=perp_trades
```

Legacy-path validation:

```bash
uv run python scripts/check_legacy_trades_dataset.py --lake-root lake/bronze
```

### 9.3 Silver Build

```bash
uv run python main.py silver-build \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --exchange deribit \
  --market spot perp oi funding perp_trades option_trades \
  --timeframe 1m
```

### 9.4 Gold Build

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.market.full.m1
```

Gold source-selection policy:

- for each required upstream dataset, if equivalent symbol variants exist
  (for example `BTC`, `BTC-USDC`, `BTC-PERPETUAL` normalizing to one base symbol),
  Gold selects the newest matching variant by parquet modification time
- `gold.hybrid.full_l2.m1` applies the same newest-artifact policy for L2 inputs

Gold retention policy:

- keeps only latest `N` versions per `dataset_id/exchange/symbol` lineage (default `N=3`)
- configure via `gold-build.retention_keep_versions` in `config.yaml`
- optional CLI override: `--retention-keep-versions`

Available Gold dataset IDs:

- `gold.market.perp_trades.m1`
- `gold.market.option_trades.m1`
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.market.full.m1`
- `gold.hybrid.full_l2.m1`

---

## 10. Quality Gates

Recommended validation sequence:

```bash
uv run ruff check .
uv run mypy .
uv run pyright --level error
uv run ty check
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.yaml
uv run pytest
```

Notes:

- `pytest` uses coverage defaults from `pyproject.toml` for `application`, `ingestion`, and `api`
- pre-commit enforces the same test and coverage checks
- architectural import boundaries are validated by import-linter (`.importlinter`)
- runtime configuration schema is validated by Pydantic

---

## 11. Quant Research Usage

### 11.1 Regime Detection

Typical methods:

- Gaussian HMMs
- Markov-switching models
- volatility state estimation

High-signal features:

- perpetual returns
- open-interest changes
- funding
- realized volatility

### 11.2 Market-Neutral Strategies

Common feature sets:

- basis spreads
- funding carry
- leverage state
- rolling hedge ratios

### 11.3 Forecasting

Potential targets:

- realized volatility
- regime transitions
- volatility expansions
- return direction

### 11.4 Reinforcement Learning

Gold datasets provide:

- deterministic replay
- aligned feature grids
- reproducible state construction

---

## 12. Engineering Standards

The repository follows engineering rules defined in `AGENTS.md`.

Operational priorities:

- typed code and explicit contracts
- modular architecture and bounded side effects
- deterministic outputs
- reproducible storage and dataset lineage
- documentation consistency

Recommended tooling:

- pytest
- ruff
- mypy
- ty
- pyright

---

## 13. Extensions and Roadmap

### 13.1 Recommended Additional Features

| Feature | Importance |
|---|---|
| rolling z-scores | Regime normalization |
| realized volatility | Risk estimation |
| EWMA statistics | Adaptive state |
| entropy measures | Market disorder |
| rolling correlations | Dependency structure |
| volatility-of-volatility | Stress estimation |
| basis z-score | Relative-value modeling |
| rolling hedge ratios | Market-neutral trading |

Recommended regime diagnostics:

- HMM probabilities
- volatility state labels
- liquidity regime labels
- market stress indicators

### 13.2 Missing Datasets and Future Extensions

L2 order book data:

- highest-priority extension
- enables microstructure modeling and liquidity imbalance features

Liquidation data:

- captures forced flows and liquidation cascades
- improves leverage-flush diagnostics

Trade-level enrichment:

- enables signed volume and order-flow imbalance
- supports VPIN-style metrics

Options surface data:

- implied volatility
- skew
- term structure
- volatility expectations

Cross-exchange data:

- Binance vs Deribit spread analytics
- fragmented liquidity indicators
- cross-exchange funding divergence

### 13.3 Prioritized Roadmap

| Priority | Area |
|---|---|
| High | Full L2 ingestion |
| High | Multi-exchange support |
| High | Liquidation datasets |
| High | Cross-exchange basis features |
| Medium | Options surface ingestion |
| Medium | TimescaleDB integration |
| Medium | MLFlow lineage tracking |
| Medium | Streaming ingestion |
