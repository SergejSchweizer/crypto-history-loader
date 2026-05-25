# crypto-history-loader

Production-grade cryptocurrency market data ingestion, normalization, feature engineering, and dataset generation framework for quantitative research and systematic trading.

---

# Table Of Contents

- [crypto-history-loader](#crypto-history-loader)
- [Table Of Contents](#table-of-contents)
- [1. Project Goals](#1-project-goals)
- [2. System Overview](#2-system-overview)
  - [2.1 Core Design Principles](#21-core-design-principles)
  - [2.2 Medallion Architecture](#22-medallion-architecture)
  - [2.3 Supported Data Domains](#23-supported-data-domains)
- [3. Repository Structure](#3-repository-structure)
- [4. Installation](#4-installation)
- [5. Pipeline Architecture](#5-pipeline-architecture)
  - [5.1 Bronze Layer](#51-bronze-layer)
  - [5.2 Silver Layer](#52-silver-layer)
  - [5.3 Gold Layer](#53-gold-layer)
  - [5.4 Layer Transitions](#54-layer-transitions)
- [6. Dataset Definitions](#6-dataset-definitions)
  - [6.1 Spot OHLCV](#61-spot-ohlcv)
  - [6.2 Perpetual OHLCV](#62-perpetual-ohlcv)
  - [6.3 Open Interest](#63-open-interest)
  - [6.4 Funding Rate](#64-funding-rate)
  - [6.5 Perp Tick Trades](#65-perp-tick-trades)
  - [6.6 Option Tick Trades](#66-option-tick-trades)
  - [6.7 Historical Volatility](#67-historical-volatility)
  - [6.8 Volatility Index Data](#68-volatility-index-data)
- [7. Quantitative Interpretation Of Features](#7-quantitative-interpretation-of-features)
  - [Price Features](#price-features)
  - [Volume Features](#volume-features)
  - [Trade Flow Features](#trade-flow-features)
  - [Funding Features](#funding-features)
  - [Open Interest Features](#open-interest-features)
  - [Cross-Market Features](#cross-market-features)
- [8. Gold Dataset Definitions](#8-gold-dataset-definitions)
  - [gold.market.option_trades.m1](#goldmarketoption_tradesm1)
  - [gold.market.perp_trades.m1](#goldmarketperp_tradesm1)
  - [gold.market.core.m1](#goldmarketcorem1)
  - [gold.market.core\_funding.m1](#goldmarketcore_fundingm1)
  - [gold.market.full.m1](#goldmarketfullm1)
  - [gold.hybrid.full\_l2.m1](#goldhybridfull_l2m1)
- [9. Recommended Additional Features](#9-recommended-additional-features)
- [11. Storage Layout](#11-storage-layout)
  - [Bronze Layout](#bronze-layout)
  - [Silver Layout](#silver-layout)
  - [Gold Layout](#gold-layout)
- [12. Example Commands](#12-example-commands)
  - [Full Medallion Pipeline (Bronze+Silver+Gold)](#full-medallion-pipeline-bronzesilvergold)
  - [Bronze Build](#bronze-build)
  - [Silver Build](#silver-build)
  - [Gold Build](#gold-build)
- [13. Quant Research Usage](#13-quant-research-usage)
  - [Regime Detection](#regime-detection)
  - [Market-Neutral Strategies](#market-neutral-strategies)
  - [Forecasting](#forecasting)
  - [Reinforcement Learning](#reinforcement-learning)
- [14. Engineering Standards](#14-engineering-standards)
- [15. Roadmap](#15-roadmap)

---

# 1. Project Goals

`crypto-history-loader` is designed as a reproducible market data platform for cryptocurrency quantitative research.

Primary goals:

- deterministic ingestion
- schema-stable parquet datasets
- reproducible feature engineering
- medallion architecture separation
- ML-ready dataset generation
- scalable historical backfills
- quantitative research workflows

The repository is intended for:

- systematic trading
- market-neutral research
- volatility forecasting
- HMM regime detection
- reinforcement learning
- feature engineering pipelines
- derivatives analytics

---

# 2. System Overview

## 2.1 Core Design Principles

The repository follows the engineering principles defined in `AGENTS.md`:

- maintainability
- modularity
- reproducibility
- deterministic processing
- idempotent ingestion
- explicit interfaces
- production-grade architecture

## 2.2 Medallion Architecture

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

## 2.3 Supported Data Domains

| Dataset | Description |
|---|---|
| Spot OHLCV | Physical spot market |
| Perpetual OHLCV | Leveraged perpetual futures |
| Funding | Long/short positioning pressure |
| Open Interest | Aggregate leveraged exposure |
| Perp Tick Trades | Historical perpetual trade-by-trade prints (REST backfill) |
| Option Tick Trades | Historical option trade prints (REST backfill) |
| Historical Volatility | Deribit historical volatility time series |
| Volatility Index Data | Deribit DVOL-style index series |

Current exchange support:

- Deribit

Default symbol sets in `config.yaml`:

- BTC
- ETH
- SOL

CLI parser fallback defaults (used when config does not override):

- `--symbols`: `BTCUSDT ETHUSDT`
- `--perp-trade-symbols`: defaults to `--symbols` when omitted
- `--option-trade-symbols`: defaults to `--symbols` when omitted

Canonical nomenclature used across this repository and documentation:

- `symbol`: traded asset symbol (for example `BTC`, `ETH`, `SOL`)
- `dataset`: data family (for example `spot`, `perp`, `oi`, `funding`)

Implementation note:

- `dataset_type` is the internal storage/schema field name for a dataset identifier in partitioned paths and records.
- Prefer `symbol` and `dataset` in user-facing docs, CLI explanations, and architecture discussions.

---

# 3. Repository Structure

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

Dataset metadata is centralized in `application/datasets.py`. New Bronze datasets should start with a
`DatasetSpec` entry that defines the CLI name, storage dataset identifier (`dataset_type`), instrument type, symbol group,
task kind, and default timeframe. Bronze planning derives legacy fetch tuples from these specs, so
new datasets can share symbol validation, deterministic scheduling, checkpoint fingerprints, and
reporting behavior instead of duplicating one-off planner logic.

---

# 4. Installation

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

# 5. Pipeline Architecture

## 5.1 Bronze Layer

Bronze stores raw normalized exchange data.

Properties:

- append-oriented
- deterministic
- audit-friendly
- minimal transformations
- preserves source fidelity

Bronze stores:

- OHLCV candles
- funding events
- open interest observations
- historical volatility observations
- volatility index observations
- perp tick trades (historical REST backfill)
- option tick trades (historical REST backfill)

## 5.2 Silver Layer

Silver transforms raw records into engineered feature datasets.

Responsibilities:

- rolling statistics
- volatility features
- funding transformations
- OI transformations
- trade-tick to 1m aggregation
- canonical resampling
- forward filling
- feature manifests

## 5.3 Gold Layer

Gold produces final modeling datasets.

Responsibilities:

- canonical 1-minute alignment
- joining feature families
- latest-source selection for equivalent upstream variants (newest artifact per required dataset)
- versioned datasets
- plot generation
- manifests/provenance

## 5.4 Layer Transitions

Bronze to Silver transition:

- input: append-only normalized Bronze partitions
- processing: deterministic aggregations and feature derivations per dataset spec
- output: schema-stable Silver feature tables partitioned by dataset and time

Silver to Gold transition:

- input: required Silver feature families for each Gold dataset contract
- processing: canonical 1-minute alignment, joins, and latest-source selection
- output: versioned Gold datasets with manifests and reproducible provenance

---

# 6. Dataset Definitions

## 6.1 Spot OHLCV

Represents the underlying physical market.

Deribit fetched columns written to Bronze:

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Normalized spot instrument (for example `BTC_USDC`) |
| timeframe | Candle interval (`1m`) |
| open_time | Candle open timestamp (UTC) |
| close_time | Candle close timestamp (UTC) |
| open_price | First traded price |
| high_price | Highest traded price |
| low_price | Lowest traded price |
| close_price | Last traded price |
| volume | Base asset turnover |
| quote_volume | Quote turnover (can be null from source) |
| trade_count | Trade count (Deribit chart endpoint currently maps as `0`) |

Silver transformations applied:

- Bronze rows with null OHLC prices are removed.
- Invalid OHLC rows are removed (`high < max(open, close)` or `low > min(open, close)`).
- Duplicate candles are collapsed by (`exchange`, `instrument_type`, `symbol`, `timeframe`, `open_time`) keeping last by `ingested_at`.
- Monthly Silver output is written as dataset `spot` with canonical OHLCV schema.

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC_USDC | 2023-01-01 | 2026-05-24 | 114 | 9.19% |
| ETH_USDC | 2023-01-01 | 2026-05-24 | 113 | 9.11% |
| SOL_USDC | 2023-01-01 | 2026-05-24 | 422 | 34.03% |

Quantitative importance:

- baseline market direction
- volatility estimation
- trend structure
- lead/lag modeling
- spot/perp basis analysis

## 6.2 Perpetual OHLCV

Represents leveraged perpetual futures trading.

Deribit fetched columns written to Bronze:

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Normalized perp instrument (for example `BTC-PERPETUAL`) |
| timeframe | Candle interval (`1m`) |
| open_time | Candle open timestamp (UTC) |
| close_time | Candle close timestamp (UTC) |
| open_price | First traded price |
| high_price | Highest traded price |
| low_price | Lowest traded price |
| close_price | Last traded price |
| volume | Base asset turnover |
| quote_volume | Quote turnover (can be null from source) |
| trade_count | Trade count (Deribit chart endpoint currently maps as `0`) |

Silver transformations applied:

- Bronze rows with null OHLC prices are removed.
- Invalid OHLC rows are removed (`high < max(open, close)` or `low > min(open, close)`).
- Duplicate candles are collapsed by (`exchange`, `instrument_type`, `symbol`, `timeframe`, `open_time`) keeping last by `ingested_at`.
- Monthly Silver output is written as dataset `perp` with canonical OHLCV schema.

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2023-01-01 | 2026-05-24 | 114 | 9.19% |
| ETH-PERPETUAL | 2023-01-01 | 2026-05-24 | 114 | 9.19% |
| SOL-PERPETUAL | 2023-01-01 | 2026-05-24 | 422 | 34.03% |

Important because perpetuals often lead spot markets during:

- liquidations
- leverage expansions
- speculative squeezes
- volatility events

Potential feature groups:

| Feature | Interpretation |
|---|---|
| perp returns | Leveraged directional pressure |
| perp volume | Speculative participation |
| basis vs spot | Carry and leverage state |
| volatility | Market stress |

## 6.3 Open Interest

Open Interest measures total leveraged exposure.

Deribit fetched columns written to Bronze:

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Normalized perp instrument |
| timeframe | Requested interval (stored as `1m`) |
| open_time | Observation timestamp (UTC) |
| close_time | Same as `open_time` for snapshot rows |
| open_interest | Position/open-interest value from Deribit settlements feed |
| open_interest_value | Placeholder value (`0.0` in current adapter) |

Silver transformations applied:

- Cast and normalize to Silver observed schema (`timestamp`, normalized `symbol`, lowercase `exchange`).
- Invalid rows are removed (null/empty symbol, null/non-finite/negative `open_interest`, null timestamp).
- Deduplicate by (`exchange`, `symbol`, `timestamp`, `open_interest`) keeping last.
- Write observed dataset `oi_observed` with `oi_source_timestamp`.
- Build `oi_1m_feature` via backward `asof` join onto 1-minute calendar with:
- `oi_is_observed`, `oi_is_ffill`, `minutes_since_oi_observation`, `oi_observation_lag_sec`.
- Enforce leakage guard: no row may use future OI observation (`oi_source_timestamp > timestamp_m1`).

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2023-01-01 | 2026-05-24 | 114 | 9.19% |
| ETH-PERPETUAL | 2023-01-01 | 2026-05-24 | 114 | 9.19% |
| SOL-PERPETUAL | 2023-01-01 | 2026-05-24 | 422 | 34.03% |

Important conceptual distinction:

| Concept | Meaning |
|---|---|
| observed OI | Native exchange observation |
| OI 1m feature | Forward-filled modeling feature |

Quantitative interpretation:

| Price | OI | Meaning |
|---|---|
| Up | Up | New longs entering |
| Down | Up | New shorts entering |
| Up | Down | Short covering |
| Down | Down | Long liquidation |

OI is extremely important for:

- leverage regime detection
- squeeze prediction
- volatility forecasting
- systemic stress estimation

## 6.4 Funding Rate

Funding transfers capital between longs and shorts.

Deribit fetched columns written to Bronze:

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Normalized perp instrument |
| timeframe | Native funding interval (`8h`) |
| open_time | Funding event timestamp (UTC) |
| close_time | Same as `open_time` for event rows |
| funding_rate | Funding rate (`interest_8h`, with source fallback handling) |
| index_price | Index price from source event |
| mark_price | Previous index/mark proxy from source (falls back to `index_price` when null) |

Silver transformations applied:

- Restrict to `instrument_type=perp`.
- Remove rows with null funding rates and invalid funding rates (non-finite or absolute value > 1.0).
- Aggregate/dedupe by (`exchange`, `symbol`, `funding_time`) using last `funding_rate`.
- Emit `funding_observed` with `base_asset`, `funding_interval_hours`, ingest min/max, source row count, quality status.
- Build `funding_1m_feature` via backward `asof` join onto 1-minute calendar with:
- `funding_rate_last_known`, `funding_observed_at`, `minutes_since_funding`,
- `is_funding_observation_minute`, `funding_data_available`.
- Enforce leakage guard: no row may use future funding event (`funding_observed_at > timestamp`).

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2023-01-01 | 2026-05-24 | 113 | 9.11% |
| ETH-PERPETUAL | 2023-01-01 | 2026-05-24 | 113 | 9.11% |
| SOL-PERPETUAL | 2023-01-01 | 2026-05-24 | 422 | 34.03% |

Interpretation:

| Funding State | Market Meaning |
|---|---|
| Positive funding | Long crowding |
| Negative funding | Short crowding |
| Neutral funding | Balanced positioning |

Funding is highly valuable for:

- carry strategies
- market-neutral trading
- crowding analysis
- mean reversion systems
- regime detection

## 6.5 Perp Tick Trades

Perp tick trades represent per-execution perpetual market prints.

Deribit fetched columns written to Bronze (`perp_trades`):

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Normalized perp symbol (base asset canonicalized) |
| instrument_type | `perp` |
| trade_id | Trade identifier |
| trade_time | Execution timestamp (UTC) |
| price | Executed price |
| quantity | Executed amount (`amount` in Deribit payload) |
| side | Trade direction (`buy`/`sell`/`unknown`) |
| is_maker | Maker flag derived from source liquidation marker |
| source_endpoint | Source route tag (`public_trades`) |

Silver transformations applied:

- Build observed ticks (`perp_trades_observed`) by casting/normalizing fields and filtering invalid rows:
- required: non-null `trade_time`, `trade_id`, finite positive `price` and `quantity`.
- Deduplicate observed ticks by (`exchange`, `instrument_type`, `symbol`, `trade_time`, `trade_id`) keeping last by `ingested_at`.
- Build 1-minute features (`perp_trades_1m_feature`) grouped by minute/exchange/symbol/instrument_type:
- OHLC from `price`, `volume` from `quantity`, `quote_volume = sum(price*quantity)`, `trade_count`,
- `buy_volume`, `sell_volume`, `buy_trade_count`, `sell_trade_count`, `buy_volume_share`.

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2023-01-01 | 2026-05-24 | 1125 | 90.73% |
| ETH-PERPETUAL | 2023-01-01 | 2026-05-24 | 1206 | 97.26% |

Silver builds `perp_trades_1m_feature` from tick data and derives:

| Feature | Meaning |
|---|---|
| open/high/low/close | Minute-level trade-price path |
| volume / quote_volume | Executed flow intensity |
| trade_count | Activity/participation |
| buy/sell volume + counts | Directional aggressor pressure proxy |
| buy_volume_share | Buy-side flow dominance |

## 6.6 Option Tick Trades

Option tick trades represent per-execution option market prints.

Silver builds `option_trades_1m_feature` from `option_trades_observed` and preserves option-contract context
(`instrument_name`, strike, expiry, option side) alongside flow and participation features.

Deribit fetched columns written to Bronze (`option_trades`):

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Canonical underlying symbol (`BTC`/`ETH`/`SOL`) |
| instrument_type | `option` |
| instrument_name | Full option contract id from Deribit |
| expiry | Parsed contract expiry token |
| strike | Parsed strike value |
| option_type | Parsed contract side (`call`/`put`/`unknown`) |
| trade_id | Trade identifier |
| trade_time | Execution timestamp (UTC) |
| price | Executed price |
| quantity | Executed amount (`amount` in Deribit payload) |
| side | Trade direction (`buy`/`sell`/`unknown`) |
| is_maker | Maker flag derived from source liquidation marker |
| source_endpoint | Source route tag (`public_option_trades`) |

Silver transformations applied:

- Build observed ticks (`option_trades_observed`) with the same validation + dedupe logic as perp trades.
- Build 1-minute features (`option_trades_1m_feature`) using the same minute aggregation logic as perp trades:
- OHLC, volume, quote_volume, trade_count, buy/sell splits, buy_volume_share.

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC | 2023-01-01 | 2026-05-24 | 895 | 72.18% |
| ETH | 2023-01-01 | 2026-05-24 | 995 | 80.24% |

## 6.7 Historical Volatility

Historical volatility captures exchange-published rolling realized-volatility series.

Deribit fetched columns written to Bronze (`historical_volatility`):

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Canonical base symbol (`BTC`/`ETH`/`SOL`) |
| timeframe | Requested interval (typically `1m`) |
| open_time | Observation timestamp (UTC) |
| close_time | Same as `open_time` for point rows |
| value | Volatility value from Deribit historical volatility endpoint |
| source_endpoint | `public_get_historical_volatility` |
| dataset_type | `historical_volatility` |

Silver transformations applied:

- Cast and normalize to observed schema (`timestamp`, normalized symbol/exchange/instrument_type).
- Remove invalid rows (null timestamp/symbol, null/non-finite/negative `volatility_value`).
- Deduplicate by (`exchange`, `symbol`, `dataset_type`, `timestamp`) keeping last.
- Write observed dataset `historical_volatility_observed` with `volatility_source_timestamp`.

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC | 2023-01-01 | 2026-05-24 | 1223 | 98.63% |
| ETH | 2023-01-01 | 2026-05-24 | 1223 | 98.63% |
| SOL | 2023-01-01 | 2026-05-24 | 1223 | 98.63% |

This dataset is useful for:

- volatility regime detection
- realized vs implied context
- feature conditioning for risk-aware models
- volatility clustering analysis

## 6.8 Volatility Index Data

Volatility index data captures DVOL-style market volatility benchmarks.

Deribit fetched columns written to Bronze (`volatility_index_data`):

| Field | Meaning |
|---|---|
| exchange | Exchange id (`deribit`) |
| symbol | Canonical base symbol (`BTC`/`ETH`/`SOL`) |
| timeframe | Requested resolution (typically `1m`) |
| open_time | Observation timestamp (UTC) |
| close_time | Same as `open_time` for point rows |
| value | Volatility index value (`index_value`) from Deribit |
| source_endpoint | `public_get_volatility_index_data` |
| dataset_type | `volatility_index` in raw rows (stored under Bronze dataset `volatility_index_data`) |

Silver transformations applied:

- Cast and normalize to observed schema (`timestamp`, normalized symbol/exchange/instrument_type).
- Remove invalid rows (null timestamp/symbol, null/non-finite/negative `volatility_value`).
- Deduplicate by (`exchange`, `symbol`, `dataset_type`, `timestamp`) keeping last.
- Write observed dataset `volatility_index_data_observed` with `volatility_source_timestamp`.

Coverage snapshot (Bronze partition-day coverage, as of 2026-05-25):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC | 2023-01-01 | 2026-05-24 | 1209 | 97.50% |
| ETH | 2023-01-01 | 2026-05-24 | 1209 | 97.50% |

This dataset is useful for:

- market stress state tracking
- derivatives risk-premium analysis
- cross-market volatility spillover features
- regime labeling inputs

---

# 7. Quantitative Interpretation Of Features

## Price Features

Describe:

- trend
- momentum
- volatility clustering
- regime shifts

## Volume Features

Describe:

- participation intensity
- speculative activity
- stress conditions
- liquidity conditions

## Trade Flow Features

Describe:

- execution-level pressure
- buy/sell imbalance
- participation bursts
- short-horizon microstructure regime shifts

## Funding Features

Describe:

- directional crowding
- leverage imbalance
- carry state
- sentiment extremes

## Open Interest Features

Describe:

- leverage expansion
- leverage unwinds
- liquidation risk
- structural market stress

## Cross-Market Features

Most powerful features usually come from interactions:

| Combination | Interpretation |
|---|---|
| spot/perp spread | Futures premium |
| funding + OI | Crowded leverage |
| OI + volatility | Fragile market state |
| volume + funding | Speculative frenzy |

---

# 8. Gold Dataset Definitions

## gold.market.perp_trades.m1

Contains:

- perp trades (`perp_trades_1m_feature`, tick-to-1m flow features)

Use cases:

- perp-flow-only modeling
- execution pressure analysis
- trade-activity regime signals

## gold.market.option_trades.m1

Contains:

- option trades (`option_trades_1m_feature`, tick-to-1m flow features)

Use cases:

- option flow regime modeling
- options activity pressure analysis
- option/perp flow comparison studies

## gold.market.core.m1

Contains:

- spot features
- perpetual features

Use cases:

- forecasting
- volatility models
- regime detection

## gold.market.core_funding.m1

Adds:

- funding features

Use cases:

- carry modeling
- crowding analysis
- market-neutral systems

## gold.market.full.m1

Adds:

- open interest
- funding
- perp trades (`perp_trades_1m_feature`, tick-to-1m flow features)
- option trades (`option_trades_1m_feature`, tick-to-1m flow features)
- full derivatives state

Use cases:

- advanced ML
- systemic risk modeling
- leverage-state analysis
- flow-aware leverage-state modeling

## gold.hybrid.full_l2.m1

Extends gold datasets with L2 order book features.
Includes spot/perp/funding/open-interest/perp-trades-derived 1m features plus L2.

Potential L2 features:

| Feature | Meaning |
|---|---|
| bid/ask imbalance | Liquidity pressure |
| spread | Market quality |
| order flow imbalance | Aggressive flow |
| microprice | Near-term directional bias |

---

# 9. Recommended Additional Features

Recommended additions based on existing historical datasets in this repository:

| Feature | Importance |
|---|---|
| rolling z-scores (price/volume/funding/OI) | Regime normalization |
| realized volatility (multi-horizon, e.g. 5m/30m/4h/1d) | Risk estimation |
| EWMA mean/variance (spot/perp returns) | Adaptive state tracking |
| basis level + basis z-score (perp vs spot) | Relative-value modeling |
| funding regime features (quantiles, persistence, mean-reversion distance) | Crowding diagnostics |
| OI change features (`ΔOI`, `ΔOI/volume`, rolling OI momentum) | Leverage state detection |
| perp/option flow imbalance (buy-share, signed notional, trade-intensity shocks) | Flow pressure signals |
| historical-volatility vs volatility-index spread | Vol risk-premium proxy |
| volatility-of-volatility from `historical_volatility` / `volatility_index_data` | Stress estimation |
| rolling cross-feature correlations (returns vs funding/OI/flow) | Dependency structure |

Recommended regime features:

- HMM probabilities
- volatility state labels
- liquidity regime labels
- market stress indicators

Scope note:

- These recommendations are designed for historical Bronze/Silver/Gold processing in `crypto-history-loader`.
- Live/streaming feature recommendations are intentionally excluded (handled in `crypto-live-loader`).

---

# 11. Storage Layout

## Bronze Layout

```text
dataset_type=spot|perp|oi|funding|historical_volatility|volatility_index_data|perp_trades|option_trades/
  exchange=<exchange>/
  instrument_type=<spot|perp|option>/
  symbol=<symbol>/
  timeframe=<interval|tick>/
  year=<YYYY>/
  month=<YYYY-MM>/
  date=<YYYY-MM-DD>/
  data.parquet
```

## Silver Layout

```text
dataset_type=<dataset>/
  exchange=<exchange>/
  symbol=<symbol>/
  timeframe=<interval>/
  year=<YYYY>/
  month=<YYYY-MM>/
  <SYMBOL>-<YYYY-MM>.parquet
```

## Gold Layout

```text
lake/gold/
  dataset_id=<dataset_id>/
  dataset_type=gold_symbol_dataset/
  feature_set_version=<version>/
  exchange=<exchange>/
  symbol=<symbol>/
  <SYMBOL>_GOLD_<feature_set_hash>_<source_data_hash>.parquet
  <SYMBOL>_GOLD_<feature_set_hash>_<source_data_hash>.json
  <SYMBOL>_GOLD_<feature_set_hash>_<source_data_hash>.png
```

---

# 12. Example Commands

## Full Medallion Pipeline (Bronze+Silver+Gold)

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

This script runs all three layers in sequence (`bronze-build` -> `silver-build` -> `gold-build`)
using `medallion-pipeline` settings from `config.yaml`. It also enforces a non-blocking single-run
lock via `.run/full-pipeline.lock` and writes a shared append-only pipeline log.

Current default behavior in `scripts/run_medallion_pipeline.py`:

- Bronze `--market` is auto-enriched to include both `historical_volatility` and `volatility_index_data`.
- Bronze date bounds are clamped to a rolling six-month window (`--start-date` and symbol date overrides).

## Bronze Build

```bash
uv run python main.py bronze-build \
  --exchange deribit \
  --market spot perp oi funding perp_trades option_trades historical_volatility volatility_index_data \
  --symbols BTC ETH SOL
```

`--debug` is a global CLI flag and must be placed before the command name:

```bash
uv run python main.py --debug bronze-build --market spot
```

Trade datasets can use independent symbol defaults and overrides:

- `--symbols` applies to `spot`, `perp`, `oi`, `funding`
- `--perp-trade-symbols` applies to `perp_trades` (defaults to `--symbols` when omitted)
- `--option-trade-symbols` applies to `option_trades` (defaults to `--symbols` when omitted)

Example: fetch only `perp_trades` and `option_trades` datasets and persist parquet outputs:

```bash
uv run python main.py --debug bronze-build \
  --market perp_trades option_trades \
  --perp-trade-symbols BTC ETH SOL \
  --option-trade-symbols BTC ETH SOL \
  --save-parquet-lake \
  --no-json-output
```

### Bronze Resume Checkpoint

`bronze-build` writes a restart checkpoint at:

```text
.run/checkpoints/bronze-build.json
```

Behavior:

- Completed tasks are recorded incrementally during the run.
- If a run fails or is interrupted, the next run with the same effective plan resumes by skipping completed tasks.
- If all tasks complete successfully, the checkpoint is deleted automatically.

Manual reset:

```bash
rm -f .run/checkpoints/bronze-build.json
```

## Silver Build

```bash
uv run python main.py silver-build \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --exchange deribit \
  --market spot perp oi funding perp_trades option_trades historical_volatility volatility_index_data \
  --timeframe 1m
```

Silver currently builds:

- `spot` and `perp` OHLCV monthly outputs
- `oi_observed`
- `funding_observed`
- `historical_volatility_observed`
- `volatility_index_data_observed`
- `perp_trades_observed` and `perp_trades_1m_feature`
- `option_trades_observed` and `option_trades_1m_feature`

## Gold Build

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.market.full.m1
```

Gold contracts currently include `historical_volatility_observed` and `volatility_index_data_observed`
as required upstream feature families where applicable.

Source selection policy for gold combinations:

- For each required upstream dataset (`spot`, `perp`, `oi_1m_feature`, `funding_1m_feature`, `perp_trades_1m_feature`, `option_trades_1m_feature`), if multiple equivalent symbol variants exist
  (for example `BTC`, `BTC-USDC`, `BTC-PERPETUAL` that normalize to the same base symbol), gold selects the newest
  matching variant by parquet file modification time and uses only that variant for the join.
- For `gold.hybrid.full_l2.m1`, L2 input also uses the newest matching artifact.

Gold retention policy:

- Gold keeps only the latest `N` versions per `dataset_id/exchange/symbol` lineage (default `N=3`).
- Configure via `gold-build.retention_keep_versions` in `config.yaml` or override with
  `--retention-keep-versions`.

Additional Gold Dataset IDs:

- `gold.market.perp_trades.m1` (perp trade flow features only)
- `gold.market.option_trades.m1` (option trade flow features only)
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.hybrid.full_l2.m1`

## Quality Checks

```bash
uv run ruff check .
uv run mypy .
uv run pyright --level error
uv run ty check
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.yaml
uv run pytest
```

`pytest` includes coverage reporting for `application`, `ingestion`, and `api` via
`pyproject.toml` defaults. The same test+coverage command is enforced in `.pre-commit-config.yaml`.
Architecture import boundaries are validated with `import-linter` using `.importlinter`.
Runtime configuration schema is validated with Pydantic via `scripts/validate_config_with_pydantic.py`.

---

# 13. Quant Research Usage

## Regime Detection

Useful for:

- Gaussian HMMs
- Markov-switching models
- volatility state estimation

Most important features:

- perp returns
- OI changes
- funding
- realized volatility

## Market-Neutral Strategies

Important features:

- basis spreads
- funding carry
- leverage state
- hedge ratios

## Forecasting

Potential targets:

- realized volatility
- regime transitions
- volatility expansions
- return direction

## Reinforcement Learning

Gold datasets provide:

- deterministic replay
- aligned feature grids
- reproducible state spaces

---

# 14. Engineering Standards

The repository follows the engineering rules defined in `AGENTS.md`.

Important principles:

- typed code
- modular design
- reproducibility
- scalable storage
- deterministic outputs
- documentation consistency

Recommended tooling:

- pytest
- ruff
- mypy
- ty
- pyright

Current default type-check policy is strict:

- `mypy`: `strict = true`
- `pyright`: `typeCheckingMode = "strict"`

---

# 15. Roadmap

Recommended future directions:

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
