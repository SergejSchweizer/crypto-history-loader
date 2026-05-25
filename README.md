# crypto-history-loader

Quant research data platform for historical crypto market features.

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Research Scope](#2-research-scope)
3. [Quant Data Architecture](#3-quant-data-architecture)
4. [Repository Map](#4-repository-map)
5. [Naming and Contracts](#5-naming-and-contracts)
6. [Dataset Wiki (Bronze -> Silver)](#6-dataset-wiki-bronze---silver)
 - [6.1 Spot OHLCV](#61-spot-ohlcv-dataset_typespot)
 - [6.2 Perpetual OHLCV](#62-perpetual-ohlcv-dataset_typeperp)
 - [6.3 Open Interest](#63-open-interest-dataset_typeoi)
 - [6.4 Funding](#64-funding-dataset_typefunding)
 - [6.5 Perp Tick Trades](#65-perp-tick-trades-dataset_typeperp_trades)
 - [6.6 Option Tick Trades](#66-option-tick-trades-dataset_typeoption_trades)
 - [6.7 Volatility Index Data](#67-volatility-index-data-dataset_typevolatility_index_data)
7. [Storage Contracts](#7-storage-contracts)
8. [Operations Runbook](#8-operations-runbook)
9. [Gold Retention Policy](#9-gold-retention-policy)
10. [Research Quality Gates](#10-research-quality-gates)

## 1. Executive Summary

`crypto-history-loader` is the historical data backbone for quant research workflows:

- deterministic historical ingestion (Bronze)
- feature-grade data curation (Silver)
- versioned model datasets (Gold)

Primary use cases:

- regime classification
- carry and crowding analysis
- microstructure flow analysis
- volatility and stress modeling

## 2. Research Scope

In scope:

- historical ingestion and backfill
- reproducible feature generation
- model-ready dataset versioning and provenance

Out of scope:

- live/streaming feature updates
- execution and order management

Live workflows belong to `crypto-live-loader`.

## 3. Quant Data Architecture

```text
Deribit APIs
 -> Bronze (raw normalized market history)
 -> Silver (validated/engineered features)
 -> Gold (versioned training/inference tables)
```

Design principles:

- deterministic scheduling
- explicit data contracts
- restart-safe ingestion
- auditable dataset lineage

## 4. Repository Map

```text
api/ CLI and command surfaces
application/ planning, orchestration, transformation services
ingestion/ exchange adapters and raw I/O
scripts/ operational automation
tests/ regression and contract validation
docs/ generated figures/tables
```

## 5. Naming and Contracts

Canonical terms:

- `symbol`: traded asset (e.g. `BTC`, `ETH`, `SOL`)
- `dataset`: conceptual data family
- `dataset_type`: physical storage/schema identifier

Registry source of truth:

- `application/datasets.py`

Start-date analysis note:

- Spot/perp/funding/oi/perp-trades baselines are derived from Deribit instrument availability.
- Option-trades/volatility-index baselines are derived from earliest endpoint-observed fetchable dates.
- Deribit spot observed starts (UTC, from `public/get_instruments` `creation_timestamp`): `BTC=2023-04-24`, `ETH=2023-04-24`, `SOL=2024-02-27`.

## 6. Dataset Wiki (Bronze -> Silver)

### 6.1 Spot OHLCV (`dataset_type=spot`)

Raw fields fetched:

- `exchange`, `symbol`, `timeframe`
- `open_time`, `close_time`
- `open_price`, `high_price`, `low_price`, `close_price`
- `volume`, `quote_volume`, `trade_count`

Feature meaning:

- `OHLC`: short-horizon direction and volatility envelope
- `volume`: participation intensity
- `quote_volume`: notional turnover proxy
- `trade_count`: activity proxy (adapter currently maps to `0`)

Market coverage:

- centralized spot order flow and price discovery for base assets versus quote currency (`*_USDC`)
- unlevered cash-market micro-regime used as anchor market state

Quant modeling use:

- baseline return/volatility factors for forecasting and regime labeling
- spot/perp basis spread construction and lead-lag analysis
- liquidity-aware feature normalization using spot turnover

Silver transforms:

- remove null/invalid candles
- dedupe by key (`exchange`, `instrument_type`, `symbol`, `timeframe`, `open_time`)
- output monthly `spot`

Coverage (baseline = oldest Deribit-offered start per dataset/symbol, as of `2026-05-25`):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC_USDC | 2023-04-24 | 2026-05-24 | 1 | 0.09% |
| ETH_USDC | 2023-04-24 | 2026-05-24 | 0 | 0.00% |
| SOL_USDC | 2024-02-27 | 2026-05-24 | 0 | 0.00% |

Column stats (Bronze snapshot):

| Column | Mean | Std % |
|---|---:|---:|
| open_price | 27087.505557 | 138.89% |
| high_price | 27090.254990 | 138.89% |
| low_price | 27084.626909 | 138.89% |
| close_price | 27087.427419 | 138.89% |
| volume | 2.336753 | 1042.28% |
| trade_count | 0.000000 | 0.00% |

### 6.2 Perpetual OHLCV (`dataset_type=perp`)

Raw fields fetched:

- `exchange`, `symbol`, `timeframe`
- `open_time`, `close_time`
- `open_price`, `high_price`, `low_price`, `close_price`
- `volume`, `quote_volume`, `trade_count`

Feature meaning:

- perp price path as leveraged sentiment proxy
- perp volume as speculative activity proxy
- spot/perp spread foundation for basis features

Market coverage:

- perpetual futures market where leverage and funding mechanics drive positioning
- dominant derivatives venue price path for directional and basis risk

Quant modeling use:

- carry/basis signals (perp vs spot) and momentum/mean-reversion features
- leveraged risk appetite proxies from perp turnover and range expansion
- regime segmentation of derivatives-led dislocations

Silver transforms:

- same validation/dedupe policy as spot
- output monthly `perp`

Coverage (baseline = oldest Deribit-offered start per dataset/symbol, as of `2026-05-25`):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2018-08-14 | 2026-05-24 | 1715 | 60.37% |
| ETH-PERPETUAL | 2019-03-14 | 2026-05-24 | 1503 | 57.17% |
| SOL-PERPETUAL | 2022-03-15 | 2026-05-24 | 714 | 46.61% |

Column stats (Bronze snapshot):

| Column | Mean | Std % |
|---|---:|---:|
| open_price | 27051.459881 | 139.20% |
| high_price | 27059.074116 | 139.20% |
| low_price | 27043.808969 | 139.20% |
| close_price | 27051.468613 | 139.20% |
| volume | 26.586912 | 532.50% |
| trade_count | 0.000000 | 0.00% |

### 6.3 Open Interest (`dataset_type=oi`)

Raw fields fetched:

- `exchange`, `symbol`, `timeframe`
- `open_time`, `close_time`
- `open_interest`, `open_interest_value`

Feature meaning:

- `open_interest`: total open leveraged exposure
- lagged OI dynamics indicate leverage build-up/unwind

Market coverage:

- aggregate outstanding perp derivatives exposure across open positions
- leverage stock variable complementary to flow variables (trades/funding)

Quant modeling use:

- leverage build-up / unwind signals for squeeze-risk and liquidation-risk models
- OI-price divergence factors for trend continuation vs exhaustion classifiers
- state variables in risk overlays and position sizing logic

Silver transforms:

- normalize and validate into `oi_observed`
- remove invalid rows
- dedupe observed rows
- build `oi_1m_feature` with backward as-of join
- emit forward-fill and lag diagnostics
- enforce leakage guard

Coverage (baseline = oldest Deribit-offered start per dataset/symbol, as of `2026-05-25`):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2018-08-14 | 2026-05-24 | 1715 | 60.37% |
| ETH-PERPETUAL | 2019-03-14 | 2026-05-24 | 1503 | 57.17% |
| SOL-PERPETUAL | 2022-03-15 | 2026-05-24 | 714 | 46.61% |

Column stats (Bronze snapshot):

| Column | Mean | Std % |
|---|---:|---:|
| open_interest | 490275819.887296 | 82.65% |
| open_interest_value | 0.000000 | 0.00% |

### 6.4 Funding (`dataset_type=funding`)

Raw fields fetched:

- `exchange`, `symbol`, `timeframe` (`8h` source interval)
- `open_time`, `close_time`
- `funding_rate`, `index_price`, `mark_price`

Feature meaning:

- `funding_rate`: crowding/carry pressure
- `index_price` vs `mark_price`: reference and risk-engine context

Market coverage:

- perpetual financing leg that equilibrates perp and spot through periodic payments
- market crowding and directional imbalance proxy in derivatives

Quant modeling use:

- carry factors for cross-sectional and time-series alpha models
- crowding stress indicators in drawdown/risk-off prediction models
- explanatory feature for basis compression/expansion dynamics

Silver transforms:

- build `funding_observed`
- validate and dedupe by funding timestamp
- build `funding_1m_feature` by backward as-of join
- derive time-since-funding and availability flags
- enforce leakage guard

Coverage (baseline = oldest Deribit-offered start per dataset/symbol, as of `2026-05-25`):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2018-08-14 | 2026-05-24 | 1714 | 60.33% |
| ETH-PERPETUAL | 2019-03-14 | 2026-05-24 | 1502 | 57.13% |
| SOL-PERPETUAL | 2022-03-15 | 2026-05-24 | 714 | 46.61% |

Column stats (Bronze snapshot):

| Column | Mean | Std % |
|---|---:|---:|
| funding_rate | 0.000067 | 226.87% |
| index_price | 27075.383297 | 138.93% |
| mark_price | 27074.798267 | 138.93% |

### 6.5 Perp Tick Trades (`dataset_type=perp_trades`)

Raw fields fetched:

- `exchange`, `symbol`, `instrument_type`
- `trade_id`, `trade_time`
- `price`, `quantity`, `side`, `is_maker`
- `source_endpoint`

Feature meaning:

- execution-level flow and aggressor pressure
- signed flow decomposition via buy/sell splits

Market coverage:

- tick-level perp transaction tape (aggressor side, size, execution price)
- high-frequency derivatives flow and microstructure pressure

Quant modeling use:

- order-flow imbalance and toxicity proxies for short-horizon return prediction
- realized volatility/jump nowcasting from trade intensity and size bursts
- execution-aware slippage and impact modeling inputs

Silver transforms:

- build `perp_trades_observed` with strict validation/dedupe
- aggregate to `perp_trades_1m_feature`
- derive OHLC, volume, quote volume, trade count
- derive buy/sell flow ratios and counts

Coverage (baseline = oldest Deribit-offered start per dataset/symbol, as of `2026-05-25`):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC-PERPETUAL | 2018-08-14 | 2026-05-24 | 2693 | 94.79% |
| ETH-PERPETUAL | 2019-03-14 | 2026-05-24 | 2595 | 98.71% |

Column stats (Bronze snapshot):

| Column | Mean | Std % |
|---|---:|---:|
| price | 36905.201716 | 69.98% |
| quantity | 3572.847416 | 913.33% |

### 6.6 Option Tick Trades (`dataset_type=option_trades`)

Raw fields fetched:

- trade fields above plus:
- `instrument_name`, `expiry`, `strike`, `option_type`

Feature meaning:

- option-specific flow by strike/expiry/moneyness context
- call/put-side pressure and participation

Market coverage:

- listed crypto options transaction tape across strikes, expiries, and call/put types
- volatility-demand and tail-hedging behavior of derivatives participants

Quant modeling use:

- skew/smile demand proxies via call-put flow asymmetry
- event-risk and convexity-demand state variables for regime models
- cross-market signal enrichment for perp/spot models during stress windows

Silver transforms:

- build `option_trades_observed`
- aggregate to `option_trades_1m_feature` with same minute logic as perp trades

Coverage (baseline = oldest Deribit-offered start per dataset/symbol, as of `2026-05-25`):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC | 2023-01-01 | 2026-05-24 | 895 | 72.18% |
| ETH | 2023-04-25 | 2026-05-24 | 881 | 78.24% |

Column stats (Bronze snapshot):

| Column | Mean | Std % |
|---|---:|---:|
| price | 0.018419 | 200.61% |
| quantity | 10.740162 | 1096.12% |
| strike | 21855.690986 | 91.64% |

### 6.7 Volatility Index Data (`dataset_type=volatility_index_data`)

Raw fields fetched:

- `exchange`, `symbol`, `timeframe`
- `open_time`, `close_time`
- `value`, `dataset_type`, `source_endpoint`

Feature meaning:

- implied/stress benchmark (DVOL-style index)
- risk-premium and stress-regime signal input

Market coverage:

- exchange-published implied volatility index representing aggregate option-implied risk
- forward-looking volatility state for BTC/ETH options complex

Quant modeling use:

- volatility regime labels and transition probabilities in state-space/HMM models
- risk-premium features when combined with realized volatility proxies
- portfolio risk targeting and volatility-scaling inputs

Silver transforms:

- build `volatility_index_data_observed`
- validate/dedupe and preserve source provenance

Coverage (baseline = oldest Deribit-offered start per dataset/symbol, as of `2026-05-25`):

| Symbol | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---:|---:|
| BTC | 2021-04-01 | 2026-05-24 | 1849 | 98.35% |
| ETH | 2021-04-01 | 2026-05-24 | 1849 | 98.35% |

Column stats (Bronze snapshot):

| Column | Mean | Std % |
|---|---:|---:|
| value | 46.678101 | 17.42% |

## 7. Storage Contracts

### Bronze

```text
dataset_type=<spot|perp|oi|funding|perp_trades|option_trades|volatility_index_data>/
 exchange=<exchange>/
 instrument_type=<spot|perp|option>/
 symbol=<symbol>/
 timeframe=<interval|tick>/
 year=<YYYY>/
 month=<YYYY-MM>/
 date=<YYYY-MM-DD>/
 data.parquet
```

### Silver

```text
dataset_type=<dataset>/
 exchange=<exchange>/
 symbol=<symbol>/
 timeframe=<interval>/
 year=<YYYY>/
 month=<YYYY-MM>/
 <SYMBOL>-<YYYY-MM>.parquet
```

### Gold

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

## 8. Operations Runbook

Install:

```bash
uv sync --extra dev
```

Bronze:

```bash
uv run python main.py --debug bronze-build \
 --exchange deribit \
 --market spot perp oi funding perp_trades option_trades volatility_index_data \
 --symbols BTC ETH SOL \
 --full-gap-fill \
 --save-parquet-lake \
 --no-json-output
```

Trade symbol inheritance:

- `--perp-trade-symbols` defaults to `--symbols` if omitted
- `--option-trade-symbols` defaults to `--symbols` if omitted

Silver:

```bash
uv run python main.py silver-build \
 --bronze-root lake/bronze \
 --silver-root lake/silver \
 --exchange deribit \
 --market spot perp oi funding perp_trades option_trades volatility_index_data \
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

Pipeline orchestration:

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

Note: pipeline script currently applies a rolling six-month Bronze clamp unless changed.

## 9. Gold Retention Policy

Configured via `--retention-keep-versions` (default `3`).

Retention enforcement is dual:

- keep latest `N` `feature_set_version=*` directories per `dataset_id/exchange/symbol`
- keep latest `N` artifact stem groups (`.parquet/.json/.png`) per `dataset_id/exchange/symbol`

## 10. Research Quality Gates

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pyright --level error
uv run ty check
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.yaml
uv run pytest
```
