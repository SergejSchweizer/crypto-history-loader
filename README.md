# CRYPTO-HISTORY-LOADER

Quant research data platform for historical crypto market features.

Author: Sergej Schweizer

---

## Table Of Contents

- [1. System Overview](#1-system-overview)
  - [1.1 Core Design Principles](#11-core-design-principles)
  - [1.2 Medallion Architecture](#12-medallion-architecture)
  - [1.3 Supported Data Domains](#13-supported-data-domains)
- [2. Repository Structure](#2-repository-structure)
- [3. Installation](#3-installation)
  - [3.1 System prerequisites](#31-system-prerequisites)
  - [3.2 Python environment setup](#32-python-environment-setup)
- [4. Raw Datasets](#4-raw-datasets)
  - [4.1 Spot (`dataset_type=spot_ohlcv`)](#41-spot_ohlcv-dataset_typespot_ohlcv)
  - [4.2 Perpetual (`dataset_type=perps_ohlcv`)](#42-perpetual-dataset_typeperps_ohlcv)
  - [4.3 Open Interest (`dataset_type=open_interest`)](#43-open-interest-dataset_typeoi)
  - [4.4 Funding (`dataset_type=funding`)](#44-funding-dataset_typefunding)
  - [4.5 `perps_trades` (`dataset_type=perps_trades`)](#45-perps_trades-dataset_typeperps_trades)
  - [4.6 `options_trades` (`dataset_type=options_trades`)](#46-options_trades-dataset_typeoptions_trades)
- [5. Example Commands](#5-example-commands)
  - [5.1 End-to-End Pipeline](#51-end-to-end-pipeline)
  - [5.2 Layer Commands](#52-layer-commands)
  - [5.3 Operational Notes](#53-operational-notes)
  - [5.4 Quality Checks](#54-quality-checks)
- [6. Roadmap](#6-roadmap)

`crypto-history-loader` is the historical data backbone for quant research workflows:

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

The durable package-boundary, data-flow, side-effect, and update rules live in
[`ARCHITECTURE.md`](ARCHITECTURE.md). Update that document in the same change set whenever dataset
contracts, layer ownership, medallion paths, runtime configuration, or quality gates change.

## 1.3 Supported Data Domains

Supported ingest domains are defined by `DATASET_REGISTRY` in `application/datasets.py`.

### Domain Groups

OHLCV:

| CLI Domain | Bronze `dataset_type` | Instrument Type | Task Kind | Default Timeframe | Symbol Source | Description |
|---|---|---|---|---|---|---|
| `spot_ohlcv` | `spot_ohlcv` | `spot_ohlcv` | `ohlcv` | `1m` | `--symbols` | Physical spot_ohlcv OHLCV candles |
| `perps_ohlcv` | `perps_ohlcv` | `perp` | `ohlcv` | `1m` | `--symbols` | Perpetual futures OHLCV candles |

Interval State:

| CLI Domain | Bronze `dataset_type` | Instrument Type | Task Kind | Default Timeframe | Symbol Source | Description |
|---|---|---|---|---|---|---|
| `open_interest` | `open_interest` | `perp` | `open_interest` | `1m` | `--symbols` | Open-interest observations |
| `funding` | `funding` | `perp` | `funding` | `1m`* | `--symbols` | Funding-rate observations (stored at native cadence) |

Trade Ticks:

| CLI Domain | Bronze `dataset_type` | Instrument Type | Task Kind | Default Timeframe | Symbol Source | Description |
|---|---|---|---|---|---|---|
| `perps_trades` | `perps_trades` | `perp` | `trade` | `tick` | `--symbols` | Historical perpetual trade ticks |
| `options_trades` | `options_trades` | `option` | `trade` | `tick` | `--symbols` | Historical option trade ticks |

Volatility:

| CLI Domain | Bronze `dataset_type` | Instrument Type | Task Kind | Default Timeframe | Symbol Source | Description |
|---|---|---|---|---|---|---|
| `volatility_index_data` | `volatility_index_data` | `perp` | `volatility` | `1m` | `--symbols` | Historical Deribit volatility index OHLC observations |

\* Funding input accepts `1m`/`m1` aliases but normalizes to Deribit-native `8h` events.

### CLI Contract

- `bronze-build --dataset` choices: `spot_ohlcv perps_ohlcv open_interest funding perps_trades options_trades volatility_index_data`
- `--symbols` applies to all selected datasets (`spot_ohlcv`, `perps_ohlcv`, `open_interest`, `funding`, `perps_trades`, `options_trades`, `volatility_index_data`)

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
ARCHITECTURE.md
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
| `ARCHITECTURE.md` | Durable architecture contract for package boundaries, medallion flow, side effects, and update rules |
| `AGENTS.md` | Generated repository operating policy (do not edit directly) |

Dataset metadata is centralized in `application/datasets.py`. New Bronze datasets should start with a
`DatasetSpec` entry that defines the CLI name, storage dataset type, instrument type, symbol group,
task kind, and default timeframe. Bronze planning derives fetch tuples from these specs, so
new datasets can share symbol validation, deterministic scheduling, checkpoint fingerprints, and
reporting behavior instead of duplicating one-off planner logic.

Canonical terms:

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
Ruff, Mypy, Pyright, ty, import-linter, pytest, pytest-cov, pytest-xdist, and pre-commit.

Runtime configuration uses:

```text
config.yaml
```

Recommended permissions:

```bash
uv run python main.py --debug bronze-build \
 --exchange deribit \
 --market spot_ohlcv perps_ohlcv open_interest funding perps_trades options_trades volatility_index_data \
 --symbols BTC ETH SOL \
 --full-gap-fill \
 --save-parquet-lake \
 --no-json-output
```

Trade symbol inheritance:

# 4. Raw Datasets

Raw ingests are defined by `application/datasets.py` and persisted by Bronze writers in
`ingestion/lake.py`. The repository currently defines seven registry-backed raw dataset types:
`spot_ohlcv`, `perps_ohlcv`, `open_interest`, `funding`, `perps_trades`, `options_trades`, and `volatility_index_data`.

All datasets share structural metadata columns:
`schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`,
`ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`.

Coverage reference for missing statistics in this section:
- Start: first observed day per dataset series
- End: dataset-specific end date shown per row (inclusive)
- Missing %: missing calendar days / expected calendar days
- Missing Days: count of missing calendar days in the [Start Date, End Date] span

Current Bronze missing-day snapshot generated from `lake/bronze` on 2026-07-09 15:55 CEST:

| Dataset Type | Series | Start Date | End Date | Expected Days | Observed Days | Missing Days | Missing % |
|---|---:|---|---|---:|---:|---:|---:|
| `funding` | 3 | 2019-04-30 | 2026-07-04 | 6,818 | 6,818 | 0 | 0.00% |
| `historical_volatility` | 3 | 2026-05-08 | 2026-05-24 | 51 | 51 | 0 | 0.00% |
| `open_interest` | 3 | 2018-08-15 | 2026-07-04 | 7,122 | 7,122 | 0 | 0.00% |
| `options_trades` | 3 | 2018-08-14 | 2026-07-05 | 5,788 | 5,788 | 0 | 0.00% |
| `perps_ohlcv` | 3 | 2018-08-14 | 2026-07-05 | 7,083 | 7,083 | 0 | 0.00% |
| `perps_trades` | 3 | 2018-08-14 | 2026-06-11 | 5,739 | 4,038 | 1,701 | 29.64% |
| `spot_ohlcv` | 3 | 2023-04-24 | 2026-07-05 | 3,198 | 3,198 | 0 | 0.00% |
| `volatility_index_data` | 3 | 2022-11-07 | 2026-05-25 | 83 | 83 | 0 | 0.00% |

| Dataset Type | Exchange | Instrument | Symbol | Timeframe | Start Date | End Date | Expected Days | Observed Days | Missing Days | Missing % |
|---|---|---|---|---|---|---|---:|---:|---:|---:|
| `funding` | deribit | perp | `BTC-PERPETUAL` | 8h | 2019-04-30 | 2026-07-04 | 2,623 | 2,623 | 0 | 0.00% |
| `funding` | deribit | perp | `ETH-PERPETUAL` | 8h | 2019-04-30 | 2026-07-04 | 2,623 | 2,623 | 0 | 0.00% |
| `funding` | deribit | perp | `SOL-PERPETUAL` | 8h | 2022-03-16 | 2026-07-04 | 1,572 | 1,572 | 0 | 0.00% |
| `historical_volatility` | deribit | perp | `BTC` | 1m | 2026-05-08 | 2026-05-24 | 17 | 17 | 0 | 0.00% |
| `historical_volatility` | deribit | perp | `ETH` | 1m | 2026-05-08 | 2026-05-24 | 17 | 17 | 0 | 0.00% |
| `historical_volatility` | deribit | perp | `SOL` | 1m | 2026-05-08 | 2026-05-24 | 17 | 17 | 0 | 0.00% |
| `open_interest` | deribit | perp | `BTC-PERPETUAL` | 1m | 2018-08-15 | 2026-07-04 | 2,881 | 2,881 | 0 | 0.00% |
| `open_interest` | deribit | perp | `ETH-PERPETUAL` | 1m | 2019-03-15 | 2026-07-04 | 2,669 | 2,669 | 0 | 0.00% |
| `open_interest` | deribit | perp | `SOL-PERPETUAL` | 1m | 2022-03-16 | 2026-07-04 | 1,572 | 1,572 | 0 | 0.00% |
| `options_trades` | deribit | option | `BTC` | tick | 2018-08-14 | 2026-07-05 | 2,883 | 2,883 | 0 | 0.00% |
| `options_trades` | deribit | option | `ETH` | tick | 2019-03-21 | 2026-07-05 | 2,664 | 2,664 | 0 | 0.00% |
| `options_trades` | deribit | option | `SOL` | tick | 2022-05-04 | 2022-12-30 | 241 | 241 | 0 | 0.00% |
| `perps_ohlcv` | deribit | perp | `BTC-PERPETUAL` | 1m | 2018-08-14 | 2026-07-05 | 2,883 | 2,883 | 0 | 0.00% |
| `perps_ohlcv` | deribit | perp | `ETH-PERPETUAL` | 1m | 2019-03-14 | 2026-07-05 | 2,671 | 2,671 | 0 | 0.00% |
| `perps_ohlcv` | deribit | perp | `SOL-PERPETUAL` | 1m | 2022-04-29 | 2026-07-05 | 1,529 | 1,529 | 0 | 0.00% |
| `perps_trades` | deribit | perp | `BTC-PERPETUAL` | tick | 2018-08-14 | 2026-06-11 | 2,859 | 2,136 | 723 | 25.29% |
| `perps_trades` | deribit | perp | `ETH-PERPETUAL` | tick | 2019-03-14 | 2026-05-29 | 2,634 | 1,656 | 978 | 37.13% |
| `perps_trades` | deribit | perp | `SOL-PERPETUAL` | tick | 2022-04-29 | 2022-12-30 | 246 | 246 | 0 | 0.00% |
| `spot_ohlcv` | deribit | spot_ohlcv | `BTC_USDC` | 1m | 2023-04-24 | 2026-07-05 | 1,169 | 1,169 | 0 | 0.00% |
| `spot_ohlcv` | deribit | spot_ohlcv | `ETH_USDC` | 1m | 2023-04-24 | 2026-07-05 | 1,169 | 1,169 | 0 | 0.00% |
| `spot_ohlcv` | deribit | spot_ohlcv | `SOL_USDC` | 1m | 2024-02-27 | 2026-07-05 | 860 | 860 | 0 | 0.00% |
| `volatility_index_data` | deribit | perp | `BTC` | 1m | 2026-04-24 | 2026-05-25 | 32 | 32 | 0 | 0.00% |
| `volatility_index_data` | deribit | perp | `ETH` | 1m | 2026-04-24 | 2026-05-25 | 32 | 32 | 0 | 0.00% |
| `volatility_index_data` | deribit | perp | `SOL` | 1m | 2022-11-07 | 2022-11-25 | 19 | 19 | 0 | 0.00% |

## 4.1 Spot (`dataset_type=spot_ohlcv`)

### 1. Bronze layer

Market role: physical spot_ohlcv-market state and baseline for directional/volatility context.
Relationship: joins with `perp` by symbol/minute for basis and premium analysis.
Time aggregation: native `1m` OHLCV ingestion (no Bronze resampling).

### 1.1 Deribit endpoint

Endpoint: `GET https://www.deribit.com/api/v2/public/get_tradingview_chart_data`.
Description: returns TradingView-style OHLCV candle arrays for a symbol and resolution over a time range.

### 2. Silver layer

- Builder: `build_silver_for_symbol`.
- Missing values: rows with null `open_price`, `high_price`, `low_price`, or `close_price` are dropped; `volume`, `quote_volume`, and `trade_count` are preserved as provided by the source.
- Filter rows with null OHLC columns: `open_price`, `high_price`, `low_price`, `close_price`.
- Remove invalid candles where `high_price < max(open_price, close_price)` or `low_price > min(open_price, close_price)`.
- Deduplicate by `exchange/instrument_type/symbol/timeframe/open_time`, keep latest by `ingested_at`.
- Output canonical columns: `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `quote_volume`, `trade_count`, plus structural metadata.
- Time aggregation: `1m -> 1m` (no Silver resample).

### 3. High-value features

- Log return: `r_t = ln(close_price_t / close_price_{t-1})`.
- Range volatility (Parkinson): `sigma^2_{P,t} = (1 / (4 ln 2)) * (ln(high_price_t / low_price_t))^2`.
- Dollar participation: `turnover_t = quote_volume_t`; trade intensity: `intensity_t = trade_count_t`.
- Spot-perp basis anchor (self-financing leg input): `basis_t = perp_close_t - spot_ohlcv_close_t`.
- Market-neutral residual alpha seed: `epsilon_t = r^{spot_ohlcv}_t - beta_t * r^{mkt}_t`.

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
| `deribit` | `BTC_USDC` | `1m` | `2023-04-24` | `2026-07-05` | 0 | 0.00% |
| `deribit` | `ETH_USDC` | `1m` | `2023-04-24` | `2026-07-05` | 0 | 0.00% |
| `deribit` | `SOL_USDC` | `1m` | `2024-02-27` | `2026-07-05` | 0 | 0.00% |

## 4.2 Perpetual (`dataset_type=perps_ohlcv`)

### 1. Bronze layer

Market role: leveraged perpetual state for faster risk transfer and price discovery.
Relationship: consumed jointly with `spot_ohlcv`, `funding`, and `open_interest` for carry/crowding context.
Time aggregation: native `1m` OHLCV ingestion.

### 1.1 Deribit endpoint

Endpoint: `GET https://www.deribit.com/api/v2/public/get_tradingview_chart_data`.
Description: returns perpetual OHLCV candle arrays (open/high/low/close/volume) for instrument-level bar construction.

### 2. Silver layer

- Builder: `build_silver_for_symbol` (same contract as spot_ohlcv).
- Missing values: rows with null `open_price`, `high_price`, `low_price`, or `close_price` are dropped; `volume`, `quote_volume`, and `trade_count` are preserved as provided by the source.
- Filter rows with null OHLC columns: `open_price`, `high_price`, `low_price`, `close_price`.
- Enforce candle consistency: `high_price >= max(open_price, close_price)` and `low_price <= min(open_price, close_price)`.
- Deduplicate by `exchange/instrument_type/symbol/timeframe/open_time`, keep latest by `ingested_at`.
- Output canonical columns: `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `quote_volume`, `trade_count`, plus structural metadata.
- Time aggregation: `1m -> 1m`.

### 3. High-value features

- Basis level vs spot_ohlcv: `basis_t = perp_close_t - spot_ohlcv_close_t`.
- Basis momentum: `delta_basis_t = basis_t - basis_{t-1}`.
- Intrabar realized range proxy: `rv_t = ln(high_price_t / low_price_t)^2`.
- Notional pressure: `pressure_t = quote_volume_t / rolling_mean(quote_volume, n)`.
- Self-financing carry sleeve signal: `s_t = zscore(basis_t) - lambda * funding_rate_t`.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `open_price` | USD (or quote/base) | Opening perpetual mark for interval. | Used against spot_ohlcv prices to infer carry and dislocation. |
| `high_price` | USD (or quote/base) | Intrabar maximum price. | Coupled with Open Interest/funding shifts to detect squeeze conditions. |
| `low_price` | USD (or quote/base) | Intrabar minimum price. | Combined with Open Interest drawdowns for liquidation diagnostics. |
| `close_price` | USD (or quote/base) | End-of-interval perpetual mark. | Canonical join key with funding/Open Interest minute features. |
| `volume` | contracts/base units | Leveraged venue traded size. | Compared with spot_ohlcv volume and tick-flow aggregates for speculation intensity. |
| `quote_volume` | quote-currency units | Perpetual notional turnover. | Used for cross-market notional participation diagnostics. |
| `trade_count` | count | Number of perp executions. | Coarse complement to `perps_trades` microstructure rows. |
| `origin_payload` | JSON/object | Full source-shaped raw record for audit/replay. | Reconciliation source for derived Silver features. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC-PERPETUAL` | `1m` | `2018-08-14` | `2026-07-05` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `1m` | `2019-03-14` | `2026-07-05` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `1m` | `2022-04-29` | `2026-07-05` | 0 | 0.00% |

## 4.3 Open Interest (`dataset_type=open_interest`)

### 1. Bronze layer

Market role: outstanding leveraged exposure stock for each perp symbol.
Relationship: used with `perp` returns and `funding` to label build-up, unwind, and squeeze regimes.
Time aggregation: native `1m` Open Interest snapshots.

### 1.1 Deribit endpoint

Endpoint: `GET https://www.deribit.com/api/v2/public/get_last_settlements_by_instrument`.
Description: returns settlement/event records per instrument; this loader extracts open-interest observations and normalizes them to the Open Interest stream.

### 2. Silver layer

- Builder 1: `build_open_interest_observed_for_symbol`.
- Normalize/cast columns: `timestamp`, `exchange`, `symbol`, `open_interest`.
- Missing values: rows with null/non-finite `open_interest` are excluded from `open_interest_observed`; `open_interest_1m_feature` uses backward as-of fill and exposes freshness/nullability state via `open_interest_is_observed`, `open_interest_is_ffill`, and `minutes_since_open_interest_observation`.
- Validate `open_interest` is finite and non-negative.
- Deduplicate observed rows by `exchange/symbol/timestamp/open_interest` into `open_interest_observed`.
- Builder 2: `build_open_interest_1m_feature_for_symbol`.
- Generate full `1m` calendar and backward `asof`-join observed rows.
- Output columns: `open_interest`, `open_interest_is_observed`, `open_interest_is_ffill`, `minutes_since_open_interest_observation`, `open_interest_observation_lag_sec`, `open_interest_source_timestamp`.
- Time aggregation: observed `1m ->` feature `1m`.

### 3. High-value features

- Open Interest change: `delta_open_interest_t = open_interest_t - open_interest_{t-1}`.
- Open Interest return proxy: `g^{open_interest}_t = ln(open_interest_t / open_interest_{t-1})`.
- Crowding regime score: `crowd_t = zscore(delta_open_interest_t) * sign(perp_return_t)`.
- Freshness penalty: `w_t = exp(-k * minutes_since_open_interest_observation_t)`.
- FFill guard flag: `stale_t = 1[open_interest_is_ffill = 1 and minutes_since_open_interest_observation_t > tau]`.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `open_interest` | contracts | Total open positions at timestamp. | Combined with price direction from `perp` to classify position flow regime. |
| `open_interest_value` | quote-currency notional | Monetary exposure form of Open Interest. | Scales raw Open Interest for cross-period comparability and risk sizing. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC-PERPETUAL` | `1m` | `2018-08-15` | `2026-07-04` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `1m` | `2019-03-15` | `2026-07-04` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `1m` | `2022-03-16` | `2026-07-04` | 0 | 0.00% |

## 4.4 Funding (`dataset_type=funding`)

### 1. Bronze layer

Market role: periodic long-short transfer (carry) state for perps.
Relationship: joined with perp and Open Interest state to identify crowding and carry pressure.
Time aggregation: native `8h` funding observations.

### 1.1 Deribit endpoint

Endpoint: `GET https://www.deribit.com/api/v2/public/get_funding_rate_history`.
Description: returns historical funding events (`interest_8h` and related mark/index fields) for perpetual instruments.

### 2. Silver layer

- Builder 1: `build_funding_observed_for_symbol`.
- Missing values: rows with null/non-finite `funding_rate` are excluded from `funding_observed`; `funding_1m_feature` carries the last known value and explicit availability flags (`funding_data_available`, `minutes_since_funding`).
- Validate `funding_rate`: non-null, finite, and `abs(funding_rate) <= 1.0`.
- Group by `exchange/symbol/funding_time`.
- Output observed columns: `funding_rate`, `base_asset`, `funding_interval_hours`, ingestion bounds, source row counts.
- Builder 2: `build_funding_1m_feature_for_symbol`.
- Backward-join observed funding into a full `1m` calendar with anti-leakage constraints.
- Output feature columns: `funding_rate_last_known`, `funding_observed_at`, `minutes_since_funding`, `is_funding_observation_minute`, `funding_data_available`.
- Time aggregation: `8h` events -> `1m` feature grid.

### 3. High-value features

- Annualized carry proxy (8h funding): `carry_ann_t ~= funding_rate_last_known_t * 3 * 365`.
- Funding shock: `shock_t = funding_rate_t - rolling_mean(funding_rate, n)`.
- Recency weight: `w_t = exp(-k * minutes_since_funding_t)`.
- Net carry after basis decay estimate: `net_carry_t = carry_ann_t - expected_basis_reversion_t`.
- Market-neutral financing filter: trade only if `|funding_rate_last_known_t| < q_alpha` or
  `sign(basis_t) = -sign(funding_rate_last_known_t)`.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `funding_rate` | fraction per `8h` event | Funding transfer rate between longs and shorts. | Combined with Open Interest/perp moves for crowding and squeeze diagnostics. |
| `index_price` | USD | External fair-value index around funding event. | Baseline for mark/index dislocation and premium state. |
| `mark_price` | USD | Exchange mark reference around funding timestamp. | Compared with index/perp close for premium and stress features. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC-PERPETUAL` | `8h` | `2019-04-30` | `2026-07-04` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `8h` | `2019-04-30` | `2026-07-04` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `8h` | `2022-03-16` | `2026-07-04` | 0 | 0.00% |

## 4.5 `perps_trades` (`dataset_type=perps_trades`)

### 1. Bronze layer

Market role: tick-level perpetual execution flow and aggressor direction.
Relationship: microstructure input for `perps_trades_1m_feature` and downstream Gold joins.
Time aggregation: native `tick` (one row per trade).

### 1.1 Deribit endpoint

Endpoint: primary `GET https://history.deribit.com/api/v2/public/get_last_trades_by_instrument_and_time`; fallback `GET https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time` (base URL may fall back to `https://www.deribit.com`).
Description: paginated tick-trade retrieval for perpetuals; returns trade-by-trade executions with timestamp, price, size, and side metadata.
Reliability behavior: requests use 500-row pages by default, stay capped by Deribit's 1000-row page
limit, split bounded Bronze fetches into 15-minute trade windows, and retry/fall back across
configured endpoints for transient route, timeout, and connection-reset failures.
Runtime override: set `DEPTH_DERIBIT_PERP_TRADES_PAGE_SIZE` to tune request page size within the
`1..1000` bound.

### 2. Silver layer

- Builder 1: `build_perps_trades_observed_for_symbol`.
- Missing values: rows with null/non-finite `price` or `quantity` are dropped; missing/unknown `side` is normalized to `unknown` where required for deterministic aggregations.
- Normalize typed trade columns: `trade_time`, `trade_id`, `price`, `quantity`, `side`.
- Filter invalid rows: `price <= 0`, `quantity <= 0`, null/non-finite values.
- Deduplicate observed rows by `exchange/instrument_type/symbol/trade_time/trade_id`.
- Builder 2: `build_perps_trades_1m_feature_for_symbol`.
- Aggregate `tick` rows to `1m` OHLC columns: `open_price`, `high_price`, `low_price`, `close_price`.
- Aggregate flow columns: `volume`, `quote_volume`, `trade_count`, `buy_volume`, `sell_volume`, `buy_trade_count`, `sell_trade_count`, `buy_volume_share`.
- Time aggregation: `tick -> 1m`.

### 3. High-value features

- Signed volume imbalance: `imb_t = (buy_volume_t - sell_volume_t) / (buy_volume_t + sell_volume_t)`.
- Trade-count imbalance: `imb_count_t = (buy_trade_count_t - sell_trade_count_t) / trade_count_t`.
- Kyle-style impact proxy: `impact_t = |return_t| / max(quote_volume_t, eps)`.
- Flow momentum: `flow_mom_t = rolling_sum(imb_t, n)`.
- Self-financing intraday spread leg: `pnl_t = w_t * (r_long_t - r_short_t) - costs_t`,
  with `w_t` increasing in `|imb_t|`.

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
| `deribit` | `BTC-PERPETUAL` | `tick` | `2018-08-14` | `2026-06-11` | 723 | 25.29% |
| `deribit` | `ETH-PERPETUAL` | `tick` | `2019-03-14` | `2026-05-29` | 978 | 37.13% |
| `deribit` | `SOL-PERPETUAL` | `tick` | `2022-04-29` | `2022-12-30` | 0 | 0.00% |

## 4.6 `options_trades` (`dataset_type=options_trades`)

### 1. Bronze layer

Market role: tick-level option execution flow, with trade-level contract metadata available at ingest (`instrument_name`, `expiry`, `strike`, `option_type`).
Relationship: upstream input for option-flow minute features and cross-asset joins.
Time aggregation: native `tick`.

### 1.1 Deribit endpoint

Endpoint: `GET https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time` (base URL may fall back to `https://www.deribit.com`).
Description: paginated option tick-trade retrieval by currency; includes contract identifier fields used to derive `expiry`, `strike`, and `option_type`.
Reliability behavior: requests use 500-row pages by default, stay capped by Deribit's 1000-row page
limit, split bounded Bronze fetches into 60-minute trade windows, and retry/fall back across
configured base URLs for transient route, timeout, and connection-reset failures.
Runtime override: set `DEPTH_DERIBIT_OPTIONS_TRADES_PAGE_SIZE` to tune request page size within the
`1..1000` bound.

### 2. Silver layer

- Builders: same trade builders with `bronze_dataset_type=options_trades`.
- Missing values: rows with null/non-finite `price` or `quantity` are dropped; missing/unknown `side` is normalized to `unknown`; contract metadata nulls (`expiry`, `strike`, `option_type`) may exist in Bronze but are currently not retained in Silver outputs.
- Output datasets: `options_trades_observed` and `options_trades_1m_feature`.
- Observed schema (post-validation/dedup): `trade_time`, `trade_id`, `price`, `quantity`, `side`, `exchange`, `symbol`, `instrument_type`.
- Feature aggregation (`tick -> 1m`) OHLC columns: `open_price`, `high_price`, `low_price`, `close_price`.
- Feature flow columns: `volume`, `quote_volume`, `trade_count`, `buy_volume`, `sell_volume`, `buy_trade_count`, `sell_trade_count`, `buy_volume_share`.
- Current Silver limitation: contract metadata columns from Bronze (`expiry`, `strike`, `option_type`) are not retained.
- Time aggregation: `tick -> 1m`.

### 3. High-value features

- Option signed flow: `opt_imb_t = (buy_volume_t - sell_volume_t) / (buy_volume_t + sell_volume_t)`.
- Call-put pressure (when split is available): `cp_t = (call_buy_notional_t - put_buy_notional_t) / total_notional_t`.
- Activity shock: `shock_t = (trade_count_t - rolling_mean(trade_count, n)) / rolling_std(trade_count, n)`.
- Moneyness-weighted pressure (if `strike` joined): `mw_t = sum_i(weight_i * signed_notional_i)` with
  `weight_i = exp(-|ln(strike_i / spot_ohlcv_t)|)`.
- Volatility timing trigger: `enter_vol_t = 1[opt_imb_t > q_{0.9} and funding_shock_t > 0]`.

| Column | Unit | Market meaning | Relationship to other datasets/columns |
|---|---|---|---|
| `trade_id` | identifier | Unique option trade id. | Deduplication/replay identity. |
| `price` | option premium (quote units) | Executed option premium. | Aggregated into option-flow pressure features. |
| `quantity` | contracts | Number of option contracts traded. | Volume and participation proxy for options activity. |
| `side` | category (`buy`/`sell`/`unknown`) | Aggressor side proxy. | Supports directional option-flow imbalance features. |
| `is_maker` | boolean | Maker-side indicator proxy. | Liquidity-taking vs provision context. |
| `instrument_name` | contract code | Full exchange contract identifier. | Parent for `expiry`, `strike`, `option_type` extraction. |
| `expiry` | contract expiry code | Option maturity bucket. | Used with timestamp for term-structure activity mapping. |
| `strike` | strike price (USD) | Contract strike level. | Combined with underlying spot_ohlcv/perp for moneyness context. |
| `option_type` | category (`call`/`put`/`unknown`) | Contract payoff side. | Enables call/put activity skew features. |

Coverage:

| Exchange | Symbol | Timeframe | Start Date | End Date | Missing Days | Missing % |
|---|---|---|---|---|---:|---:|
| `deribit` | `BTC` | `tick` | `2018-08-14` | `2026-07-05` | 0 | 0.00% |
| `deribit` | `ETH` | `tick` | `2019-03-21` | `2026-07-05` | 0 | 0.00% |
| `deribit` | `SOL` | `tick` | `2022-05-04` | `2022-12-30` | 0 | 0.00% |

---

# 5. Example Commands

## 5.1 End-to-End Pipeline

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

Runs `bronze-build -> silver-build -> gold-build` using `medallion-pipeline` settings from
`config.yaml`, enforces single-run locking via `.run/full-pipeline.lock`, and writes a shared
append-only pipeline log. The configured Bronze and Silver steps include
`volatility_index_data`, so Deribit volatility-index OHLC fields flow into Gold as
`volatility_index_value`, `volatility_index_open`, `volatility_index_high`,
`volatility_index_low`, and `volatility_index_close`.

## 5.2 Layer Commands

Bronze:

```bash
uv run python main.py bronze-build \
  --exchange deribit \
  --dataset spot_ohlcv perps_ohlcv open_interest funding perps_trades options_trades \
  --symbols BTC ETH SOL
```

Silver:

```bash
uv run python main.py silver-build \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --exchange deribit \
  --dataset spot_ohlcv perps_ohlcv open_interest funding perps_trades options_trades \
    options_instrument_ticker_snapshot_1m options_surface_1m_feature \
  --timeframe 1m \
  --maxprocesses 4
```

Gold:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --maxprocesses 4 \
  --dataset-id gold.market.full.m1
```

## 5.3 Operational Notes

Symbol-group controls for Bronze:

- `--symbols` applies to all selected datasets (`spot_ohlcv`, `perps_ohlcv`, `open_interest`, `funding`, `perps_trades`, `options_trades`)
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

`perps_trades` storage path: `dataset_type=perps_trades`.

Gold source selection:

- for each required upstream dataset, equivalent symbol variants are normalized and the newest
  parquet artifact is selected
- `gold.hybrid.full_l2.m1` applies the same newest-artifact policy for L2 input

Gold retention policy:

- keep latest `N` versions per `dataset_id/exchange/symbol` lineage (default `N=3`)
- configure via `gold-build.retention_keep_versions` in `config.yaml` or override with
  `--retention-keep-versions`.

Available Gold dataset IDs:

- `gold.market.perps_trades.m1` (`perps_trades` flow only)
- `gold.market.options_trades.m1` (`options_trades` flow only)
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.hybrid.full_l2.m1`

## 5.4 Quality Checks

Run this sequence before pushing changes:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pyright --level error
uv run ty check
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.yaml
uv run --extra dev pytest
```

| Check | Scope | Gate Objective | Failure Signal |
|---|---|---|---|
| `uv run ruff check .` | Lint and static quality rules | Keep code quality and prevent obvious correctness pitfalls before runtime. | Style/correctness violations such as unused imports, invalid patterns, or rule breaches. |
| `uv run mypy .` | Static typing | Enforce typed contracts across DTOs, services, and module boundaries. | Type mismatches, invalid `None` handling, incompatible signatures. |
| `uv run pyright --level error` | Static typing (strict) | Provide complementary type analysis and stricter narrowing checks. | Type errors not caught by mypy or stricter incompatibility findings. |
| `uv run ty check` | Additional typing gate | Maintain policy-level typing consistency across the codebase. | Unresolved typing gaps and annotation inconsistencies. |
| `uv run lint-imports --config .importlinter` | Architecture boundaries | Enforce dependency direction and import-layer contracts. | Boundary violations (for example domain importing infrastructure internals). |
| `uv run python scripts/validate_config_with_pydantic.py --config config.yaml` | Runtime config schema | Reject invalid runtime configuration before pipeline execution. | Missing/invalid config fields or schema/type constraint failures. |
| `uv run --extra dev pytest` | Behavioral + regression tests | Validate functional behavior in parallel and enforce coverage thresholds. | Test failures, behavioral regressions, or coverage below configured threshold. |

Operational notes:

- `pytest` coverage and parallel execution defaults are configured in `pyproject.toml`; xdist uses logical CPU workers capped at 4 with load-scope distribution.
- Pre-commit enforces the same logical quality-gate path used in CI.

---

# 6. Roadmap

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
