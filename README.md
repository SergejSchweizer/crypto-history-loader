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
  - [4.7 Layer Inventory Snapshot](#47-layer-inventory-snapshot)
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
| `config.yaml` | Canonical runtime configuration |
| `pyproject.toml` | Project metadata and Python tooling configuration |
| `main.py` | Python entrypoint wrapper for CLI execution |
| `ARCHITECTURE.md` | Durable architecture contract for package boundaries, medallion flow, side effects, and update rules |
| `AGENTS.md` | Standalone repository operating policy |

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
`ingestion/lake.py`. The historical CLI registry defines seven raw dataset types:
`spot_ohlcv`, `perps_ohlcv`, `open_interest`, `funding`, `perps_trades`, `options_trades`, and
`volatility_index_data`. The physical Bronze inventory also contains eleven live-origin snapshot
datasets, all listed in section 4.7.1.

All datasets share structural metadata columns:
`schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`,
`ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`.

Coverage reference for missing statistics in this section:
- Start: first observed day per dataset series
- End: dataset-specific end date shown per row (inclusive)
- Missing %: missing calendar days / expected calendar days
- Missing Days: count of missing calendar days in the [Start Date, End Date] span

The complete Bronze missing-day snapshot generated from `lake/bronze` on 2026-07-12 CEST is in
[the authoritative layer inventory](#47-layer-inventory-snapshot). It reports dates and missing days
per primary series, which avoids treating BTC, ETH, and SOL series with different lifetimes as one series.

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
| `deribit` | `BTC_USDC` | `1m` | `2023-04-24` | `2026-07-12` | 0 | 0.00% |
| `deribit` | `ETH_USDC` | `1m` | `2023-04-24` | `2026-07-12` | 0 | 0.00% |
| `deribit` | `SOL_USDC` | `1m` | `2024-02-27` | `2026-07-12` | 0 | 0.00% |

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
| `deribit` | `BTC-PERPETUAL` | `1m` | `2018-08-14` | `2026-07-12` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `1m` | `2019-03-14` | `2026-07-12` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `1m` | `2022-04-29` | `2026-07-12` | 0 | 0.00% |

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
| `deribit` | `BTC-PERPETUAL` | `1m` | `2018-08-15` | `2026-07-11` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `1m` | `2019-03-15` | `2026-07-11` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `1m` | `2022-03-16` | `2026-07-11` | 0 | 0.00% |

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
| `deribit` | `BTC-PERPETUAL` | `8h` | `2019-04-30` | `2026-07-11` | 0 | 0.00% |
| `deribit` | `ETH-PERPETUAL` | `8h` | `2019-04-30` | `2026-07-11` | 0 | 0.00% |
| `deribit` | `SOL-PERPETUAL` | `8h` | `2022-03-16` | `2026-07-11` | 0 | 0.00% |

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
| `deribit` | `BTC-PERPETUAL` | `tick` | `2018-08-14` | `2026-06-11` | 498 | 17.42% |
| `deribit` | `ETH-PERPETUAL` | `tick` | `2019-03-14` | `2026-05-29` | 748 | 28.40% |
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
| `deribit` | `BTC` | `tick` | `2018-08-14` | `2026-07-12` | 0 | 0.00% |
| `deribit` | `ETH` | `tick` | `2019-03-21` | `2026-07-12` | 0 | 0.00% |
| `deribit` | `SOL` | `tick` | `2022-05-04` | `2022-12-30` | 0 | 0.00% |

---

## 4.7 Layer Inventory Snapshot

This is the authoritative local-data snapshot as of 2026-07-12 CEST. Dates are derived from the
partitioned Parquet files. Missing days are counted per primary series between that series' first and
last observed day; different series with different lifetimes are not treated as one continuous series.
The tables describe physical files, not only contracts declared in code.

### 4.7.1 Bronze: all physical datasets

Historical datasets are produced by this repository. Snapshot datasets are produced by the live-loader
origin and copied or mounted into the shared lake. The variable lists below are the observed Parquet
schemas, including lineage and payload fields.

| Dataset type | Series | Period | Missing days | Physical variables |
|---|---|---|---|---|
| `funding` | BTC/ETH/SOL perpetuals | BTC/ETH 2019-04-30..2026-07-11; SOL 2022-03-16..2026-07-11 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `funding_rate`, `index_price`, `mark_price` |
| `perps_ohlcv` | BTC/ETH/SOL perpetuals | BTC 2018-08-14..2026-07-12; ETH 2019-03-14..2026-07-12; SOL 2022-04-29..2026-07-12 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `quote_volume`, `trade_count`, `origin_payload` |
| `open_interest` | BTC/ETH/SOL perpetuals | BTC 2018-08-15..2026-07-11; ETH 2019-03-15..2026-07-11; SOL 2022-03-16..2026-07-11 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `open_interest`, `open_interest_value` |
| `perps_trades` | BTC/ETH/SOL perpetuals | BTC 2018-08-14..2026-06-11; ETH 2019-03-14..2026-05-29; SOL 2022-04-29..2022-12-30 | BTC 498; ETH 748; SOL 0 | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `trade_id`, `price`, `quantity`, `side`, `is_maker` |
| `options_trades` | BTC/ETH/SOL options | BTC 2018-08-14..2026-07-12; ETH 2019-03-21..2026-07-12; SOL 2022-05-04..2022-12-30 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `trade_id`, `price`, `quantity`, `side`, `is_maker`, `instrument_name`, `expiry`, `strike`, `option_type` |
| `volatility_index_data` | BTC/ETH/SOL index series | BTC/ETH 2026-04-24..2026-05-25; SOL 2022-11-07..2022-11-25 | 0 per series; 1,245 only in the combined cross-series envelope | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `value` |
| `historical_volatility` | BTC/ETH/SOL | 2026-05-08..2026-05-24 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `value` |
| `volatility_index_snapshot_1m` | BTC/ETH live snapshots | 2026-06-12..2026-07-11 | 0 per series; SOL not present | `schema_version`, `dataset_type`, `exchange`, `source`, `currency`, `source_currency`, `timestamp`, `open`, `high`, `low`, `close`, `resolution`, `snapshot_time`, `ingested_at`, `run_id`, `raw_payload_hash` |
| `index_price_snapshot_1m` | `btc_usd`, `eth_usd`, `sol_usdc` | 2026-05-24..2026-07-11 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `source`, `index_name`, `snapshot_time`, `event_time`, `price`, `ingested_at`, `run_id`, `raw_payload_hash` |
| `futures_summary_snapshot_1m` | BTC/ETH/SOL currency groups | 2026-06-12..2026-07-11 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `source`, `currency`, `requested_currency`, `source_currency`, `instrument_name`, `instrument_type`, `snapshot_time`, `exchange_creation_time`, `ingested_at`, `run_id`, `bid_price`, `ask_price`, `mid_price`, `mark_price`, `last`, `open_interest`, `volume`, `volume_usd`, `high`, `low`, `price_change`, `underlying_price`, `estimated_delivery_price`, `interest_rate`, `raw_payload_hash` |
| `options_ticker_snapshot_1m` | BTC/ETH/SOL currency groups | 2026-05-24..2026-07-11 | 0 per series | `exchange`, `dataset_type`, `source`, `currency`, `requested_currency`, `source_currency`, `instrument_name`, `base_currency`, `quote_currency`, `instrument_type`, `snapshot_time`, `exchange_creation_time`, `ingested_at`, `run_id`, `bid_price`, `ask_price`, `mid_price`, `mark_price`, `mark_iv`, `underlying_price`, `underlying_index`, `interest_rate`, `open_interest`, `volume`, `volume_usd`, `high`, `low`, `last`, `price_change`, `raw_payload_hash`, `schema_version` |
| `options_instrument_ticker_snapshot_1m` | BTC/ETH/SOL currency groups | 2026-06-12..2026-07-11 | 0 per series | `exchange`, `dataset_type`, `source`, `currency`, `instrument_name`, `instrument_type`, `snapshot_time`, `exchange_creation_time`, `exchange_timestamp`, `ingested_at`, `run_id`, `state`, `bid_price`, `ask_price`, `best_bid_price`, `best_ask_price`, `best_bid_amount`, `best_ask_amount`, `bid_iv`, `ask_iv`, `mark_iv`, `mark_price`, `last_price`, `underlying_price`, `underlying_index`, `index_price`, `interest_rate`, `open_interest`, `volume`, `volume_usd`, `high`, `low`, `price_change`, `delta`, `gamma`, `theta`, `vega`, `rho`, `raw_payload_hash`, `schema_version` |
| `perps_l2_snapshot_1m` | BTC/ETH/SOL perpetuals | 2026-05-05..2026-07-11 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source`, `depth`, `fetch_duration_s`, `bids`, `asks`, `mark_price`, `index_price`, `open_interest`, `funding_8h`, `current_funding` |
| `options_l2_snapshot_1m` | BTC/ETH/SOL options | 2026-07-03..2026-07-11 | 0 per currency group | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source`, `depth`, `fetch_duration_s`, `bids`, `asks`, `currency`, `instrument_name`, `snapshot_time`, `exchange_timestamp`, `state`, `bid_levels`, `ask_levels`, `best_bid_price`, `best_ask_price`, `best_bid_amount`, `best_ask_amount`, `mark_price`, `index_price`, `underlying_price`, `underlying_index`, `interest_rate`, `bid_iv`, `ask_iv`, `mark_iv`, `open_interest`, `last_price`, `settlement_price`, `min_price`, `max_price`, `volume`, `volume_usd`, `high`, `low`, `price_change`, `delta`, `gamma`, `theta`, `vega`, `rho`, `raw_payload_hash` |
| `recent_trade_snapshot_1m` | BTC/ETH/SOL currency groups | 2026-06-12..2026-07-11 | 0 per series | `schema_version`, `dataset_type`, `exchange`, `source`, `requested_currency`, `source_currency`, `currency`, `instrument_name`, `instrument_type`, `kind`, `trade_id`, `trade_sequence`, `exchange_timestamp`, `snapshot_time`, `ingested_at`, `run_id`, `price`, `amount`, `direction`, `tick_direction`, `mark_price`, `index_price`, `iv`, `liquidation`, `block_trade_id`, `signed_amount`, `notional`, `raw_payload_hash` |
| `instrument_metadata_snapshot_daily` | aggregate | 2026-05-25..2026-07-11 | 0 | `schema_version`, `dataset_type`, `exchange`, `source`, `snapshot_date`, `ingested_at`, `run_id`, `instrument_name`, `kind`, `base_currency`, `quote_currency`, `counter_currency`, `settlement_currency`, `instrument_type`, `tick_size`, `contract_size`, `min_trade_amount`, `is_active`, `creation_timestamp`, `expiration_timestamp`, `option_type`, `strike`, `raw_payload_hash` |
| `futures_instrument_metadata_snapshot_daily` | aggregate | 2026-06-13..2026-07-11 | 0 | `schema_version`, `dataset_type`, `exchange`, `source`, `snapshot_date`, `ingested_at`, `run_id`, `instrument_name`, `kind`, `base_currency`, `quote_currency`, `counter_currency`, `settlement_currency`, `instrument_type`, `settlement_period`, `price_index`, `state`, `tick_size`, `contract_size`, `min_trade_amount`, `is_active`, `creation_timestamp`, `expiration_timestamp`, `option_type`, `strike`, `raw_payload_hash` |

### 4.7.2 Silver: physical status and exact contract variables

The following ten Silver datasets are physically present. The remaining contracted Silver outputs are
listed as missing and must be materialized by the backlog stack. Exact contract definitions are also
maintained in [`application/dataset_contracts.py`](application/dataset_contracts.py).

| Silver dataset | Physical period / missing days | Contract variables |
|---|---|---|
| `spot_ohlcv` | BTC/ETH 2023-04-24..2026-06-25, SOL 2024-02-27..2026-06-25; 0 per series | `schema_version`, `dataset_type`, `exchange`, `symbol`, `instrument_type`, `event_time`, `ingested_at`, `run_id`, `source_endpoint`, `open_time`, `close_time`, `timeframe`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `quote_volume`, `trade_count`, `origin_payload` |
| `perp` (legacy `perps_ohlcv`) | BTC 2018-08-14..2026-06-25, ETH 2019-03-14..2026-06-25, SOL 2022-04-29..2026-06-25; 0 | Same OHLCV variables as `spot_ohlcv` |
| `funding_observed` | BTC/ETH 2019-04-30..2026-06-24, SOL 2022-03-16..2026-06-24; 0 | `funding_time`, `exchange`, `symbol`, `base_asset`, `instrument_type`, `funding_rate`, `funding_interval_hours`, `ingested_at_min`, `ingested_at_max`, `source_row_count`, `silver_built_at`, `data_quality_status` |
| `funding_1m_feature` | BTC/ETH 2019-04-01..2026-06-24, SOL 2022-03-01..2026-06-24; 0 | `timestamp`, `exchange`, `symbol`, `funding_rate_last_known`, `funding_observed_at`, `minutes_since_funding`, `is_funding_observation_minute`, `funding_data_available` |
| `open_interest_observed` | BTC 2018-08-15..2026-06-24, ETH 2019-03-15..2026-06-24, SOL 2022-03-16..2026-06-24; 0 | `timestamp`, `exchange`, `symbol`, `open_interest`, `open_interest_source_timestamp`, `ingested_at`, `source_endpoint` |
| `open_interest_1m_feature` | BTC 2018-08-01..2026-06-24, ETH 2019-03-01..2026-06-24, SOL 2022-03-01..2026-06-24; 0 | `timestamp_m1`, `exchange`, `symbol`, `open_interest`, `open_interest_is_observed`, `open_interest_is_ffill`, `minutes_since_open_interest_observation`, `open_interest_observation_lag_sec`, `open_interest_source_timestamp` |
| `perps_trades_observed` | BTC 2018-08-14..2026-06-11: 1,717 missing; ETH 2019-03-14..2026-05-29: 1,939 missing; SOL 0 | `trade_time`, `exchange`, `symbol`, `instrument_type`, `trade_id`, `price`, `quantity`, `side` |
| `perps_trades_1m_feature` | Same source spans; BTC 1,717 missing, ETH 1,939 missing, SOL 0 | `timestamp_m1`, `exchange`, `symbol`, `instrument_type`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `quote_volume`, `trade_count`, `buy_volume`, `sell_volume`, `buy_trade_count`, `sell_trade_count`, `buy_volume_share` |
| `options_trades_observed` | BTC 2018-08-14..2026-06-25, ETH 2019-03-21..2026-06-11: 184 missing, SOL 2022-05-04..2022-12-30; 0/184/0 | Same observed trade variables as `perps_trades_observed` |
| `options_trades_1m_feature` | Same source spans; BTC 0, ETH 184, SOL 0 | Same 1m trade-feature variables as `perps_trades_1m_feature` |

Missing contracted Silver datasets: `volatility_index_data_observed`,
`volatility_index_1m_observed`, `volatility_index_snapshot_1m_observed`, `volatility_index_1m_feature`,
`realized_volatility_1m_feature`, `iv_rv_1m_feature`, `index_price_snapshot_1m_observed`,
`index_price_1m_feature`, `futures_summary_snapshot_1m_observed`, `futures_summary_1m_feature`,
`options_ticker_snapshot_1m_observed`, `options_instrument_ticker_snapshot_1m_observed`,
`options_surface_1m_feature`, `perps_l2_snapshot_1m_observed`, `perps_l2_1m_feature`,
`options_l2_snapshot_1m_observed`, `options_l2_1m_feature`, `recent_trade_snapshot_1m_observed`,
`instrument_metadata_snapshot_daily_observed`, `futures_instrument_metadata_snapshot_daily_observed`, and
`historical_volatility_observed`. Their exact variables are the corresponding contract `output_columns`.
They currently have no physical Silver period and therefore no observed/missing-day count.

Exact variables for the missing Silver contracts:

| Dataset | Variables |
|---|---|
| `volatility_index_data_observed`, `volatility_index_1m_observed`, `volatility_index_snapshot_1m_observed` | `timestamp`, `exchange`, `symbol`, `instrument_type`, `dataset_type`, `volatility_value`, `volatility_open`, `volatility_high`, `volatility_low`, `volatility_close`, `volatility_source_timestamp`, `ingested_at`, `source_endpoint` |
| `volatility_index_1m_feature` | `timestamp_m1`, `exchange`, `symbol`, `iv_open`, `iv_high`, `iv_low`, `iv_close`, `iv_range`, `iv_return_1m`, `iv_change_5m`, `iv_change_15m`, `iv_change_1h`, `iv_zscore_1d`, `iv_zscore_7d`, `iv_percentile_30d`, `iv_source_dataset`, `iv_source_timestamp`, `minutes_since_iv_observation`, `iv_data_available` |
| `realized_volatility_1m_feature` | `timestamp_m1`, `exchange`, `symbol`, `rv_5m`, `rv_15m`, `rv_1h`, `rv_4h`, `rv_1d`, `parkinson_rv_1h`, `jump_proxy`, `spot_available`, `perps_available` |
| `iv_rv_1m_feature` | `timestamp_m1`, `exchange`, `symbol`, `iv_minus_rv_1h`, `iv_minus_rv_1d`, `iv_rv_ratio_1h`, `iv_rv_ratio_1d`, `iv_rv_zscore_1d`, `iv_rv_percentile_30d`, `minutes_since_iv_observation`, `minutes_since_rv_observation`, `iv_available`, `rv_available` |
| `index_price_snapshot_1m_observed` | `timestamp`, `exchange`, `symbol`, `index_name`, `index_price`, `index_price_source_timestamp`, `ingested_at`, `source_endpoint` |
| `index_price_1m_feature` | `timestamp_m1`, `exchange`, `symbol`, `index_price`, `index_price_is_observed`, `index_price_source_timestamp`, `minutes_since_index_price_observation` |
| `futures_summary_snapshot_1m_observed` | `timestamp`, `exchange`, `symbol`, `instrument_type`, `mark_price`, `index_price`, `open_interest`, `volume`, `turnover`, `funding_rate`, `ingested_at`, `source_endpoint` |
| `futures_summary_1m_feature` | `timestamp_m1`, `exchange`, `symbol`, `instrument_type`, `mark_price`, `index_price`, `mark_index_spread`, `mark_index_ratio`, `open_interest`, `volume`, `turnover`, `funding_rate`, `summary_is_observed`, `minutes_since_summary_observation` |
| `options_ticker_snapshot_1m_observed`, `options_instrument_ticker_snapshot_1m_observed` | `timestamp`, `exchange`, `symbol`, `instrument_name`, `underlying`, `expiry`, `strike`, `underlying_price`, `index_price`, `option_type`, `mark_price`, `bid_price`, `ask_price`, `implied_volatility`, `delta`, `gamma`, `vega`, `theta`, `open_interest`, `volume`, `ingested_at`, `source_endpoint` |
| `options_surface_1m_feature` | `timestamp_m1`, `exchange`, `symbol`, `atm_iv`, `short_dated_iv`, `skew`, `term_structure`, `put_call_iv_spread`, `contract_count`, `fresh_quote_count`, `stale_quote_count`, `max_quote_age_seconds`, `quote_coverage_ratio` |
| `perps_l2_snapshot_1m_observed`, `options_l2_snapshot_1m_observed` | `timestamp`, `exchange`, `symbol`, `instrument_type`, `instrument_name`, `underlying`, `expiry`, `strike`, `option_type`, `best_bid_price`, `best_bid_size`, `best_ask_price`, `best_ask_size`, `bids`, `asks`, `ingested_at`, `source_endpoint` |
| `perps_l2_1m_feature`, `options_l2_1m_feature` | `timestamp_m1`, `exchange`, `symbol`, `instrument_type`, `instrument_name`, `underlying`, `expiry`, `strike`, `option_type`, `best_bid_price`, `best_ask_price`, `mid_price`, `spread`, `top_bid_size`, `top_ask_size`, `top_of_book_imbalance`, `bid_depth_10bps`, `ask_depth_10bps`, `bid_depth_50bps`, `ask_depth_50bps`, `quote_available`, `quote_age_seconds`, `stale_quote`, `minutes_since_l2_observation` |
| `recent_trade_snapshot_1m_observed` | `trade_time`, `exchange`, `symbol`, `instrument_type`, `instrument_name`, `underlying`, `expiry`, `strike`, `option_type`, `trade_id`, `deduplication_key`, `trade_id_is_source`, `price`, `quantity`, `side`, `snapshot_timestamp`, `snapshot_derived`, `ingested_at`, `source_endpoint` |
| `instrument_metadata_snapshot_daily_observed`, `futures_instrument_metadata_snapshot_daily_observed` | `snapshot_date`, `exchange`, `instrument_name`, `symbol`, `instrument_type`, `base_currency`, `quote_currency`, `settlement_currency`, `expiry`, `strike`, `option_type`, `tick_size`, `contract_size`, `min_trade_amount`, `creation_timestamp`, `is_active`, `is_listed`, `listing_state`, `ingested_at`, `source_endpoint` |
| `historical_volatility_observed` | `timestamp`, `exchange`, `symbol`, `historical_volatility`, `historical_volatility_source_timestamp`, `ingested_at`, `source_endpoint` |

### 4.7.3 Gold: physical status by repository origin

Historical Gold artifacts are built by this repository. Live Gold artifacts have lineage paths under
`/home/vcs/git/crypto-live-loader`; this is recorded in the transform-state JSON files.

The Gold layer is organized around two canonical model-ready datasets:

- `gold.market.history_full.m1` for historical data produced by `crypto-history-loader`
- `gold.live.full.m1` for live-origin data produced from `crypto-live-loader` inputs

Narrower Gold dataset IDs remain available as internal building blocks and compatibility outputs,
but downstream training and inference workflows should target the two full datasets.

| Origin | Gold dataset | Physical period / missing days | Variables |
|---|---|---|---|
| Historical | `gold.market.core.m1` | BTC 2018-08-14..2026-06-25, ETH 2019-03-14..2026-06-25, SOL 2022-04-29..2026-06-25; 0 | `timestamp_m1`, `exchange`, `symbol`, `spot_open_price`, `spot_high_price`, `spot_low_price`, `spot_close_price`, `spot_volume`, `perp_open_price`, `perp_high_price`, `perp_low_price`, `perp_close_price`, `perp_volume` |
| Historical | `gold.market.core_funding.m1` | Same core period; 0 | Core variables plus `funding_rate_last_known`, `minutes_since_funding`, `is_funding_observation_minute`, `funding_data_available` |
| Historical | `gold.market.perps_trades.m1` | BTC 2018-08-14..2026-06-11, ETH 2019-03-14..2026-05-29, SOL 2022-04-29..2022-12-30; 0 in Gold artifact | `timestamp_m1`, `exchange`, `symbol`, `trades_open_price`, `trades_high_price`, `trades_low_price`, `trades_close_price`, `trades_volume`, `trades_quote_volume`, `trades_trade_count`, `trades_buy_volume`, `trades_sell_volume`, `trades_buy_trade_count`, `trades_sell_trade_count`, `trades_buy_volume_share` |
| Historical | `gold.market.options_trades.m1` | BTC 2018-08-14..2026-06-25, ETH 2019-03-21..2026-06-11, SOL 2022-05-04..2022-12-30; 0 | Same trade variables with `option_trades_` prefix |
| Historical | `gold.market.full.m1` | BTC 2018-08-01..2026-06-25, ETH 2019-03-01..2026-06-25, SOL 2022-03-01..2026-06-25; 0 | Core plus OI flags, funding features, perp trade features, and option trade features; no IV/RV or regime variables |
| Live | `index_price_m1_features` | BTC/ETH/SOL 2026-05-24..2026-06-07; 0 | `schema_version`, `dataset_type`, `exchange`, `index_name`, `ts_minute`, `snapshot_count`, `price_open`, `price_high`, `price_low`, `price_close`, `price_mean`, `log_return_1m_mean` |
| Live | `l2_m1_features` | BTC/ETH/SOL 2026-05-05..2026-06-07; 0 | `ts_minute`, `exchange`, `symbol`, `instrument_type`, `depth`, `feature_set_version`, `snapshot_count`, `coverage_ratio`, `first_snapshot_ts`, `last_snapshot_ts`, `is_complete_minute`, `quality_flags`, mid/microprice OHLC statistics, spreads, imbalance, bid/ask depth, book pressure, mark/index/OI/funding fields |
| Live | `option_surface_m1` | BTC/ETH/SOL on 2026-05-24 only; 0 within that day | `schema_version`, `dataset_type`, `ts_minute`, `month`, `exchange`, `instrument_type`, `currency`, `expiry_date`, `term_days`, `term_bucket`, `atm_iv`, `atm_strike`, `atm_moneyness`, `iv_near_atm_call`, `iv_near_atm_put`, `open_interest_sum`, `volume_sum`, `contract_count`, `valid_surface_contract_count`, `surface_coverage_ratio`, `skew_slope`, `smile_curvature`, `rr25`, `bf25` |
| Live | `instrument_metadata_daily_summary` | Aggregate 2026-05-25..2026-06-07; 0 | `schema_version`, `dataset_type`, `exchange`, `snapshot_date`, `kind`, `base_currency`, `instrument_count`, `active_instrument_count`, `option_instrument_count`, `mean_strike` |

Contracts without physical Gold artifacts are `gold.market.history_full.m1`, `gold.market.iv_rv.m1`,
`gold.market.index_price.m1`, `gold.market.futures_summary.m1`, `gold.market.regime_features.m1`,
`gold.market.prediction_targets.m1`, `gold.live.volatility_features.m1`,
`gold.live.microstructure_features.m1`, `gold.live.full.m1`, and `gold.hybrid.full_l2.m1`. The existing
`gold.market.full.m1` must not be treated as an IV/RV-ready dataset until those feature columns and
manifests are rebuilt.

# 5. Example Commands

## 5.1 End-to-End Pipeline

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

Runs `bronze-build -> silver-build -> gold-build` using `medallion-pipeline` settings from
`config.yaml`, enforces single-run locking via `.run/full-pipeline.lock`, and writes a shared
append-only pipeline log. The configured code path supports volatility-index OHLC fields, but the
physical inventory in section 4.7 is authoritative: existing Gold artifacts must be rebuilt before
they can be considered IV/RV-ready.

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
    volatility_index_data volatility_index_snapshot_1m historical_volatility \
    index_price_snapshot_1m futures_summary_snapshot_1m \
    options_ticker_snapshot_1m options_instrument_ticker_snapshot_1m \
    options_surface_1m_feature perps_l2_snapshot_1m options_l2_snapshot_1m \
    recent_trade_snapshot_1m instrument_metadata_snapshot_daily \
    futures_instrument_metadata_snapshot_daily \
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
  --dataset-id gold.market.history_full.m1
```

`gold.market.history_full.m1` is the canonical historical Gold dataset. It joins historical
spot/perpetual OHLCV, funding, open interest, trades, realized volatility, and IV/RV features on
the minute grid, keeps optional historical references nullable, emits trailing strategy features,
and excludes forward-looking targets and labels.

Regime research Gold contract (optional Silver sources may be absent):

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.market.regime_features.m1 \
  --symbols BTC ETH
```

`gold.market.regime_features.m1` always keeps the same schema. Its manifest reports optional
source availability, minute coverage, source time span, and freshness; missing optional features
remain typed nulls and never change the required-source minute grid.

Live volatility Gold contract:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.live.volatility_features.m1 \
  --symbols BTC ETH
```

`gold.live.volatility_features.m1` is sourced only from `volatility_index_1m_feature`. It keeps the
historical `iv_*` feature names and minute timestamp semantics, records `as_of` and
`live_snapshot_derived` lineage, and leaves missing live minutes null instead of backfilling from
historical datasets.

Live microstructure Gold contract:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.live.microstructure_features.m1 \
  --symbols BTC ETH
```

`gold.live.microstructure_features.m1` joins `perps_l2_1m_feature` and
`options_l2_1m_feature` on the Gold minute grid. It preserves `perps_l2_*` book-state fields,
aggregates option-book rows into `options_l2_*` coverage and depth fields, records per-source
`*_as_of` and `*_live_snapshot_derived` lineage, and leaves missing live source minutes null.

Live full Gold contract:

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.live.full.m1 \
  --symbols BTC ETH
```

`gold.live.full.m1` is the canonical live Gold dataset. It combines live volatility-index,
perpetual-L2, and options-L2 feature families into one inference table, records
`origin_repository=crypto-live-loader` in the manifest, keeps optional live index/futures/option
surface features nullable, and never fills live gaps from historical datasets.

Inventory:

```bash
uv run python main.py dataset-inventory \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --format markdown \
  --output docs/dataset_inventory.md \
  --no-json-output
```

The inventory command is read-only for Lake data. It reports physical datasets, contracted but
unmaterialized outputs, schemas, row/file counts, per-series date spans, and missing calendar days.

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

- Canonical historical dataset: `gold.market.history_full.m1` (contract; not yet physically materialized)
- Canonical live dataset: `gold.live.full.m1` (contract; not yet physically materialized)
- `gold.market.perps_trades.m1` (`perps_trades` flow only)
- `gold.market.options_trades.m1` (`options_trades` flow only)
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.market.full.m1`
- `gold.market.iv_rv.m1` (contract; not yet physically materialized)
- `gold.market.index_price.m1` (contract; not yet physically materialized)
- `gold.market.futures_summary.m1` (contract; not yet physically materialized)
- `gold.market.regime_features.m1` (contract; not yet physically materialized)
- `gold.market.prediction_targets.m1` (contract; not yet physically materialized)
- `gold.live.volatility_features.m1` (contract; not yet physically materialized)
- `gold.live.microstructure_features.m1` (contract; not yet physically materialized)
- `gold.live.full.m1` (contract; not yet physically materialized)
- `gold.hybrid.full_l2.m1` (contract; not yet physically materialized)

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
uv run python scripts/validate_conventional_commit.py --latest
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
| `uv run python scripts/validate_conventional_commit.py --latest` | Commit policy | Enforce Conventional Commit subjects for local commits, PR titles, and squash commits. | Non-compliant commit or PR title such as missing `type:` prefix. |
| `uv run --extra dev pytest` | Behavioral + regression tests | Validate functional behavior in parallel and enforce coverage thresholds. | Test failures, behavioral regressions, or coverage below configured threshold. |

Operational notes:

- `pytest` coverage and parallel execution defaults are configured in `pyproject.toml`; xdist uses logical CPU workers capped at 4 with load-scope distribution.
- Pre-commit enforces the same logical quality-gate path used in CI.
- Commit messages and PR titles must follow Conventional Commits, for example
  `docs: update README missing day snapshot` or `feat(gold): add live full dataset contract`.
- GitHub repository gates are configured through the versioned CLI script
  `scripts/github/apply_quality_gates.sh`. The GitHub web UI is only an inspection surface; rerun
  the script after intentional changes to required checks, merge policy, or branch protection.
- Pull requests run the required `pr-quality` job before merge.
- Pushes to `main` and merge-queue candidates run the full `main-quality` job, including coverage.
- If GitHub rejects merge-queue setup through the API, the script keeps branch protection in place
  and reports the remaining manual UI action.

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
