# crypto-history-loader

Historical and live-origin crypto market-data loader with a contracted Bronze/Silver/Gold Lake for
reproducible quantitative research. The repository currently focuses on Deribit data and canonical
BTC, ETH, and SOL market state.

## Documentation map

| Document | Ownership |
|---|---|
| [`README.md`](README.md) | Setup, commands, operations, and repository entry points |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Components, boundaries, data flow, and reliability rules |
| [`DATASETS.md`](DATASETS.md) | Canonical Gold dataset catalog, exact feature membership, and feature definitions |
| [`docs/dataset_inventory.md`](docs/dataset_inventory.md) | Generated physical Lake inventory and dated coverage snapshot |
| [`BACKLOG.md`](BACKLOG.md) | Historical and planned implementation work |
| [`DECISIONS.md`](DECISIONS.md) | Generated decision history |
| [`RISKS.md`](RISKS.md) | Generated risk history |
| [`TIMELINE.md`](TIMELINE.md) | Generated delivery timeline |

Gold dataset IDs and feature schemas are documented only in [`DATASETS.md`](DATASETS.md). This
prevents the README, architecture document, and generated inventory from drifting into competing
schema references.

## Medallion model

### Bronze

Bronze stores source-shaped, audit-friendly records with normalized partition paths and ingestion
metadata. Historical fetches are idempotent and gap-aware.

Historical families fetched directly by this repository include:

- spot and perpetual OHLCV;
- funding and open interest;
- perpetual and option trades;
- historical volatility-index data.

Live-origin Bronze families produced by `crypto-live-loader` and consumed here include index-price,
futures-summary, volatility-index, option ticker/surface, L2, recent-trade, and instrument-metadata
snapshots.

### Silver

Silver owns normalization, deduplication, timestamp semantics, observation flags, explicit
forward-fill policy, and reusable market-state features. Contracts are declared in
`application/dataset_contracts.py`.

Major Silver outputs include:

- canonical spot/perpetual OHLCV;
- observed and minute-feature funding/open-interest data;
- observed and minute-aggregate perpetual/option trades;
- implied-volatility index features;
- realized-volatility and IV/RV state;
- index-price and futures-summary state;
- perpetual/option L2 state;
- option-surface and historical-volatility references.

Silver creates reusable state, not model labels. Forward-looking targets belong in Gold.

### Gold

Gold joins contracted Silver sources into versioned, model-ready datasets. It records source
lineage, coverage, feature-set hashes, source-data hashes, Git commit, and semantic versioning in
manifests. Broad and narrow contracts, every emitted feature, null/alignment rules, and target
semantics are defined in [`DATASETS.md`](DATASETS.md).

## Requirements

- Python 3.14
- [`uv`](https://docs.astral.sh/uv/)
- Network access to configured exchange APIs for Bronze collection
- Sufficient local storage for Parquet Lake artifacts

Install dependencies:

```bash
uv sync --extra dev
```

Copy and adjust configuration where required:

```bash
cp .env.example .env
```

Primary configuration lives in `config.yaml`. Environment overrides are supported for secrets and
runtime deployment concerns. Do not commit credentials or generated Lake data.

## Core commands

Show CLI help:

```bash
uv run python main.py --help
```

Build Bronze data:

```bash
uv run python main.py bronze-build --config config.yaml
```

Build selected Silver families:

```bash
uv run python main.py silver-build \
  --dataset spot_ohlcv perps_ohlcv funding open_interest perps_trades options_trades \
  --manifest \
  --plot \
  --maxprocesses 4 \
  --no-json-output
```

Build all supported Gold contracts:

```bash
uv run python main.py gold-build \
  --manifest \
  --plot \
  --maxprocesses 4 \
  --no-json-output
```

Build one Gold contract:

```bash
uv run python main.py gold-build \
  --dataset-id gold.market.history_full.m1 \
  --manifest \
  --plot \
  --maxprocesses 4 \
  --no-json-output
```

Run the complete medallion scheduler:

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

The scheduler intentionally omits `--dataset-id` in its Gold step so every registered Gold contract
is considered.

## Lake layout

The canonical roots are configured in `config.yaml` and normally resolve beneath `lake/`:

```text
lake/
├── bronze/
│   └── dataset_type=<type>/exchange=<exchange>/.../*.parquet
├── silver/
│   └── dataset_type=<type>/exchange=<exchange>/symbol=<symbol>/timeframe=<tf>/.../*.parquet
└── gold/
    └── dataset_id=<id>/exchange=<exchange>/symbol=<symbol>/version=<semver>/build_id=<id>/
        ├── data.parquet
        └── manifest.json
```

Generated Lake data is ignored by Git and must not be committed.

## Data semantics

### Time

- Timestamps are normalized to UTC.
- Minute datasets use explicit timestamp columns and deterministic sorting.
- Gold uses `timestamp_m1`, `exchange`, and normalized base `symbol` as its standard key.
- Required Gold sources use a union minute grid; gaps remain visible as nulls unless Silver exposes
  an explicit last-known state.

### Missing data

- Observation and forward-fill state are represented explicitly where applicable.
- Trade aggregates are not forward-filled.
- Optional Gold sources produce stable nullable columns when absent.
- Prediction targets remain null when the complete future horizon is unavailable.

### Idempotency and gaps

Bronze fetches and Lake writes are restart-safe. Full-gap-fill runs rescan head, internal, and tail
gaps. Sidecars and checkpoints are repaired selectively rather than hiding inconsistent state.

### Quantitative correctness

- Raw realized-volatility windows are non-annualized `sqrt(sum(log_return^2))` estimates.
- Annualized RV percentage-point fields use a 365-day basis.
- Unit-safe IV/RV comparisons use matched 30-day annualized percentage-point fields.
- Legacy mixed-unit IV/RV fields remain for compatibility and are marked deprecated in
  [`DATASETS.md`](DATASETS.md).
- Feature-only Gold contracts use present/trailing information; forward-looking values are isolated
  in the prediction-target contract.

## Physical inventory

Regenerate the dated Lake inventory:

```bash
uv run python scripts/update_dataset_inventory.py
```

The generated report is [`docs/dataset_inventory.md`](docs/dataset_inventory.md). It is evidence of
physical files and coverage at generation time, not the canonical dataset contract.

Validate the Gold catalog against the typed registry and inventory policy:

```bash
uv run python scripts/validate_readme_inventory.py
```

The script path is retained for command compatibility; it now validates `DATASETS.md` by default.

## Quality gates

Run the repository quality suite:

```bash
make check
```

The configured gates include:

- Ruff lint and formatting checks;
- Mypy, Pyright, and `ty` type checks;
- import-boundary checks;
- Pydantic configuration validation;
- Gold dataset catalog validation;
- Conventional Commit validation;
- unit and integration tests with coverage enforcement.

For focused local work, run the smallest relevant test set first, then the full suite before
publication.

## Automation and cron

Use the medallion scheduler rather than duplicating long Bronze/Silver/Gold command lines in cron.
A typical crontab entry is:

```cron
15 2 * * * cd /path/to/crypto-history-loader && /usr/bin/flock -n /tmp/crypto-history-loader.lock uv run python scripts/run_medallion_pipeline.py --config config.yaml >> .logs/cron.log 2>&1
```

Adjust paths to the deployment. Keep one lock owner and one shared `.logs` root so overlapping runs
cannot corrupt checkpoints or artifacts.

## Repository rules

- Dataset contract changes must update typed contracts, transformations, tests, and
  [`DATASETS.md`](DATASETS.md) in the same change.
- Physical coverage changes update the generated inventory, not the semantic catalog.
- Historical backlog, decision, risk, and timeline records may mention prior Gold designs but must
  not become competing current-schema documentation.
- Preserve deterministic column order, partition layout, deduplication, and sort keys.
- Never commit credentials, Lake outputs, local manifests, plots, or logs.

## License

See [`LICENSE`](LICENSE).