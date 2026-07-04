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

Silver and Gold contracts live in `application/dataset_contracts.py`. If a dataset is renamed,
added, or removed, update all of the following in the same change set:

- `application/datasets.py`
- `application/dataset_contracts.py`
- Bronze/Silver/Gold command choices
- lake path helpers and sidecar defaults
- README dataset tables and examples
- this file
- focused tests for planning, contracts, storage, Silver, Gold, and CLI parsing

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

## Update Protocol

Update `ARCHITECTURE.md` in the same PR when a change does any of the following:

- changes package dependency direction or import-linter contracts
- changes a dataset name, path, schema, timestamp semantics, or missing-data policy
- changes Bronze/Silver/Gold orchestration or restart behavior
- changes side-effect ownership for HTTP, lake IO, plotting, manifests, or CLI output
- changes runtime configuration, quality gates, or documented validation commands

If the architecture does not change, no edit is required. If behavior changes but this file does not,
the PR should explain why the existing architecture contract still applies.
