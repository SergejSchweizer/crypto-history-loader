# Architecture

## Purpose

`crypto-history-loader` is a deterministic medallion data pipeline for historical cryptocurrency
market data. It fetches exchange data, persists audit-friendly Bronze records, produces canonical
Silver datasets, and publishes versioned Gold datasets for research, training, and inference.

This document defines durable package boundaries, data-flow rules, side-effect ownership, and
change protocol. Dataset-specific Gold contracts and feature semantics belong exclusively in
[`DATASETS.md`](DATASETS.md).

## Architectural principles

- Keep exchange-facing IO outside application policy.
- Define typed dataset and request contracts before orchestration code.
- Make transformations deterministic, idempotent, and restart-safe.
- Keep timestamps UTC and make alignment, deduplication, fill, and lookback rules explicit.
- Preserve raw source evidence in Bronze.
- Keep Silver reusable and free of forward-looking model labels.
- Keep forward-looking targets isolated from feature-only datasets.
- Put filesystem, subprocess, network, plotting, and logging effects behind explicit adapters.
- Version research-facing outputs and record enough lineage to reproduce every build.
- Prefer additive, backward-compatible schema evolution unless an intentional versioned break is
  documented.

## Repository boundaries

| Path | Responsibility |
|---|---|
| `api/` | CLI parsing, command dispatch, presentation, and process exit semantics |
| `application/` | Typed contracts, planning, orchestration, transformation policy, and reporting |
| `ingestion/` | Exchange clients, source parsing, Bronze storage IO, and source-facing adapters |
| `scripts/` | Operational entrypoints and repository validation/maintenance commands |
| `tests/` | Unit, integration, architecture, contract, and regression coverage |
| `docs/` | Generated physical inventory and supporting documentation assets |
| `DATASETS.md` | Canonical Gold dataset and feature catalog |
| `config.yaml` | Canonical runtime configuration |
| `main.py` | Thin executable wrapper around the CLI |

Allowed dependency direction is from presentation and infrastructure toward application contracts,
not the reverse. Application policy must not depend on CLI parser objects, concrete exchange
clients, or storage implementation details.

## Runtime flow

```text
CLI / scheduler
      |
      v
typed request + validated config
      |
      v
application orchestration
      |
      +--> exchange/source adapters
      +--> transformation services
      +--> storage/versioning adapters
      +--> manifests, plots, logs, reports
```

The CLI owns argument parsing and user-facing output. Application services own workflow policy.
Ingestion adapters own source communication and Bronze writes. Transformation services own
schema-normalization and feature logic. Versioning and audit services own Gold manifests,
retention, hashes, and reproducibility metadata.

## Medallion flow

```text
exchange APIs / live-loader artifacts
                |
                v
             Bronze
     append-oriented source evidence
                |
                v
             Silver
 canonical observed and feature datasets
                |
                v
              Gold
 versioned model-ready joins and targets
```

### Bronze

Bronze is the auditable source layer.

- Source payloads are normalized into stable records without hiding exchange semantics.
- Writes are append-oriented, partitioned, deterministic, and safe to resume.
- Checkpoints record completed fetch tasks incrementally.
- Full gap-fill runs rescan internal, head, and tail coverage.
- Source endpoint, run ID, ingest time, event time, and original payload remain available for
  reconciliation and replay.
- Bronze does not contain model labels.

### Silver

Silver is the canonical reusable transformation layer.

- Contracts are declared in `application/dataset_contracts.py`.
- Output columns, timestamp semantics, missing-data policy, and quantitative semantics are
  explicit.
- Builders normalize types, timestamps, symbols, deduplication, observation flags, and source
  freshness.
- Observed datasets preserve native event cadence.
- Feature datasets may align to a one-minute grid only when their contract explicitly requires it.
- Rolling calculations read sufficient prior calendar context and write only the target partition.
- IV and RV units, horizons, estimators, annualization basis, source-selection policy, and null
  policy are machine-readable.
- Silver features use current and trailing information only.

### Gold

Gold is the versioned model-ready publication layer.

- The typed registry in `application/dataset_contracts.py` declares dataset IDs and required or
  optional sources.
- `application/services/gold_frames.py` normalizes source frames and constructs reusable feature
  and target families.
- `application/services/gold_service.py` owns joins, final column selection, versioning, manifests,
  retention, and writes.
- Required sources define the dataset grid. Optional sources preserve a stable typed nullable
  schema when absent.
- Event aggregates are not forward-filled.
- Forward-looking targets are emitted only by the dedicated prediction-target contract.
- Every build records feature-set hash, source-data hash, Git commit, source lineage, coverage,
  version bump reason, and artifact paths.
- Only the latest three versions are retained per dataset/exchange/symbol lineage.
- The complete dataset-by-dataset schema and every feature definition are maintained in
  `DATASETS.md`; they must not be duplicated here or in `README.md`.

## Time and alignment rules

- All internal timestamps are timezone-aware UTC.
- Minute features use a closed one-minute key named `timestamp_m1`.
- Source observations keep their own source timestamp alongside the aligned minute when freshness
  matters.
- Required-source Gold joins use a union grid unless a contract explicitly states otherwise.
- Missing required-source values remain null.
- Optional source absence never removes contracted columns.
- A forward-filled Silver state must expose whether the value was observed or filled and how old
  the source observation is.
- Trailing windows must not read future rows.
- Prediction targets must require a complete future horizon; incomplete horizons remain null.

## Quantitative correctness

- Raw realized-volatility windows are non-annualized `sqrt(sum(log_return^2))` estimators.
- Annualized RV siblings use a documented 365-day scaling basis and percentage-point units.
- Deribit DVOL-style IV is a 30-day annualized percentage-point measure.
- Comparable IV/RV analytics use matched 30-day annualized percentage-point fields.
- Legacy mixed-unit IV/RV fields remain only for backward compatibility and must be marked
  deprecated in dataset documentation.
- Canonical RV source selection is stable for the whole symbol and must not switch row by row.
- Numerical operations must handle non-positive prices, zero denominators, insufficient windows,
  and missing observations explicitly.

## Side-effect ownership

| Side effect | Owner |
|---|---|
| Exchange HTTP requests | `ingestion/` exchange adapters |
| Bronze parquet and sidecars | Bronze storage services in `ingestion/` |
| Silver parquet, manifests, and plots | Silver application services through storage/plot adapters |
| Gold parquet, manifests, plots, and retention | Gold service plus versioning/audit adapters |
| Checkpoints and run locks | Application runtime/checkpoint adapters |
| Logs | Shared logging configuration with module-specific files under `.logs/` |
| GitHub repository policy | Versioned scripts under `scripts/github/` |

Pure transformation functions must not discover files, read configuration implicitly, call the
network, mutate process-global state, or write artifacts.

## Configuration

`config.yaml` is the runtime source of truth. Configuration is parsed and validated before workflow
execution. CLI overrides must be explicit and must not mutate module-global configuration.

Important configuration domains include:

- exchange and symbol selection
- Bronze start bounds and fetch limits
- Silver/Gold roots and partitioning
- medallion scheduler behavior
- concurrency bounds
- logging paths
- manifests, plots, and maintenance policy

## Concurrency and restart safety

- Work is bounded by explicit process limits.
- Scheduling order and partition keys are deterministic.
- Re-running a completed partition must not create semantically different output from identical
  input.
- Checkpoints are plan-aware and may not silently apply to a different effective request.
- Locks prevent overlapping full-pipeline runs.
- Partial writes use atomic replacement or versioned build directories.
- Maintenance jobs must be safe to retry.

## Versioning and lineage

Gold version changes are derived from contract and source changes rather than wall-clock time alone.

A Gold manifest records at least:

- dataset ID and semantic version
- exchange and normalized symbol
- row count, columns, and timestamp span
- feature-set hash
- source-data hash
- Git commit hash
- required and optional source lineage
- optional-source availability and coverage
- version bump level and reason
- parquet, manifest, and plot paths

Physical inventory is generated into `docs/dataset_inventory.md`. It is evidence of the local Lake,
not the schema source of truth.

## Validation gates

The repository keeps local pre-commit and GitHub Actions gates logically aligned:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pyright --level error
uv run ty check
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.yaml
uv run python scripts/validate_readme_inventory.py
uv run python scripts/validate_conventional_commit.py --latest
uv run --extra dev pytest
```

The catalog validator checks `DATASETS.md` against the typed Gold contract registry and validates
physical inventory origin policy. CI remains the final merge-readiness authority.

## Documentation ownership

| Information | Canonical document |
|---|---|
| User-facing setup, Bronze/Silver overview, and commands | `README.md` |
| Package boundaries, data flow, side effects, and change rules | `ARCHITECTURE.md` |
| Gold dataset IDs, sources, null/alignment policy, every feature, and feature meaning | `DATASETS.md` |
| Physical Lake files, rows, coverage, and materialization state | `docs/dataset_inventory.md` |
| Planned and historical implementation work | `BACKLOG.md` |
| Repository operating policy | `AGENTS.md` |
| Generated project history | `DECISIONS.md`, `RISKS.md`, `TIMELINE.md` |

Historical backlog and history documents may mention earlier Gold decisions as an audit trail, but
they must not be treated as current schema documentation.

## Change protocol

A dataset or feature change is complete only when the same change set updates:

1. typed contracts and transformation code;
2. focused unit/integration tests;
3. `DATASETS.md` for Gold membership or feature semantics;
4. `README.md` when commands or user workflow change;
5. `ARCHITECTURE.md` only when a durable boundary or invariant changes;
6. generated physical inventory after artifacts are rebuilt;
7. backlog/history documentation when planning or audit status changes.

Do not duplicate Gold feature tables in other documentation. Link to `DATASETS.md` instead.
