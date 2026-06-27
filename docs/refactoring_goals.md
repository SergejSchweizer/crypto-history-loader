# Refactoring Goals

This document defines the end state for the project-wide refactoring. A phase is complete only when the stated evidence exists in the repository and the listed validation commands pass.

## Current Baseline

The codebase is already split into `api`, `application`, and `ingestion`, but several modules still own too many responsibilities:

| Area | Current signal | Refactoring risk |
|---|---:|---|
| `application/services/fetch_service.py` | 1,854 lines | Fetch planning, windowing, task execution, retries, and reporting are coupled. |
| `application/services/silver_service.py` | 1,322 lines | Dataset-specific transformations share one large service surface; Silver sidecar writing is isolated in `application/services/silver_sidecars.py`. |
| `ingestion/lake.py` | 1,248 lines | Bronze persistence, lake reads, and schema handling remain coupled; partition layout helpers are isolated in `ingestion/lake_layout.py`, and Bronze sidecar generation/repair is isolated in `ingestion/lake_sidecars.py`. |
| `api/commands/loader.py` | 998 lines | CLI orchestration, runtime state, checkpoint glue, and command behavior are coupled. |
| `application/services/gold_service.py` | 929 lines | Frame loading, validation, joins, and output writing remain coupled after feature profiling extraction. |
| `ingestion/feature_profile.py` | 416 lines | Shared feature metadata and plotting are isolated, but still adapter-heavy and need interface extraction. |

Existing safety net:

- 41 test modules exist under `tests/`.
- Import-linter currently enforces four dependency contracts: `application -> api`, `ingestion -> api`,
  `ingestion -> application`, and `ingestion.exchanges -> application` are forbidden.
- Repository tooling currently targets Python 3.11 and Pyright standard mode.
- CI, pre-commit, `make check`, and `tests/test_quality_gate_contract.py` now assert the same core quality-gate
  sequence: Ruff lint/format, Mypy, Pyright, Ty, import-linter, config validation, and pytest.
- Coverage enforcement is configured in `pyproject.toml` under `[tool.coverage.report]`.
- Fetch task timeout, heartbeat, and trade window sizing are now resolved through
  `application/services/fetch_runtime_policy.py`; durable `config.yaml` values for the main fetch/trade knobs are
  bounded by Pydantic validation.

## Final Refactoring Objectives

### 1. Architecture Boundaries Are Explicit

End state:

- `api/` owns argument parsing, command output shape, and command-level orchestration only.
- `application/` owns use-case policies, typed DTOs, fetch/build planning, transformation contracts, and restart behavior.
- `ingestion/` owns external exchange adapters, HTTP access, lake persistence, and plotting/storage adapters.
- No `application` module imports `api`.
- No `ingestion` module imports `api` or `application.services.*` transformation logic.
- Persistence and plotting are side effects behind explicit application-facing interfaces.

Evidence:

- `.importlinter` contains contracts for forbidden dependency directions.
- `tests/test_import_linter.py` runs those contracts in CI.
- Public cross-layer data uses DTOs, dataclasses, `TypedDict`, Pydantic models, or explicit schema helpers.

### 2. Bronze Runtime Is Modular And Restart-Safe

End state:

- Bronze fetch planning is separate from execution.
- Fetch execution is separate from persistence.
- Checkpointing is separate from output/report formatting.
- Runtime policy, including concurrency, timeout, heartbeat, page size, and trade window sizing, has one validated configuration path.
- Every long-running fetch logs start, progress, retry/failure classification, persisted partition, and completion.
- Trade, OHLCV, OI, and funding flows share common execution contracts where behavior is genuinely common.

Evidence:

- `api/commands/loader.py` is reduced to command assembly and no longer owns mutable runtime globals.
- `application/services/fetch_service.py` is split into smaller modules by concern.
- Checkpoint resume tests cover completed, partial, stale, and failed task states.
- Trade gap-fill tests cover dense windows, empty windows, retryable failures, all-window failures, and restart after partial persistence.

### 3. Silver And Gold Transformations Have Dataset Contracts

End state:

- Each Silver output dataset has an explicit input schema, output schema, timestamp semantics, and missing-data policy.
- Gold build inputs are declared by dataset ID and feature contract rather than embedded conditionals.
- Feature metadata and validation results are generated consistently across all Gold datasets.
- Timestamp normalization, as-of joins, forward fill, rolling windows, and leakage-prevention choices are documented in code where the logic is non-obvious.

Evidence:

- Dataset contracts live in typed modules, not README prose only.
- Tests validate every supported Silver and Gold dataset contract.
- Gold service reports missing values, invalid rows, input freshness, and feature coverage from shared helpers.

### 4. Lake IO Is A Clear Adapter

End state:

- Bronze write paths expose one typed writer interface.
- Lake read paths expose one typed reader/query interface per layer.
- Plotting and sidecar repair are not mixed into core write/read functions.
- Partition layout decisions are centralized and tested.
- Writes remain idempotent and deterministic.

Evidence:

- `ingestion/lake.py` is split into focused modules or classes for partition paths, Bronze writes, Bronze reads, Silver/Gold reads, and sidecar repair.
- Existing partition layout remains backward compatible.
- Tests prove repeated writes do not duplicate natural keys or regress partition metadata.

### 5. Configuration And Runtime Policy Are Centralized

End state:

- `config.yaml` is the canonical durable runtime configuration source.
- Environment variables are supported only through typed runtime-policy readers.
- New options are documented and validated.
- No new ad hoc `os.getenv` calls are introduced outside runtime-policy modules or low-level adapters where a direct environment override is intentionally adapter-local.

Evidence:

- `application/services/config_validation.py` validates all durable config sections.
- Runtime policy tests cover valid, missing, invalid, minimum, and maximum values.
- README documents operational knobs and defaults.

### 6. Quality Gates Match The Repository Policy

End state:

- Ruff lint and format are configured in `pyproject.toml`.
- Type checking runs consistently in CI and local validation.
- Import boundary checks run in CI and pre-commit.
- Test coverage threshold is explicit and enforced.
- Docstring coverage/signature checks are either configured or the repository policy is updated to explain why they are deferred.

Evidence:

- `.pre-commit-config.yaml` exists and mirrors CI checks.
- `.github/workflows/ci.yml` runs the same logical checks.
- `pyproject.toml` contains one canonical configuration source for Ruff, Pyright or mypy, pytest, and coverage.

### 7. Documentation Tracks Behavior

End state:

- README describes the current pipeline architecture and runtime commands.
- Dataset coverage statistics have a reproducible generation path.
- Refactoring decisions that affect runtime behavior are captured in docs or ADR-style notes.
- Known limitations and follow-up work are explicit.

Evidence:

- README links to this document.
- Coverage tables can be regenerated by a documented command.
- Release notes or PR bodies identify operational impact and rollback path for runtime-sensitive changes.

## Completion Checklist

The refactoring is complete only when all items below are true:

- [ ] Architecture contracts cover forbidden dependency directions, circular imports, infrastructure leaking into domain logic, API/presentation importing persistence internals, and dependency-heavy shared utilities.
- [ ] `api/commands/loader.py`, `application/services/fetch_service.py`, `application/services/silver_service.py`, `application/services/gold_service.py`, and `ingestion/lake.py` have each been split or justified with explicit ownership boundaries.
- [ ] Bronze fetch planning, execution, persistence, checkpointing, and reporting are separately testable.
- [ ] Silver and Gold dataset contracts are explicit and tested.
- [ ] Runtime configuration is typed, validated, documented, and covered by tests.
- [ ] Logging uses the shared `.logs` root and consistent message structure.
- [ ] README and operational docs match actual behavior.
- [ ] CI and local validation gates are aligned.
- [ ] Full validation passes.

## Required Validation Commands

Run these before marking the project-wide refactoring complete:

```bash
ruff check .
ruff format --check .
pyright
lint-imports --config .importlinter
pytest -q
pytest --cov --cov-report=term-missing
python scripts/validate_config_with_pydantic.py
```

If docstring gates are enabled during the refactor, also run:

```bash
interrogate .
pydoclint src
```

## Phase Plan

### Phase 1: Refactoring Contract And Guardrails

- Add this goals document.
- Expand import-linter contracts.
- Add or update architecture tests for configured boundaries.
- Align documented validation commands with actual tooling.

Progress:

- Architecture contracts now include an acyclic-sibling check for the root packages, in addition to the existing
  forbidden dependency directions.

Exit criteria:

- Architecture and validation expectations are explicit.
- Shared feature profiling is no longer owned by Gold transformation code.
- `ingestion` has no dependency on `application`.
- Existing tests still pass.

### Phase 2: Runtime Policy And Configuration

- Move runtime/environment parsing into typed policy modules.
- Validate durable runtime config.
- Document operational knobs.

Exit criteria:

- New runtime behavior is configured through typed policy readers.
- Policy tests cover bounds and invalid values.
- Durable `config.yaml` fetch/trade knobs are validated before command execution.

### Phase 3: Bronze Orchestration Split

- Split loader command assembly from mutable runtime state.
- Split fetch planning, task execution, persistence, checkpointing, and reporting.
- Preserve restart-safe behavior.

Exit criteria:

- Bronze components are independently testable.
- Checkpoint key mapping, alias hydration, and pending-task filtering live in
  `application/services/bronze_runtime_service.py`, with `api/commands/loader_checkpoint.py` kept as a compatibility
  facade.
- Bronze start-bound parsing and tail-mode bound resolution live in
  `application/services/bronze_runtime_service.py`; `api/commands/loader.py` now carries one
  `BronzeRuntimeBoundsContext` instead of separate mutable start-bound globals.
- Bronze output, storage buffers, and pending task lists are grouped in `BronzeRunState`, reducing scattered mutable
  containers inside `run_bronze_build`.
- Existing CLI behavior remains backward compatible.

### Phase 4: Lake Adapter Split

- Separate partition layout, Bronze writes, lake reads, sidecar repair, and plotting hooks.
- Preserve existing lake path compatibility.

Exit criteria:

- Partition layout path construction and parquet path parsing live in `ingestion/lake_layout.py` and are tested
  against both current `year/month/date` and previous `month/date` layouts.
- Bronze sidecar manifest/plot generation and backfill repair live in `ingestion/lake_sidecars.py`; `ingestion.lake`
  keeps compatibility imports for existing callers and write-path integration.
- Persistence side effects are isolated.
- Idempotency tests cover repeated writes and partial reruns.

### Phase 5: Silver And Gold Contract Extraction

- Extract per-dataset transformation contracts.
- Reduce large service modules by dataset family and shared frame utilities.
- Strengthen missing-data and timestamp semantics tests.

Progress:

- Silver output column, timestamp semantics, and missing-data policy contracts now live in
  `application/dataset_contracts.py`.
- Gold dataset requirements and L2 inclusion flags now live in typed contracts, with
  `application/services/gold_service.py` keeping the previous public constants as compatibility views.
- Silver monthly manifest and plot sidecar writing now lives in `application/services/silver_sidecars.py`, keeping
  side effects separate from Silver transformation functions.

Exit criteria:

- Dataset transformations are discoverable by contract.
- Feature outputs remain backward compatible unless versioned.

### Phase 6: Final Hardening

- Align CI, pre-commit, docs, coverage, and type gates.
- Run full validation.
- Update README and release notes.

Progress:

- Script logging now reuses the runtime logging adapter, keeping module-specific files and the unified log message
  structure consistent across CLI commands and maintenance scripts.

Exit criteria:

- CI, pre-commit, `make check`, and repository tests enforce the same required gate list.
- Completion checklist is fully checked.
- Full validation passes or any skipped gate has an explicit documented reason and follow-up.
