# Backlog

This file is the single implementation backlog for `crypto-loader`.

Last updated: 2026-08-24

## Backlog policy

- `BACKLOG.md` is the only backlog file in the repository. Do not create `BACKLOG_*.md`, `docs/backlog/*.md`, or another parallel backlog.
- Planned and in-progress work stays detailed in this file. Completed work is removed from the active section and summarized only in **Completed PR summary** at the end.
- Every active ticket must contain numbered `Description` requirements (`R1`, `R2`, ...) and matching numbered `Acceptance` checks (`A1`, `A2`, ...). `A1` verifies only `R1`, `A2` verifies only `R2`, and so on.
- Every active ticket must contain `Git branch` and `Git status` fields.
- Keep each ticket atomic: one contract boundary, one pure planner, one adapter, one operational concern, or one integration concern.
- Agents must not broaden scope into another ticket. Missing dependency contracts are a hard stop, not permission to reimplement them locally.
- Every implementation commit and squash-merge title must include the backlog identifier, for example `feat(PR-68): ...`.
- Use a separate checkout/worktree per parallel agent. Parallel agents must never share one working tree.
- Before editing any ticket: `git fetch origin`, switch to current `main`, fast-forward only, then run `git status --short`. Any output is a hard stop.
- Before handoff, run `git status --short` again and record the exact output in the PR/handoff evidence.

## Current operational baseline

The repository implements a deterministic Medallion pipeline:

```text
Bronze -> Silver -> Gold
```

Bronze dataset types present locally:

```text
funding
futures_instrument_metadata_snapshot_daily
futures_summary_snapshot_1m
historical_volatility
index_price_snapshot_1m
instrument_metadata_snapshot_daily
open_interest
options_instrument_ticker_snapshot_1m
options_l2_snapshot_1m
options_ticker_snapshot_1m
options_trades
perps_l2_snapshot_1m
perps_ohlcv
perps_trades
recent_trade_snapshot_1m
spot_ohlcv
volatility_index_data
volatility_index_snapshot_1m
```

Parquet Gold remains the canonical source of truth. The active PostgreSQL stack below adds a rebuildable serving-plane replica after Gold; it does not move canonical ownership away from Parquet.

The PostgreSQL endpoint is exactly `10.10.1.3:54321`. The dedicated runtime LOGIN role is exactly `crypto-loader`. The operational password is supplied only from protected runtime configuration/environment and must never be committed, printed, logged, embedded in examples, placed in command-line arguments, or persisted in sync metadata. Administrator credentials are separate from application runtime credentials.

PostgreSQL consumer data lives in schema `crypto_loader`. Synchronization state lives separately in schema `crypto_loader_sync`. Every registered current Gold dataset maps one-to-one to a consumer table whose name is derived deterministically from the dataset ID by replacing `.` with `_`, for example:

```text
gold.market.regime_features.m1
-> crypto_loader.gold_market_regime_features_m1
```

All mapped names must be unique and fit PostgreSQL's 63-byte identifier limit. Collisions or overlong names are hard errors.

Each consumer table mirrors the current Parquet Gold row schema and uses the composite logical key:

```text
(exchange, symbol, timestamp_m1)
```

Every publishable current Gold contract must expose those three fields. Unsupported or ambiguous source types fail before any write. Existing table/source schema-signature mismatch is a migration-required error. Normal sync must never `DROP`, `TRUNCATE`, replace a table, delete-all, or silently mutate a live schema.

### Timestamp compatibility contract

The timestamp contract must be type-compatible with the implemented `regime-loader` and `xetra-loader` PostgreSQL serving paths.

Canonical source timestamp type:

```text
timestamp_m1: Polars Datetime(time_unit="us", time_zone="UTC")
```

PostgreSQL timestamp type:

```text
TIMESTAMPTZ(6)
```

Mandatory invariants:

- `timestamp_m1` must be normalized and validated as UTC-aware microsecond precision before hashing or PostgreSQL mutation.
- Every Gold source column whose logical type is timestamp/datetime must use UTC-aware microsecond semantics at the PostgreSQL sync boundary; true calendar-date fields remain `DATE` and are not converted to timestamps.
- Every PostgreSQL consumer timestamp/datetime column is exactly `TIMESTAMPTZ(6)`.
- Internal sync timestamps are also exactly `TIMESTAMPTZ(6)`: `gold_sync_state.min_timestamp`, `gold_sync_state.max_timestamp`, `gold_sync_state.synced_at_utc`, and `gold_row_hashes.timestamp_m1`.
- Every PostgreSQL sync session explicitly uses timezone `UTC`.
- `TIMESTAMP WITHOUT TIME ZONE`, naive datetimes, non-UTC timestamp semantics, millisecond/nanosecond timestamp storage, and PostgreSQL timestamp precision other than `(6)` are forbidden at this boundary.
- The PostgreSQL persistence boundary receives only timezone-aware UTC values with zero offset; any non-UTC aware value must be normalized upstream before the persistence contract and is rejected if it reaches that boundary.
- Timestamp round-trip tests must prove that an aware UTC microsecond source value is read back from PostgreSQL with identical instant and microsecond precision.
- PostgreSQL stores typed instants rather than a textual datetime format. Where acceptance artifacts/logs serialize an instant, the canonical diagnostic form is `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
- This is the shared `pg-temporal-v1` convention across `crypto-loader`, `regime-loader`, and `xetra-loader`; observation semantics remain repository-specific.

Synchronization is state reconciliation, not a timestamp-watermark feed:

- First successful sync of a `(dataset_id, exchange, symbol)` lineage inserts the complete current Gold history.
- Later sync checks the current Gold source fingerprint against the last successful sync state.
- If the fingerprint is unchanged, zero consumer-row mutations are allowed; target count/min/max are still verified.
- If the fingerprint changed, current Gold is hashed row-by-row and compared with the complete PostgreSQL digest state for that lineage.
- Only accumulated `INSERT`, `UPDATE`, and `DELETE` deltas are written. Unchanged rows are never rewritten.
- Historical corrections, deleted rows, and any number of missed runs must be discovered on the next successful sync.
- A last-timestamp watermark is forbidden because it cannot detect historical revisions or deletions.
- Consumer rows, digest rows, and sync-state checkpoint commit atomically per lineage under a lineage-scoped advisory transaction lock.
- Exactly one current artifact per `(dataset_id, exchange, symbol)` is synchronized. Retained older Gold versions are not replicated.

The target execution order is:

```text
Bronze -> Silver -> Gold -> PostgreSQL Gold sync
```

PostgreSQL runs only after Gold succeeds. PostgreSQL failure makes the Medallion invocation non-zero but never rolls back already-published local Gold or its existing NAS mirror. Recovery runs only `gold-sync-postgres` and converges from the last successful per-lineage checkpoint.

## Parallel delivery waves

```text
PR-67 consolidate/plan PostgreSQL sync
   |
   +------------------------+------------------------+
   v                        v                        v
PR-68 contracts         PR-73 role provisioning  PR-74 runtime config
   |
   +----------------+----------------+
   v                v                v
PR-69 delta        PR-70 inventory  PR-71 schema mapper
                                      |
                         PR-68 + PR-71
                                      |
                                      v
                                  PR-72 adapter
                    PR-69 + PR-70 + PR-72
                                      |
                                      v
                                  PR-75 use case
                              PR-74 + PR-75
                                      |
                                      v
                                  PR-76 CLI
                              PR-73 + PR-76
                                      |
                                      v
                                  PR-77 Medallion integration
```

Maximum safe parallelism:

- Wave 1: PR-68, PR-73, PR-74 in parallel after PR-67 merges.
- Wave 2: PR-69, PR-70, PR-71 in parallel after PR-68 merges.
- Wave 3: PR-72 after PR-68 and PR-71.
- Wave 4: PR-75 after PR-69, PR-70, PR-72.
- Wave 5: PR-76 after PR-74 and PR-75.
- Wave 6: PR-77 after PR-73 and PR-76.

---

<!-- PR-67 through PR-77 are completed. Their implementation records are retained in Git history; exact GitHub PR mappings are listed in the Completed PR summary. -->

## PR-78: Comprehensive Repository Audit And Corrective Plan

PR name: `repository-audit-corrective-plan`
Status: In Progress
Updated: 2026-08-24
PR: #185
Git branch: `codex/pr78-postgres-temporal-conformance-plan`
Git status: `planning branch; handoff requires empty git status --short`
Agent lane: Planning/governance; one agent only
Depends on: none
Commit: `docs(PR-78): expand repository audit corrective backlog`
Allowed files: `BACKLOG.md`, `tests/test_cli_lock.py`, `tests/test_postgres_gold_repository.py`

Description:
- R1: Freeze `pg-temporal-v1` and record every verified audit finding above without claiming that the live database or historical lake is already corrupt.
- R2: Replace the earlier temporal-only PR-79/PR-80 sequence with atomic PR-79 through PR-102 and make PR-102 the sole destructive PostgreSQL reconstruction authority; source/lake reload is conditional on PR-99 evidence and occurs only in PR-100.
- R3: Require every corrective ticket to have explicit dependencies, branch, commit, allowed files, one weak-agent lane, and one-to-one numbered requirements/acceptance criteria.
- R4: Record that current branch protection is enabled and requires `pr-quality` plus `pr-coverage-90`, with a repository-wide 90% coverage threshold.
- R5: Repair only the two pre-existing CI blockers exposed by this documentation branch: isolate `tests/test_cli_lock.py` from the newly mandatory external `config.yaml` dependency and Ruff-format `tests/test_postgres_gold_repository.py`; do not change production semantics in this planning PR.
- R6: Define final production completion as PR-100 certified lake/Gold state plus PR-101 live PostgreSQL `PASS` plus PR-102 owned-schema reconstruction and zero-mutation replay.

Acceptance:
- A1 (verifies R1): findings are traceable to current code/settings and no unsupported live-data corruption statement is made.
- A2 (verifies R2): PR-79..PR-102 are contiguous and no earlier ticket authorizes destructive PostgreSQL reconstruction.
- A3 (verifies R3): each work order contains complete Git/ownership/dependency/R-A metadata suitable for a weak agent.
- A4 (verifies R4): the plan records the `pr-coverage-90` required check and its exact 90% threshold.
- A5 (verifies R5): the planning PR's required tests no longer fail solely because a unit fixture lacks `config.yaml`, Ruff reports the audited PostgreSQL test file formatted, and production files are untouched.
- A6 (verifies R6): completion text requires PR-100, PR-101, and PR-102 evidence.

---

## PR-79: Reconcile Backlog Delivery State And Remove Duplicate PostgreSQL Backlog

PR name: `backlog-postgres-source-of-truth`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr79-backlog-postgres-source-of-truth`
Git status: `planned; start only when git status --short is empty`
Agent lane: Documentation/governance; one weak agent
Depends on: PR-78
Commit: `docs(PR-79): restore one PostgreSQL backlog source`
Allowed files: `BACKLOG.md`, `BACKLOG_POSTGRES.md`, `README.md`, `ARCHITECTURE.md`, documentation contract tests only

Description:
- R1: Move detailed PR-67 through PR-77 tickets out of the active section in accordance with backlog policy and record their exact merged GitHub mapping (#174 through #184) in `Completed PR summary`; no merged ticket remains active.
- R2: Delete `BACKLOG_POSTGRES.md` again because PR #174 established `BACKLOG.md` as the single planning source of truth; migrate no still-unique requirement without first placing it in `BACKLOG.md`.
- R3: Remove README/ARCHITECTURE references that treat the duplicate file as authoritative and add a regression check preventing a second active PostgreSQL backlog source from reappearing.

Acceptance:
- A1 (verifies R1): the active section contains no PR-67..PR-77 ticket body and `Completed PR summary` maps each backlog ID to its exact merged GitHub PR #174..#184.
- A2 (verifies R2): repository contains one active backlog source and no requirement is lost by deletion.
- A3 (verifies R3): docs point only to `BACKLOG.md` and a negative fixture fails if a second PostgreSQL backlog source is introduced.

---

## PR-80: Maintain Measured Production Coverage At At Least 90 Percent

PR name: `coverage-90-test-gap`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr80-coverage-90-test-gap`
Git status: `planned; start only when git status --short is empty`
Agent lane: Tests only; one weak agent
Depends on: PR-78
Commit: `test(PR-80): maintain 90 percent coverage`
Allowed files: production-focused test files and test fixtures only; no production code, workflow, or threshold changes

Description:
- R1: Measure current production-code line coverage using the existing repository coverage configuration and record the exact uncovered lines/modules.
- R2: Add deterministic tests for real production behavior until total measured coverage is at least 90.00%; do not exclude production files, use pragma escapes, or lower coverage scope.
- R3: Keep tests offline except for existing explicitly marked network tests and preserve all current behavioral assertions.

Acceptance:
- A1 (verifies R1): before/after coverage evidence names exact totals and the highest-value uncovered branches covered.
- A2 (verifies R2): `coverage report --fail-under=90` passes at 90.00% or higher without production exclusions/threshold edits.
- A3 (verifies R3): the full required offline suite remains deterministic and network-independent.

---

## PR-81: Restore The Canonical 90 Percent Coverage Gate

PR name: `coverage-90-enforcement`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr81-coverage-90-enforcement`
Git status: `planned; start only when git status --short is empty`
Agent lane: CI/governance; one weak agent
Depends on: PR-80
Commit: `ci(PR-81): restore 90 percent coverage enforcement`
Allowed files: `.github/workflows/ci.yml`, `pyproject.toml`, `scripts/github/apply_quality_gates.sh`, coverage/quality contract tests, `README.md`, `ARCHITECTURE.md`

Description:
- R1: Enforce `tool.coverage.report.fail_under=90` and make the `pr-coverage-90` job execute `--cov-fail-under=90`; remove text that incorrectly describes that job as a 95% gate.
- R2: Keep branch protection requiring `pr-quality` and `pr-coverage-90`, verify the exact read-back from GitHub, and ensure the setup script cannot silently configure a contradictory coverage context.
- R3: Add executable contract tests proving 89.99% fails and 90.00% passes and that workflow name, threshold, docs, and branch-protection context agree.

Acceptance:
- A1 (verifies R1): pyproject/workflow both enforce exactly 90 and no required-gate message says 95.
- A2 (verifies R2): GitHub read-back shows the intended required contexts and setup is idempotent.
- A3 (verifies R3): negative/positive 90% threshold fixtures and cross-file consistency tests pass.

---

## PR-82: Run Real PostgreSQL Integration Tests In Required CI

PR name: `real-postgres-ci`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr82-real-postgres-ci`
Git status: `planned; start only when git status --short is empty`
Agent lane: CI/database integration; one weak agent
Depends on: PR-78
Commit: `ci(PR-82): add real PostgreSQL integration gate`
Allowed files: `.github/workflows/ci.yml`, PostgreSQL integration fixtures/tests, test-only config; no production endpoint credentials

Description:
- R1: Provision a disposable supported PostgreSQL service in required PR/main integration jobs with test-only credentials and readiness checks.
- R2: Execute real-psycopg tests for session UTC, DDL/catalog introspection, transactions/rollback, advisory locks, microsecond timestamps, and consumer/sync-state operations; no silent skip when PostgreSQL is unavailable.
- R3: Keep `10.10.1.3:54321` unreachable from required CI and keep provider network tests excluded.

Acceptance:
- A1 (verifies R1): CI fails if its disposable PostgreSQL service cannot become ready.
- A2 (verifies R2): deliberate server-side type/lock/rollback regressions fail real integration tests.
- A3 (verifies R3): CI contains no production database secret/route and makes no provider network request.

---

## PR-83: Enforce Strict UTC On PostgreSQL Read Boundaries

PR name: `postgres-temporal-read-boundary`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr83-postgres-temporal-read-boundary`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL temporal contract; one weak agent
Depends on: PR-78
Commit: `fix(PR-83): enforce PostgreSQL UTC read boundary`
Allowed files: `infra/postgres/gold_repository.py`, `application/postgres_sync/contracts.py`, focused temporal repository tests

Description:
- R1: Make every database-decoded instant reject naive datetimes and any value whose `utcoffset()` is not exactly zero before sync state, digest, or summary objects are constructed.
- R2: Preserve the existing strict write/source contract and normalize nothing silently at the database boundary; upstream normalization must happen before persistence.
- R3: Add UTC, naive, `+01:00`, `+02:00`, and another non-zero-offset fake-driver regression fixtures.

Acceptance:
- A1 (verifies R1): only aware zero-offset UTC database values are accepted.
- A2 (verifies R2): valid current UTC behavior is unchanged and non-UTC values fail before semantic state creation.
- A3 (verifies R3): all listed offset fixtures are deterministic and credential-free.

---

## PR-84: Lock Before Reading And Planning A PostgreSQL Lineage

PR name: `postgres-reconcile-transaction-uow`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr84-postgres-reconcile-transaction-uow`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL concurrency; one weak agent
Depends on: PR-82
Commit: `fix(PR-84): make PostgreSQL reconcile one locked transaction`
Allowed files: `application/postgres_sync/service.py`, `application/postgres_sync/contracts.py`, `infra/postgres/gold_repository.py`, concurrency/integration tests

Description:
- R1: Move lineage advisory lock acquisition before target state/digest reads and hold one transaction through read -> source comparison -> delta plan -> consumer/digest mutations -> verification -> checkpoint -> commit.
- R2: Expose one narrow repository Unit-of-Work API so application code cannot accidentally plan against unlocked state while preserving deterministic planner purity.
- R3: Add two-writer real-PostgreSQL tests proving a second writer cannot use a stale pre-lock plan and that failure rolls back every target mutation/checkpoint.

Acceptance:
- A1 (verifies R1): event traces prove lock precedes every target read used for planning and lasts until commit/rollback.
- A2 (verifies R2): no public reconciliation path can perform unlocked read-plan-write sequencing.
- A3 (verifies R3): concurrent fixtures converge to one correct target with no lost update/stale checkpoint.

---

## PR-85: Verify Actual PostgreSQL Catalog Schema And Versioned Migrations

PR name: `postgres-schema-catalog-contract`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr85-postgres-schema-catalog-contract`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL schema; one weak agent
Depends on: PR-82, PR-83
Commit: `fix(PR-85): verify actual PostgreSQL schema contract`
Allowed files: `application/postgres_sync/schema.py`, `infra/postgres/gold_repository.py`, migration metadata/helpers, PostgreSQL schema tests

Description:
- R1: Introspect `information_schema`/PostgreSQL catalog for every owned consumer and sync table and compare actual column order/name/type/nullability/key/precision with the canonical mapped contract.
- R2: Stop treating `CREATE TABLE IF NOT EXISTS` plus saved state signature as proof of schema compatibility; missing/incompatible objects must produce an explicit migration-required result before DML.
- R3: Introduce source-controlled versioned migration/bootstrap metadata for creating exact current schemas; normal sync never silently mutates incompatible DDL.

Acceptance:
- A1 (verifies R1): compatible real catalog passes; wrong type, precision, nullability, PK, extra/missing column fixtures fail.
- A2 (verifies R2): stale/absent sync state cannot make an incompatible actual table appear compatible.
- A3 (verifies R3): bootstrap is deterministic/idempotent and incompatible existing schemas fail before consumer DML.

---

## PR-86: Separate Administrator DDL From Runtime DML And Harden The Service Role

PR name: `postgres-runtime-least-privilege`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr86-postgres-runtime-least-privilege`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL security/operations; one weak agent
Depends on: PR-85
Commit: `fix(PR-86): restrict PostgreSQL runtime role to DML`
Allowed files: `infra/postgres/provisioning.sql`, `scripts/provision_postgres_sync_role.py`, migration/provisioning helpers, privilege tests, PostgreSQL runbook docs

Description:
- R1: Move schema/table creation and migrations to the explicit administrator provisioning path; runtime `gold-sync-postgres` must not require or execute DDL.
- R2: Make schemas/admin-owned or otherwise non-runtime-owned and revoke runtime `CREATE`; grant `crypto-loader` only the exact USAGE plus table DML/SELECT permissions needed for current consumer, digest, and state operations.
- R3: Add real permission probes proving runtime cannot CREATE/DROP/ALTER unrelated objects and can still perform every normal sync mutation.

Acceptance:
- A1 (verifies R1): runtime trace contains no CREATE/ALTER/DROP and admin bootstrap owns DDL.
- A2 (verifies R2): catalog/grant read-back matches the exact DML-only contract with no super/create-db/create-role/bypass-RLS privileges.
- A3 (verifies R3): allowed DML succeeds and prohibited DDL fails under the actual runtime role.

---

## PR-87: Prove Consumer, Digest Index, And Sync State Are Mutually Consistent

PR name: `postgres-consumer-integrity-verification`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr87-postgres-consumer-integrity-verification`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL integrity; one weak agent
Depends on: PR-84, PR-85
Commit: `fix(PR-87): verify PostgreSQL consumer integrity`
Allowed files: `application/postgres_sync/service.py`, `application/postgres_sync/delta.py`, `infra/postgres/gold_repository.py`, integrity tests

Description:
- R1: Replace count/min/max-only success and unchanged checks with exact logical-key and deterministic row-digest equivalence between canonical source, consumer table, digest table, and saved checkpoint.
- R2: On the unchanged-fingerprint path, detect consumer tampering, missing/extra rows, stale digest rows, and checkpoint inconsistency before returning unchanged.
- R3: Check exact affected-row counts for UPDATE/DELETE operations so a stale plan or unexpected target mutation cannot be silently checkpointed.

Acceptance:
- A1 (verifies R1): clean source/consumer/digest/state fixture passes with zero symmetric differences.
- A2 (verifies R2): same-count/same-bounds payload tampering and digest/state tampering fail closed.
- A3 (verifies R3): injected zero-row or multi-row unexpected mutation prevents checkpoint commit and rolls back.

---

## PR-88: Add Bounded PostgreSQL Connection, Lock, Statement, And Transaction Timeouts

PR name: `postgres-timeout-policy`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr88-postgres-timeout-policy`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL resilience; one weak agent
Depends on: PR-82, PR-84
Commit: `fix(PR-88): bound PostgreSQL waits`
Allowed files: PostgreSQL runtime config/contracts, `infra/postgres/gold_repository.py`, timeout tests, config docs

Description:
- R1: Define explicit positive bounded connect timeout, `lock_timeout`, `statement_timeout`, and `idle_in_transaction_session_timeout` defaults configurable only through validated protected runtime config.
- R2: Apply timeouts at connection/session start before any schema/read/write work and report sanitized timeout categories without credentials/DSNs.
- R3: Add real-PostgreSQL lock-contention/statement timeout tests proving the sync fails within the configured bound and rolls back.

Acceptance:
- A1 (verifies R1): valid boundaries resolve exactly and invalid/non-positive settings fail configuration.
- A2 (verifies R2): session read-back matches configured timeouts and errors expose no secret.
- A3 (verifies R3): held-lock/slow-statement fixtures terminate predictably with no partial commit.

---

## PR-89: Attest Every Gold Output Parquet With Its Own SHA-256

PR name: `gold-output-artifact-sha`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr89-gold-output-artifact-sha`
Git status: `planned; start only when git status --short is empty`
Agent lane: Gold artifact integrity; one weak agent
Depends on: PR-78
Commit: `feat(PR-89): attest Gold output parquet bytes`
Allowed files: Gold manifest/publication/versioning services, Gold manifest tests; no PostgreSQL code

Description:
- R1: Compute SHA-256 from the exact staged/final Gold Parquet bytes and store it in the paired manifest together with row count, schema signature, bounds, input fingerprint, feature version, and build identity.
- R2: Validate manifest hash against final published bytes before publication reports success; unchanged deterministic rebuilds must yield stable content identity when Parquet encoding inputs are unchanged.
- R3: Add corruption tests for modified Parquet bytes, modified manifest hash, and interrupted publication without weakening existing Silver-input fingerprint semantics.

Acceptance:
- A1 (verifies R1): manifest contains one exact output byte hash per Parquet artifact.
- A2 (verifies R2): independently recomputed hash matches after success and mismatch prevents successful publication.
- A3 (verifies R3): all corruption/interruption fixtures fail closed while valid publication remains backward-compatible through an explicit manifest-version transition.

---

## PR-90: Verify Gold Artifact Bytes Before PostgreSQL Inventory Selection

PR name: `postgres-gold-artifact-integrity`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr90-postgres-gold-artifact-integrity`
Git status: `planned; start only when git status --short is empty`
Agent lane: Gold/PostgreSQL boundary; one weak agent
Depends on: PR-89
Commit: `fix(PR-90): verify Gold bytes before PostgreSQL sync`
Allowed files: `application/postgres_sync/inventory.py`, Gold snapshot contracts, integrity tests

Description:
- R1: Require the current Gold selector to recompute and match each Parquet SHA-256 plus manifest dataset/exchange/symbol/build/version/schema/row-count/bounds metadata before producing a sync snapshot.
- R2: Reject missing hash, stale/tampered manifest, changed Parquet bytes, manifest/Parquet schema disagreement, and legacy artifacts that cannot meet the new certified manifest version; do not open PostgreSQL on failure.
- R3: Preserve deterministic exactly-one current lineage selection only among fully certified eligible artifacts.

Acceptance:
- A1 (verifies R1): valid certified artifact produces the same logical snapshot and exact verified output hash.
- A2 (verifies R2): every listed corruption/legacy-negative fixture fails before repository construction.
- A3 (verifies R3): older retained/unregistered/tampered candidates can never outrank a valid current certified artifact.

---

## PR-91: Enforce Medallion Bronze Before Silver Before Gold

PR name: `medallion-layer-order-contract`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr91-medallion-layer-order-contract`
Git status: `planned; start only when git status --short is empty`
Agent lane: Medallion orchestration; one weak agent
Depends on: PR-78
Commit: `fix(PR-91): enforce Medallion dependency order`
Allowed files: `scripts/run_medallion_pipeline.py`, config validation, `config.example.yaml`, pipeline tests, scheduler docs

Description:
- R1: Validate configured enabled layer order against the dependency DAG so Silver cannot run before enabled Bronze and Gold cannot run before enabled Silver; duplicate layers remain forbidden.
- R2: Define explicit semantics for intentionally disabled prerequisite layers: downstream execution is allowed only with a named reuse-existing-inputs mode that runs freshness checks before continuing.
- R3: Add invalid-order/disabled-prerequisite/fresh-existing/stale-existing dry-run and execution tests.

Acceptance:
- A1 (verifies R1): `[bronze,silver,gold]` passes while `[gold,bronze]`, `[silver,bronze,gold]`, and duplicate variants fail before subprocess execution.
- A2 (verifies R2): omitted prerequisite is never silently assumed fresh; explicit reuse mode requires a passing freshness proof.
- A3 (verifies R3): all listed configurations are deterministic and mutate nothing on preflight failure.

---

## PR-92: Scope PostgreSQL Sync To Fresh Eligible Gold From The Medallion Run

PR name: `medallion-postgres-freshness-scope`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr92-medallion-postgres-freshness-scope`
Git status: `planned; start only when git status --short is empty`
Agent lane: Medallion/PostgreSQL integration; one weak agent
Depends on: PR-90, PR-91
Commit: `fix(PR-92): sync only fresh eligible Gold lineages`
Allowed files: pipeline/Gold result contracts, PostgreSQL command/inventory wiring, config, integration tests, docs

Description:
- R1: Pass an explicit successful Gold publication result/eligible-lineage set to the subsequent PostgreSQL sync instead of blindly discovering every supported filesystem lineage after the Gold command.
- R2: Define excluded/stale lineage semantics: a dataset intentionally excluded from the current Gold run must not be silently republished as fresh from an older retained artifact; removal from PostgreSQL requires an explicit serving-deprecation policy rather than accidental filesystem absence.
- R3: Make PostgreSQL sync fail before mutation when a lineage declared fresh by the Gold step cannot be certified by PR-90.

Acceptance:
- A1 (verifies R1): integration trace proves PostgreSQL sees exactly the lineages successfully published/approved by the Gold result.
- A2 (verifies R2): stale excluded Extended-history artifacts are not presented as current-run outputs and no unintended delete occurs.
- A3 (verifies R3): missing/tampered declared-fresh artifact prevents all mutation for that lineage and reports a sanitized failure.

---

## PR-93: Make OHLCV Range Fetches Fail Closed On Retrieval Errors

PR name: `ohlcv-fetch-failure-contract`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr93-ohlcv-fetch-failure-contract`
Git status: `planned; start only when git status --short is empty`
Agent lane: Bronze OHLCV ingestion; one weak agent
Depends on: PR-78
Commit: `fix(PR-93): propagate OHLCV retrieval failures`
Allowed files: `ingestion/spot_ohlcv.py`, OHLCV fetch-service/gapfill adapters as strictly needed, focused tests

Description:
- R1: Stop converting `HttpClientError` from `fetch_candles_range()` into `[]`; transport/provider failure must remain distinguishable from a successful empty response.
- R2: Preserve a typed successful-empty outcome only when the provider request itself succeeds and returns no rows; callers must not mark failed ranges complete.
- R3: Add range/gap-fill tests proving failed retrieval leaves the interval retryable and successful empty behavior is explicit.

Acceptance:
- A1 (verifies R1): injected HTTP failure propagates a typed failure and cannot be counted as zero-row success.
- A2 (verifies R2): legitimate successful empty response remains representable without fabricating candles.
- A3 (verifies R3): retry on the next run includes the failed interval while confirmed-success intervals follow existing gap rules.

---

## PR-94: Define Exact Funding Success-Empty Versus Error Semantics

PR name: `funding-fetch-failure-contract`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr94-funding-fetch-failure-contract`
Git status: `planned; start only when git status --short is empty`
Agent lane: Bronze funding ingestion; one weak agent
Depends on: PR-78
Commit: `fix(PR-94): distinguish funding no-data from failure`
Allowed files: `ingestion/funding.py`, Deribit funding adapter, focused funding tests

Description:
- R1: Stop swallowing generic `HttpClientError`; transport/timeouts/retry exhaustion must fail the requested range instead of returning no rows.
- R2: Replace blanket HTTP-400-as-empty behavior with an explicit tested provider capability/no-data classification based on known Deribit response semantics; malformed/bad-request parameters remain errors.
- R3: Preserve unsupported non-perpetual-market behavior as an explicit local capability decision separate from provider response failure.

Acceptance:
- A1 (verifies R1): transport failure propagates and remains retryable.
- A2 (verifies R2): one known confirmed-no-data provider response maps to empty, while arbitrary HTTP 400/malformed request fails.
- A3 (verifies R3): local unsupported-market fixtures remain deterministic and cannot hide a network request failure.

---

## PR-95: Make Open-Interest Fetches Fail Closed On Retrieval Errors

PR name: `open-interest-fetch-failure-contract`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr95-open-interest-fetch-failure-contract`
Git status: `planned; start only when git status --short is empty`
Agent lane: Bronze open-interest ingestion; one weak agent
Depends on: PR-78
Commit: `fix(PR-95): propagate open-interest retrieval failures`
Allowed files: `ingestion/open_interest.py`, Deribit open-interest adapter, focused tests

Description:
- R1: Stop converting generic `HttpClientError` to an empty result in both range and all-history paths.
- R2: Represent successful empty provider responses explicitly and preserve unsupported-market/exchange capability behavior without conflating it with request failure.
- R3: Add retry/gap tests proving failed intervals are not certified as complete.

Acceptance:
- A1 (verifies R1): transport/timeout/provider errors propagate as typed failures.
- A2 (verifies R2): confirmed empty and unsupported capability cases remain deterministic and distinguishable.
- A3 (verifies R3): failed historical/range windows remain in the next reconciliation plan.

---

## PR-96: Reject Malformed Required Trade Payload Fields

PR name: `trade-payload-validation`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr96-trade-payload-validation`
Git status: `planned; start only when git status --short is empty`
Agent lane: Bronze trade ingestion; one weak agent
Depends on: PR-78
Commit: `fix(PR-96): validate required trade payload fields`
Allowed files: `ingestion/trades.py`, shared Deribit trade parser helpers, focused trade tests

Description:
- R1: Require non-empty trade ID, positive valid timestamp, finite positive price, finite positive quantity under documented provider semantics, and side exactly `buy|sell`; do not synthesize zero/empty/`unknown` values for malformed required fields.
- R2: For options, require parseable instrument name, expiry, positive finite strike, and option type `call|put`; malformed contract identity is a data-quality failure.
- R3: Reject non-dict malformed trade entries instead of silently dropping them from an otherwise successful provider response when that would hide source corruption.

Acceptance:
- A1 (verifies R1): missing/zero/non-finite/unknown fixtures fail and valid perps trades remain unchanged.
- A2 (verifies R2): malformed option-name/strike/type fixtures fail before Bronze persistence.
- A3 (verifies R3): mixed valid+malformed payload cannot silently reduce row count without an explicit provider-supported omission rule.

---

## PR-97: Enforce OHLCV Candle Numerical And Time Semantics

PR name: `ohlcv-semantic-validation`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr97-ohlcv-semantic-validation`
Git status: `planned; start only when git status --short is empty`
Agent lane: Bronze OHLCV validation; one weak agent
Depends on: PR-93
Commit: `fix(PR-97): validate OHLCV candle semantics`
Allowed files: `ingestion/spot_ohlcv.py`, shared candle parser helper if introduced, focused tests

Description:
- R1: Validate input row shape and require finite positive OHLC prices, finite non-negative volume/quote-volume, non-negative integer trade count, and timezone-aware UTC open/close times.
- R2: Require `high >= max(open, close, low)`, `low <= min(open, close, high)`, close time not before open time, and interval-consistent candle duration/timestamp alignment.
- R3: Reject NaN/Inf, negative economics, impossible high/low, malformed row-length, and invalid time-order fixtures before creating `SpotCandle`.

Acceptance:
- A1 (verifies R1): all valid current provider fixtures parse without coercion drift and invalid numeric values fail.
- A2 (verifies R2): impossible OHLC/time relations fail deterministically.
- A3 (verifies R3): every listed negative fixture is covered and no invalid candle reaches Bronze persistence.

---

## PR-98: Sanitize HTTP Diagnostic URLs And Make Retry Tests Deterministic

PR name: `http-diagnostic-redaction`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr98-http-diagnostic-redaction`
Git status: `planned; start only when git status --short is empty`
Agent lane: Shared HTTP boundary; one weak agent
Depends on: PR-78
Commit: `fix(PR-98): sanitize HTTP request diagnostics`
Allowed files: `ingestion/http_client.py`, shared HTTP tests

Description:
- R1: Never include raw query values in `HttpClientError`/`HttpClientHttpError`; diagnostic URL retains only scheme/host/path plus an optional allowlisted non-secret parameter-name list.
- R2: Keep original request values internal to the request only and guarantee chained exceptions/log rendering cannot reveal query tokens/credentials.
- R3: Inject sleeper/jitter source or otherwise make retry-delay tests deterministic while preserving bounded transient retry categories.

Acceptance:
- A1 (verifies R1): a fake `api_key=super-secret` query never appears in exception string/repr/log capture.
- A2 (verifies R2): HTTP/URL/timeout/JSON failures preserve actionable safe path/status category without raw values.
- A3 (verifies R3): exact retry attempts/categories can be tested with zero wall-clock sleep/randomness.

---

## PR-99: Audit Historical Lake Completeness Without Mutating Data

PR name: `historical-lake-completeness-audit`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr99-historical-lake-completeness-audit`
Git status: `planned; start only when git status --short is empty`
Agent lane: Data-quality verification; one weak agent
Depends on: PR-93, PR-94, PR-95, PR-96, PR-97, PR-98
Commit: `test(PR-99): audit historical lake completeness`
Allowed files: read-only audit service/CLI, data-quality report schema, focused tests, sanitized acceptance artifact; no Bronze/Silver/Gold mutation

Description:
- R1: Build a read-only completeness auditor for every configured historical Bronze lineage (including the currently configured BTC/ETH/SOL lineages) that distinguishes observed rows, explicitly confirmed empty intervals, expected provider gaps, and intervals whose acquisition success cannot be proven.
- R2: Check OHLCV minute continuity over configured listing bounds, funding/open-interest provider cadence/capability semantics, and trade minute coverage/confirmed-empty sidecars without inventing rows or market-calendar assumptions unsupported by Deribit.
- R3: Emit a deterministic sanitized interval report with lineage, bounds, gap category, evidence source, and `PASS|RECONCILE_REQUIRED`; no provider payloads/secrets and no data writes.

Acceptance:
- A1 (verifies R1): fixtures distinguish a real observation gap from confirmed-empty and expected provider absence.
- A2 (verifies R2): each audited dataset family applies its own documented cadence/capability rather than one generic minute rule.
- A3 (verifies R3): audit is byte-for-byte read-only and cannot return PASS with an unverified interval.

---

## PR-100: Reconcile Only Failed Or Unverified Historical Source Intervals And Rebuild Dependents

PR name: `targeted-historical-reconciliation`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr100-targeted-historical-reconciliation`
Git status: `planned; start only when git status --short is empty`
Agent lane: Production lake recovery; one agent only
Depends on: PR-89, PR-90, PR-99
Commit: `chore(PR-100): reconcile unverified historical intervals`
Allowed files: guarded reconciliation command/runbook, reconciliation state/report, focused operator tests; no PostgreSQL destructive code

Description:
- R1: Consume a PR-99 report and perform provider reload only for exact failed/unverified intervals; if PR-99 is already PASS, perform no source mutation and record a no-op certification.
- R2: Before source mutation, back up affected Bronze partitions/manifests; refetched rows must pass PR-93..PR-98 contracts and preserve existing valid rows outside target intervals.
- R3: Rebuild only source-change-dependent Silver partitions plus required lookback propagation. Then ensure every PostgreSQL-serving-eligible current Gold lineage is published under the PR-89 certified manifest contract: rebuild affected Gold and re-publish/re-certify any otherwise-current legacy Gold that lacks required output attestation, without refetching source data solely for certification.
- R4: Rerun PR-99, Gold freshness/input-fingerprint checks, and PR-90 certified-artifact inventory validation; emit a sanitized `PASS|FAIL` recovery report and block live PostgreSQL certification while any serving-eligible Gold lineage is uncertified.

Acceptance:
- A1 (verifies R1): target interval set equals PR-99 non-PASS intervals exactly; PASS input performs zero provider/source mutation, while certification-only Gold republish allowed by R3 is not treated as a source reload.
- A2 (verifies R2): before/after evidence proves unaffected Bronze bytes/partitions are unchanged and recovered rows satisfy strict source validation.
- A3 (verifies R3): only dependency-reachable Silver changes occur; every serving-eligible current Gold lineage is PR-89/PR-90 certified even when source reconciliation was a no-op, and source data is never refetched merely to upgrade artifact attestation.
- A4 (verifies R4): final lake audit and PR-90 inventory validation are PASS and any unresolved gap/corruption/uncertified serving lineage prevents downstream PR-101/PR-102.

---

## PR-101: Independently Verify The Live PostgreSQL Serving Plane

PR name: `postgres-live-conformance-verifier`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr101-postgres-live-conformance-verifier`
Git status: `planned; start only when git status --short is empty`
Agent lane: Production verification; one agent only
Depends on: PR-81, PR-82, PR-83, PR-84, PR-85, PR-86, PR-87, PR-88, PR-90, PR-92, PR-100
Commit: `test(PR-101): verify live PostgreSQL serving plane`
Allowed files: read-only/live verifier command, sanitized acceptance report schema, focused offline/live tests; no destructive SQL

Description:
- R1: Preflight exact production endpoint and introspect only `crypto_loader`/`crypto_loader_sync`: tables, columns, PKs, types/precision, ownership, grants, runtime role attributes, session timezone, and configured timeouts.
- R2: For every eligible certified current Gold lineage, compare exact source and consumer row counts, logical-key sets, deterministic row digests, digest-table contents, checkpoint versions/fingerprint/hash/bounds, and require zero symmetric differences.
- R3: Run rollback-only `pg-temporal-v1` microsecond probes around European DST boundaries and DML/DDL permission probes under the runtime role.
- R4: Emit a sanitized `artifacts/acceptance/postgres-live-conformance-v2.json` with `PASS|FAIL`; any schema/role/data/temporal/permission/timeout discrepancy is FAIL. A pre-reconstruction live FAIL is valid verifier output and remains strictly read-only: it blocks production certification but does not itself authorize reconstruction or block merging a correctly implemented verifier whose offline/controlled tests pass.

Acceptance:
- A1 (verifies R1): compatible target passes and deliberate catalog/role/grant/timeout drift fails.
- A2 (verifies R2): same-count payload tamper, missing/extra key, digest mismatch, and stale checkpoint fixtures all fail.
- A3 (verifies R3): exact six-digit UTC instants round-trip and runtime DML succeeds while prohibited DDL fails; probes leave no durable mutation.
- A4 (verifies R4): report contains no password/DSN/raw market payload and cannot be PASS when any check fails; an expected pre-reconstruction FAIL is preserved as evidence rather than rewritten into PASS.

---

## PR-102: Conditionally Reconstruct Owned PostgreSQL Schemas Or Certify No-Op

PR name: `postgres-authoritative-reconstruction`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr102-postgres-authoritative-reconstruction`
Git status: `planned; start only when git status --short is empty`
Agent lane: Production reconstruction; one agent only
Depends on: PR-79, PR-100, PR-101
Commit: `chore(PR-102): certify or reconstruct PostgreSQL serving state`
Allowed files: guarded production reconstruction command/runbook, final acceptance report, focused operator tests; no provider/business-feature code

Description:
- R1: Consume the latest PR-101 report and choose exactly one fail-closed mode: `no-op-certification` when PR-101 already reports PASS, or `reconstruction` only when PR-101 identifies owned-schema/serving-data drift that reconstruction can correct. Other failures such as unresolved configuration, permission, timeout, or unrelated-object drift are hard stops. Destructive SQL is forbidden in `no-op-certification` mode.
- R2: In `reconstruction` mode only, disable the scheduled Medallion publication path, acquire the host pipeline lock plus a dedicated PostgreSQL reconstruction lock, verify the exact endpoint/database, then create and validate a timestamped backup of only `crypto_loader` and `crypto_loader_sync` before the first destructive statement; stop if lock or backup verification fails. In `no-op-certification` mode no destructive maintenance window or backup is required.
- R3: In `reconstruction` mode only, recreate/migrate only the two loader-owned serving/sync schemas through the PR-85/PR-86 administrator path, preserving all unrelated schemas/roles/data exactly and provisioning the runtime role with DML-only privileges; in `no-op-certification` mode catalog/schema bytes and ownership remain untouched.
- R4: In `reconstruction` mode, bootstrap every PR-92 eligible current certified Gold lineage from PR-100 certified Gold using the locked normal PR-84 repository path and write exact consumer rows, digest index, and checkpoints under `pg-temporal-v1`; in `no-op-certification` mode perform no bootstrap or row rewrite.
- R5: Run PR-101 independently after the selected mode and require PASS plus exact source/consumer keys/digests/rows/bounds/versions/output hashes and role/permission/schema/timeout/temporal equality; reconstruction that does not convert the relevant report to PASS fails closed.
- R6: Immediately run unchanged `gold-sync-postgres` and require zero inserts, updates, deletes, digest changes, checkpoint semantic rewrites, or timestamp rewrites. Restore scheduling only if it was disabled for reconstruction and only after all evidence is PASS.
- R7: Commit only a sanitized `artifacts/acceptance/postgres-production-reconstruction-v2.json` recording mode `no-op-certification|reconstruction`; any required backup, source-certification, schema, role, data, temporal, permission, timeout, unrelated-object, or replay mismatch blocks completion.

Acceptance:
- A1 (verifies R1): a pre-existing PR-101 PASS selects no-op certification with zero destructive SQL; only a reconstruction-correctable PR-101 failure selects reconstruction, while unrelated/unresolved failures hard-stop.
- A2 (verifies R2): reconstruction mode proves scheduler/host/DB exclusion and validated restore evidence before the first destructive statement; no-op mode proves neither destructive SQL nor unnecessary backup/rewrite occurred.
- A3 (verifies R3): reconstruction-mode before/after catalog proves only `crypto_loader` and `crypto_loader_sync` were reconstructed and runtime has no DDL privilege; no-op mode proves owned catalog/ownership was unchanged.
- A4 (verifies R4): reconstruction mode fully bootstraps every eligible certified Gold lineage with exact canonical keys/data/digests/checkpoints and `TIMESTAMPTZ(6)` UTC semantics; no-op mode performs zero bootstrap/row rewrite.
- A5 (verifies R5): PR-101 independently returns PASS after either mode with zero symmetric differences and all operational contracts satisfied.
- A6 (verifies R6): immediate replay is a semantic no-op; scheduling is restored only when reconstruction had disabled it and post-mode evidence is PASS.
- A7 (verifies R7): final report is sanitized, names `pg-temporal-v1`, records the selected mode, and any injected mismatch prevents completion.

## Corrected production completion definition

The PostgreSQL serving plane is not production-certified merely because PR-67 through PR-77 are merged or because unit/fake-connection checks pass. Completion requires: PR-79 establishes one accurate backlog source; PR-81 restores the approved 90% quality gate; PR-82 proves real PostgreSQL behavior in CI; PR-83 through PR-98 close the temporal, concurrency, schema, privilege, integrity, orchestration, ingestion-validation, and diagnostic gaps; PR-99 certifies historical lake completeness; PR-100 performs only evidence-driven source reconciliation and produces certified current Gold; PR-101 independently verifies the live target; and PR-102 performs evidence-driven no-op certification when the live target already passes or reconstructs only the owned PostgreSQL schemas when PR-101 proves reconstruction is required, then proves exact equivalence and a zero-mutation replay. Until PR-102 PASS, existing PostgreSQL data must be treated as a serving replica whose full current correctness has not been independently certified.

---

# Completed PR summary

Completed work is intentionally summarized here instead of keeping large finished ticket bodies in the active backlog. Git history and merged pull requests remain the detailed audit trail.

| Completed backlog IDs | Summary |
|---|---|
| PR-01–PR-12 | Established Silver contracts and canonical naming; added volatility-index, realized-volatility, IV/RV, index-price, futures-summary, option-ticker, and option-surface foundations feeding Gold. |
| PR-13–PR-20 | Completed the next Silver/Gold dataset coverage wave, including additional live-origin market-state inputs, explicit deterministic normalization, lineage, and model-ready Gold integration. |
| PR-21–PR-38 | Hardened medallion data handling, live/history boundaries, deterministic inventory/reporting, Gold construction, operational scripts, and test coverage. |
| PR-39–PR-43 | Extracted Silver build registry, shared monthly IO/report kernel, Gold frame-preparation registry, contract-driven command choices, and typed test/command harnesses. |
| PR-44–PR-46 | Added typed Bronze build contracts, explicit runtime adapter, and explicit Bronze workflow-stage result contracts. |
| QC-01–QC-06 | Corrected IV/RV units/horizons, cross-month rolling state, spot/perpetual RV source semantics, quantitative contract metadata, executable documented CLI contracts, and quality-gate alignment. |
| PR-54–PR-61 | Added medallion performance telemetry, Silver/Gold fingerprints, incremental partition planning, shared-source dependency planning, Gold multi-timeframe fan-out, plot decoupling, and incremental freshness orchestration. |
| PR-62–PR-66 | Enforced backlog-number branch/commit conventions, cleaned superseded backlog entries, disabled scheduled historical prediction builds, and deduplicated `gold.live.full.*` artifacts while preserving normal Gold retention. |
| PR-67 | Consolidated the PostgreSQL Gold-sync backlog and serving-plane contract. Merged as GitHub PR #174. |
| PR-68 | Defined PostgreSQL Gold-sync contracts. Merged as GitHub PR #175. |
| PR-69 | Added deterministic Gold row-delta planning. Merged as GitHub PR #178. |
| PR-70 | Added current Gold lineage inventory selection. Merged as GitHub PR #179. |
| PR-71 | Added Gold-schema to PostgreSQL DDL mapping. Merged as GitHub PR #180. |
| PR-72 | Added the PostgreSQL Gold repository adapter. Merged as GitHub PR #181. |
| PR-73 | Added dedicated PostgreSQL service-role provisioning. Merged as GitHub PR #176. |
| PR-74 | Added PostgreSQL sync runtime configuration. Merged as GitHub PR #177. |
| PR-75 | Added Gold-to-PostgreSQL reconciliation orchestration. Merged as GitHub PR #182. |
| PR-76 | Added the PostgreSQL Gold-sync CLI. Merged as GitHub PR #183. |
| PR-77 | Added Medallion PostgreSQL Gold-sync integration. Merged as GitHub PR #184. |

Completed ticket index: `PR-01` through `PR-46`, `QC-01` through `QC-06`, and `PR-54` through `PR-77`.

Latest completed backlog ticket: PR-77, merged as GitHub pull request #184.

## Superseded, not completed

PR-47 through PR-53 were intentionally removed/superseded by later correctness and performance work. They are not counted as completed PRs and must not be resurrected unless a new backlog ticket explicitly redefines the missing work.
