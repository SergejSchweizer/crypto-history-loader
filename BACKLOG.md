# Backlog

This file is the single implementation backlog for `crypto-history-loader`.

Last updated: 2026-08-22

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

Parquet Gold remains the canonical source of truth. The active PostgreSQL stack below adds a rebuildable serving-plane replica after Gold; it does not move canonical ownership away from Parquet.

The PostgreSQL endpoint is exactly `10.10.1.3:54321`. The dedicated runtime LOGIN role is exactly `crypto-history-loader`. The operational password is supplied only from protected runtime configuration/environment and must never be committed, printed, logged, embedded in examples, placed in command-line arguments, or persisted in sync metadata. Administrator credentials are separate from application runtime credentials.

PostgreSQL consumer data lives in schema `crypto_history_gold`. Synchronization state lives separately in schema `crypto_history_sync`. Every registered current Gold dataset maps one-to-one to a consumer table whose name is derived deterministically from the dataset ID by replacing `.` with `_`, for example:

```text
gold.market.regime_features.m1
-> crypto_history_gold.gold_market_regime_features_m1
```

All mapped names must be unique and fit PostgreSQL's 63-byte identifier limit. Collisions or overlong names are hard errors.

Each consumer table mirrors the current Parquet Gold row schema and uses the composite logical key:

```text
(exchange, symbol, timestamp_m1)
```

Every publishable current Gold contract must expose those three fields. Unsupported or ambiguous source types fail before any write. Existing table/source schema-signature mismatch is a migration-required error. Normal sync must never `DROP`, `TRUNCATE`, replace a table, delete-all, or silently mutate a live schema.

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

## PR-67: Consolidate Backlog And Define PostgreSQL Gold Sync Stack

PR name: `postgres-gold-sync-backlog`
Status: In Progress
Updated: 2026-08-22
PR: #174
Git branch: `codex/pr67-postgres-gold-sync-backlog`
Git status: `planning branch; handoff requires empty git status --short`
Agent lane: Planning/governance; one agent only
Depends on: none
Commit: `docs(PR-67): consolidate backlog and PostgreSQL Gold sync plan`
Allowed files: `BACKLOG.md`; delete obsolete `BACKLOG_POSTGRES.md`

Description:
- R1: Make `BACKLOG.md` the only backlog file and remove `BACKLOG_POSTGRES.md`; no active planning information may be lost.
- R2: Keep PR-68 through PR-77 as small, exact, dependency-aware tickets with explicit branch, Git-status, file-ownership, requirement, and acceptance metadata for weak agents.
- R3: Define the serving-plane contract: only registered current Gold is replicated to `10.10.1.3:54321`; Parquet Gold remains authoritative and Bronze/Silver replication is forbidden.
- R4: Define exact runtime role `crypto-history-loader`, consumer schema `crypto_history_gold`, internal schema `crypto_history_sync`, deterministic table naming, and logical row key `(exchange, symbol, timestamp_m1)`.
- R5: Define first-full/later-delta reconciliation using source fingerprints plus complete row digests for changed lineages, including insert/update/delete, missed-run catch-up, and historical corrections; timestamp watermarks are forbidden.
- R6: Define atomic lineage transactions, advisory locks, schema-mismatch failure behavior, and forbidden destructive SQL during normal sync.
- R7: Define Medallion ordering `Bronze -> Silver -> Gold -> PostgreSQL`, Gold-success gating, non-rollback of already-published Gold, and sync-only retry semantics.
- R8: Move completed backlog history out of the active section and summarize completed work only at the end of this file; explicitly distinguish superseded PR-47 through PR-53 from completed work.

Acceptance:
- A1 (verifies R1): repository root contains `BACKLOG.md` and no `BACKLOG_POSTGRES.md`; repository contains no second backlog source of truth.
- A2 (verifies R2): PR-68 through PR-77 each appear exactly once in the active section and each contains `Git branch`, `Git status`, `Allowed files`, matching R/A IDs, and exact dependencies.
- A3 (verifies R3): endpoint and Gold-only serving-plane rules are explicit and PostgreSQL is never described as canonical storage.
- A4 (verifies R4): exact role/schema names, deterministic table mapping, and the exact three-column logical key are explicit; no operational password literal is present.
- A5 (verifies R5): bootstrap, no-op, accumulated delta, missed-run, update, delete, and historical-revision semantics are explicit and no last-timestamp watermark is permitted.
- A6 (verifies R6): atomic lineage transaction, advisory lock, migration-required schema mismatch, and forbidden destructive SQL are explicit.
- A7 (verifies R7): post-Gold ordering, failure propagation, local-Gold non-rollback, and retry-only-sync semantics are explicit.
- A8 (verifies R8): completed PRs are represented in the final summary section and PR-47 through PR-53 are marked superseded/not completed.

---

## PR-68: PostgreSQL Gold Sync Contracts

PR name: `postgres-gold-sync-contracts`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr68-postgres-gold-sync-contracts`
Git status: `planned; start only when git status --short is empty`
Agent lane: Foundation; one weak agent
Depends on: PR-67
Commit: `feat(PR-68): define PostgreSQL Gold sync contracts`
Allowed files: `application/postgres_sync/__init__.py`, `application/postgres_sync/contracts.py`, `tests/test_postgres_sync_contracts.py`

Description:
- R1: Add immutable typed contracts `GoldLineage`, `GoldSourceSnapshot`, `GoldSyncState`, `GoldRowDigest`, `GoldDeltaPlan`, and `GoldSyncResult`; counts include inserted/updated/deleted/unchanged.
- R2: Define exact constants for host `10.10.1.3`, port `54321`, role `crypto-history-loader`, consumer schema `crypto_history_gold`, sync schema `crypto_history_sync`, state table `gold_sync_state`, and digest table `gold_row_hashes`.
- R3: Define deterministic dataset-ID -> consumer-table mapping by replacing `.` with `_`; reject invalid characters, collisions, names longer than 63 bytes, or mapping outside `crypto_history_gold`.
- R4: Define publishable Gold row key exactly as `(exchange, symbol, timestamp_m1)` and add a contract check over every current `supported_gold_build_ids()` dataset; no current Gold contract may be silently excluded.
- R5: Define application-layer `GoldSyncRepository` Protocol for reading sync state/digests/target summary, validating/creating consumer storage, and applying one lineage delta atomically; `application/` must not import psycopg or `infra`.
- R6: Define source compatibility fields: dataset ID, exchange, symbol, source artifact path, source fingerprint, schema signature, row count, timestamp min/max, and stable source version/build identity when present.
- R7: Keep application/domain contracts credential-free; password, administrator credentials, raw DSN, connection object, and cursor must not appear in dataclasses/results/errors.

Acceptance:
- A1 (verifies R1): tests instantiate all six immutable contracts and verify exact fields/count semantics.
- A2 (verifies R2): tests assert every endpoint/role/schema/internal-table constant exactly.
- A3 (verifies R3): all current Gold dataset IDs map uniquely/deterministically; invalid/colliding/overlong fixtures fail before SQL generation.
- A4 (verifies R4): registry test iterates every current Gold build ID and fails if any cannot provide `exchange`, `symbol`, and `timestamp_m1`.
- A5 (verifies R5): fake repository satisfies the Protocol and import-boundary tests find no psycopg/infra import in `application/postgres_sync`.
- A6 (verifies R6): source snapshot fixtures serialize all compatibility fields deterministically.
- A7 (verifies R7): contract introspection proves no credential/DSN/connection/cursor field exists.

---

## PR-69: Deterministic Gold Row Delta Planner

PR name: `postgres-gold-delta-planner`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr69-postgres-gold-delta-planner`
Git status: `planned; start only when git status --short is empty`
Agent lane: Pure logic; one weak agent
Depends on: PR-68
Commit: `feat(PR-69): compute deterministic PostgreSQL Gold deltas`
Allowed files: `application/postgres_sync/delta.py`, `tests/test_postgres_sync_delta.py`

Description:
- R1: Implement deterministic SHA-256 row hashing over exact source column order with type tags/null markers, UTC epoch-microsecond datetime encoding, canonical finite floating-point encoding, and `-0.0 -> 0.0`; reject NaN/infinity where deterministic representation is not allowed.
- R2: Implement pure complete-state comparison keyed by `(exchange, symbol, timestamp_m1)` producing disjoint, deterministically sorted insert/update/delete/unchanged key sets.
- R3: Bootstrap rule: empty sync state plus empty digest state classifies every current source row as insert and no row as update/delete.
- R4: Reject ambiguous bootstrap when authoritative sync state is absent but lineage digest state is non-empty.
- R5: Classify identical key/hash as unchanged, changed hash as update, source-only key as insert, and target-only key as delete.
- R6: Do not use a timestamp watermark or previous-Gold-build dependency; arbitrarily old corrections and additions accumulated over multiple missed weeks must be discoverable whenever the source fingerprint changes.
- R7: Keep this module side-effect free: no filesystem, Polars scan, PostgreSQL, logging, wall-clock, or environment access.

Acceptance:
- A1 (verifies R1): equal canonical rows hash identically; one value change changes digest; null/value differs; `-0.0` equals `0.0`; invalid non-finite fixtures fail deterministically.
- A2 (verifies R2): mixed fixtures yield exact mutually exclusive ordered key sets with no key in two sets.
- A3 (verifies R3): N source rows and empty target state yield exactly N inserts.
- A4 (verifies R4): digest rows without sync state fail before a plan is returned.
- A5 (verifies R5): dedicated fixtures independently prove insert/update/delete/unchanged classification.
- A6 (verifies R6): tests detect a historical correction and three missed-run additions without a last-sync timestamp.
- A7 (verifies R7): import/monkeypatch tests prove no external side effect and repeated calls serialize identically.

---

## PR-70: Current Gold Lineage Inventory Selector

PR name: `postgres-current-gold-inventory`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr70-postgres-current-gold-inventory`
Git status: `planned; start only when git status --short is empty`
Agent lane: Gold discovery; one weak agent
Depends on: PR-68
Commit: `feat(PR-70): select current Gold lineages for PostgreSQL sync`
Allowed files: `application/postgres_sync/inventory.py`, `tests/test_postgres_sync_inventory.py`

Description:
- R1: Build a read-only inventory over `lake/gold` using existing Gold contracts/manifests/discovery semantics and return exactly one current source snapshot per `(dataset_id, exchange, symbol)` lineage.
- R2: Include every current materialized registered Gold dataset regardless of timeframe/family; Bronze/Silver and unregistered files are never publishable.
- R3: Select current artifacts by repository version/manifest semantics, not mtime/ctime or arbitrary lexical recency; retained older Gold versions must not appear.
- R4: Require valid source fingerprint, schema signature, row count, and timestamp min/max from validated manifest or deterministic existing metadata; missing/inconsistent metadata fails the lineage rather than guessing.
- R5: Return lineages in stable `(dataset_id, exchange, symbol)` order and reject duplicate current candidates.
- R6: Keep selector read-only: no Gold build, NAS mirror, retention/pruning, manifest mutation, or PostgreSQL connection.
- R7: Add fixtures for one current plus retained old versions, multiple datasets/symbols/timeframes, unregistered artifacts, duplicate-current ambiguity, and missing/corrupt metadata.

Acceptance:
- A1 (verifies R1): fixtures produce exactly one snapshot for every expected current lineage.
- A2 (verifies R2): every materialized registered Gold fixture is selected and Bronze/Silver/unregistered fixtures are absent.
- A3 (verifies R3): changing file mtimes does not alter selection and retained old versions are never selected.
- A4 (verifies R4): missing/corrupt fingerprint/schema/count/bounds fails deterministically with no guessed values.
- A5 (verifies R5): output ordering is stable and duplicate current candidates fail.
- A6 (verifies R6): spies prove no build/mirror/prune/write/DB call occurs.
- A7 (verifies R7): all listed inventory scenarios pass offline under `tmp_path`.

---

## PR-71: Gold Schema To PostgreSQL DDL Mapper

PR name: `postgres-gold-schema-mapper`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr71-postgres-gold-schema-mapper`
Git status: `planned; start only when git status --short is empty`
Agent lane: Pure schema logic; one weak agent
Depends on: PR-68
Commit: `feat(PR-71): map Gold schemas to PostgreSQL DDL`
Allowed files: `application/postgres_sync/schema.py`, `tests/test_postgres_sync_schema.py`

Description:
- R1: Generate deterministic quoted PostgreSQL DDL for one table in `crypto_history_gold` using source column order; primary key exactly `(exchange, symbol, timestamp_m1)`.
- R2: Map datetime -> `TIMESTAMPTZ(6)` UTC, date -> `DATE`, string/categorical/enum -> `TEXT`, bool -> `BOOLEAN`, signed integer -> `BIGINT`, UInt64 -> `NUMERIC(20,0)`, float -> `DOUBLE PRECISION`, decimal -> exact `NUMERIC`, binary -> `BYTEA`, list/struct-like -> `JSONB`; reject unknown/ambiguous dtypes.
- R3: Quote every schema/table/column identifier safely; dataset IDs and source column names are never interpolated unquoted.
- R4: Generate deterministic schema signature from ordered `(column_name, normalized_source_type, postgres_type, nullable)` entries plus primary-key contract.
- R5: Require `exchange`, `symbol`, `timestamp_m1` to exist and be non-nullable at the logical-key boundary; do not invent surrogate IDs or row-position keys.
- R6: Normal-sync DDL may create missing schemas/tables/indexes idempotently but must not emit `DROP`, `TRUNCATE`, table replacement, or destructive automatic `ALTER`.
- R7: Test mapper against every current Gold schema fixture constructible from repository tests, including nested fields mapped to JSONB.

Acceptance:
- A1 (verifies R1): generated DDL has exact qualified table name, source column order, and composite primary key.
- A2 (verifies R2): one fixture per listed dtype produces exact PostgreSQL type and unknown dtype fails.
- A3 (verifies R3): adversarial identifiers stay quoted and cannot inject SQL statements.
- A4 (verifies R4): equal ordered schemas yield equal signatures and any column/type/nullability/key change changes signature.
- A5 (verifies R5): missing or nullable logical-key fields fail before DDL is returned.
- A6 (verifies R6): generated SQL contains no destructive operation and has no automatic destructive migration path.
- A7 (verifies R7): mapper coverage passes for all current Gold contract schema fixtures.

---

## PR-72: PostgreSQL Gold Repository Adapter

PR name: `postgres-gold-repository-adapter`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr72-postgres-gold-repository-adapter`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL persistence; one weak agent
Depends on: PR-68, PR-71
Commit: `feat(PR-72): implement PostgreSQL Gold repository adapter`
Allowed files: `infra/postgres/__init__.py`, `infra/postgres/gold_repository.py`, `pyproject.toml`, `uv.lock`, `tests/test_postgres_gold_repository.py`

Description:
- R1: Add `psycopg` as the only new PostgreSQL runtime client; no SQLAlchemy, ORM, or second PostgreSQL driver.
- R2: Create connections only from injected `PGHOST/PGPORT/PGUSER/PGDATABASE/PGPASSWORD`; require exact host `10.10.1.3`, port `54321`, user `crypto-history-loader`, and force session timezone UTC before data operations.
- R3: Idempotently create/validate consumer tables from PR-71 DDL plus internal `crypto_history_sync.gold_sync_state` and `crypto_history_sync.gold_row_hashes`; internal tables are not consumer Gold tables.
- R4: Read per-lineage sync state, target summary `(count,min_timestamp,max_timestamp)`, and complete `(exchange,symbol,timestamp_m1,row_sha256)` digest state without fetching unchanged feature payloads.
- R5: Implement one-lineage `apply_delta` under deterministic lineage-scoped `pg_advisory_xact_lock`: consumer mutations -> digest mutations -> sync-state write -> summary verification -> commit.
- R6: Roll back consumer rows, digest rows, and sync state together on SQL/verification error; retry against same source converges without duplicates.
- R7: Bootstrap may insert complete validated lineage; non-bootstrap writes exactly supplied delta and never `TRUNCATE`, `DROP`, delete-all, table swap, or full-table replacement.
- R8: Detect source/existing consumer schema-signature mismatch before row mutation and raise sanitized migration-required error; never auto-alter a live table destructively.
- R9: Redact runtime/admin secrets and credential-bearing DSNs from repr/errors/logs; persist no credentials in internal tables.
- R10: Add deterministic adapter tests with connection/cursor fakes for endpoint, timezone, DDL validation, lock/order, mixed delta counts, rollback, retry, schema mismatch, forbidden SQL, and redaction.

Acceptance:
- A1 (verifies R1): dependency inspection finds psycopg and no newly added ORM/second driver.
- A2 (verifies R2): connection spy observes exact host/port/user, injected database/password, and UTC session timezone; wrong endpoint/user fails before data SQL.
- A3 (verifies R3): DDL tests create/validate exact consumer/internal identities and keep sync metadata out of consumer tables.
- A4 (verifies R4): query trace reads only state, summary, and key/hash digests for comparison.
- A5 (verifies R5): trace order is advisory lock -> consumer mutations -> digest mutations -> state write -> summary verification -> commit.
- A6 (verifies R6): injected failure leaves prior committed consumer/digest/state unchanged and retry succeeds once.
- A7 (verifies R7): bootstrap N rows produces N inserts; later `2 insert + 1 update + 1 delete` executes exactly those mutations and no full reload.
- A8 (verifies R8): schema mismatch causes zero consumer-row mutations and returns migration-required category.
- A9 (verifies R9): fake secrets/full DSN never appear in diagnostics or persisted parameters.
- A10 (verifies R10): all listed adapter cases pass offline without a live PostgreSQL server.

---

## PR-73: Provision Dedicated PostgreSQL Service Role

PR name: `postgres-service-role-provisioning`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr73-postgres-service-role-provisioning`
Git status: `planned; start only when git status --short is empty`
Agent lane: PostgreSQL operations; one weak agent
Depends on: PR-67
Commit: `feat(PR-73): add PostgreSQL service-role provisioning`
Allowed files: `scripts/provision_postgres_sync_role.py`, `infra/postgres/provisioning.sql`, `tests/test_postgres_role_provisioning.py`

Description:
- R1: Add idempotent operator provisioning targeting exactly `10.10.1.3:54321` that creates/validates LOGIN role exactly `crypto-history-loader`; static SQL must quote the hyphenated role name.
- R2: Receive administrator username/password and application-role password only from protected environment/runtime input; no secret in tracked files, process-list-visible arguments, examples, logs, or exception text.
- R3: Enforce role attributes exactly `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`.
- R4: Create/validate schemas `crypto_history_gold` and `crypto_history_sync` owned by or granting only sufficient `USAGE/CREATE` rights to `crypto-history-loader`; no rights on other repository schemas.
- R5: Keep administrator credentials separate from application runtime credentials and never export admin credentials into Medallion/CLI runtime configuration.
- R6: Make repeated provisioning idempotent; incompatible pre-existing role attributes/schema ownership fail safely instead of broadening privileges silently.
- R7: Require `PGDATABASE` as protected operator input; do not guess or hard-code a database name.
- R8: Add offline command/SQL contract tests for endpoint/role/attributes/schemas, secret placeholders, idempotency, quoted role identity, and absence of literal credentials.

Acceptance:
- A1 (verifies R1): command/SQL fixtures resolve exact endpoint and exact role `crypto-history-loader`.
- A2 (verifies R2): tracked content contains only environment references/test placeholders and process commands never embed a password argument.
- A3 (verifies R3): SQL contract asserts all six exact least-privilege attributes.
- A4 (verifies R4): only `crypto_history_gold` and `crypto_history_sync` rights are provisioned for the application role.
- A5 (verifies R5): admin inputs are distinct and absent from application-runtime output/config objects.
- A6 (verifies R6): second-run fixture is no-op/validation while incompatible state fails without privilege escalation.
- A7 (verifies R7): missing/blank database input fails before connection.
- A8 (verifies R8): all listed provisioning tests pass offline and repository scans find no real password literal.

---

## PR-74: PostgreSQL Sync Runtime Configuration

PR name: `postgres-sync-runtime-config`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr74-postgres-sync-runtime-config`
Git status: `planned; start only when git status --short is empty`
Agent lane: Runtime configuration; one weak agent
Depends on: PR-67
Commit: `feat(PR-74): add PostgreSQL sync runtime configuration`
Allowed files: `application/postgres_sync/config.py`, `scripts/runtime_config.py`, `tests/test_postgres_sync_config.py`, `tests/test_runtime_config.py`

Description:
- R1: Define typed runtime configuration resolving exact `PGHOST=10.10.1.3`, `PGPORT=54321`, `PGUSER=crypto-history-loader`, required non-empty `PGDATABASE`, and protected `PGPASSWORD` from environment or already-ignored runtime config; tracked source/docs contain no password value.
- R2: Preserve existing logging/runtime behavior in `scripts/runtime_config.py`; PostgreSQL support is additive and must not change log-path resolution.
- R3: Explicit environment values override ignored runtime-config PostgreSQL values; partial mixed sources are allowed only when final five-variable set is complete and exact.
- R4: Validate endpoint/user/database/password before adapter construction; wrong host/port/user or blank database/password fails without opening a connection.
- R5: Redact password and credential-bearing DSN from validation errors, dataclass repr, debug logs, and JSON result/error payloads.
- R6: Provide method returning the five standard `PG*` values for subprocess/CLI composition without admin provisioning credentials.
- R7: Add deterministic tests for environment-only, ignored-config-only, override precedence, invalid identity, missing values, shell-special fake passwords, and redaction.

Acceptance:
- A1 (verifies R1): valid fixture resolves exact host/port/user plus injected database/password and no tracked fixture contains operational secret.
- A2 (verifies R2): pre-existing runtime/log configuration tests remain passing without behavior change.
- A3 (verifies R3): precedence fixtures produce exact final five-variable mapping.
- A4 (verifies R4): each invalid/missing required field fails before mocked connection factory is called.
- A5 (verifies R5): fake password/full DSN is absent from repr, errors, logs, and serialized payloads.
- A6 (verifies R6): exported runtime mapping contains only `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD` and no admin variables.
- A7 (verifies R7): all listed config cases pass offline.

---

## PR-75: Gold To PostgreSQL Reconciliation Use Case

PR name: `postgres-gold-sync-use-case`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr75-postgres-gold-sync-use-case`
Git status: `planned; start only when git status --short is empty`
Agent lane: Application orchestration; one weak agent
Depends on: PR-69, PR-70, PR-72
Commit: `feat(PR-75): reconcile current Gold into PostgreSQL`
Allowed files: `application/postgres_sync/service.py`, `tests/test_postgres_sync_service.py`

Description:
- R1: Implement deterministic application service receiving PR-70 current-lineage inventory and `GoldSyncRepository`; it must not invoke Bronze/Silver/Gold build, mirror, retention, provider, or provisioning operations.
- R2: Process lineages sequentially in stable `(dataset_id, exchange, symbol)` order so restart behavior/logging are deterministic and DB load is bounded.
- R3: On absent sync state plus empty digest state, load complete current source lineage, compute digests, and submit every row as bootstrap insert.
- R4: If synchronized source fingerprint/schema/count/bounds equal current snapshot, perform zero consumer/digest row mutations and verify target summary.
- R5: If source fingerprint changed, read complete current lineage, compute complete current digests, compare through PR-69, and submit only planned insert/update/delete payloads.
- R6: Preserve accumulated-delta semantics across any number of missed runs and historical corrections; timestamp-watermark optimization is forbidden.
- R7: After repository commit, require final target row count/min/max to equal source snapshot before reporting synchronized; verification failure must not advance authoritative sync checkpoint.
- R8: Stop on first lineage failure, return non-success with failing lineage/category, keep already committed earlier lineages valid, leave later lineages untouched, and make retry resume idempotently.
- R9: Return aggregate/per-lineage inserted/updated/deleted/unchanged counts plus source identities with no credential fields.
- R10: Add offline fake-inventory/repository/source-reader tests for bootstrap, unchanged fast path, mixed delta, historical update, delete, three missed runs, schema mismatch, verification failure, partial-progress retry, and empty inventory.

Acceptance:
- A1 (verifies R1): spies prove only inventory/source-read/repository interfaces are called.
- A2 (verifies R2): shuffled input produces stable sorted processing order and no concurrent DB writes.
- A3 (verifies R3): empty target plus N rows yields exactly N bootstrap inserts.
- A4 (verifies R4): unchanged fingerprint/state yields zero consumer/digest mutations while target summary is checked.
- A5 (verifies R5): fixture `2 new + 1 changed + 1 stale + 100 unchanged` submits exactly 2 inserts, 1 update, 1 delete and no unchanged payloads.
- A6 (verifies R6): old correction and three missed-week additions reconcile fully in one later run.
- A7 (verifies R7): summary mismatch fails and cannot advance sync state to new source fingerprint.
- A8 (verifies R8): failure on lineage 2 preserves committed lineage 1, leaves lineage 3 untouched, and retry converges without duplicates.
- A9 (verifies R9): aggregate/per-lineage result fields/counts are exact and credential-free.
- A10 (verifies R10): every listed use-case scenario passes offline.

---

## PR-76: PostgreSQL Gold Sync CLI

PR name: `postgres-gold-sync-cli`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr76-postgres-gold-sync-cli`
Git status: `planned; start only when git status --short is empty`
Agent lane: CLI/composition; one weak agent
Depends on: PR-74, PR-75
Commit: `feat(PR-76): expose Gold PostgreSQL sync CLI`
Allowed files: `api/commands/postgres.py`, `api/cli.py`, `tests/test_postgres_sync_command.py`, `tests/test_cli.py`

Description:
- R1: Add exactly one operational command `gold-sync-postgres` with `--gold-root`, `--debug`, and existing global `--config`; default Gold root `lake/gold`.
- R2: Compose PR-70 inventory, source reader, PR-75 service, and PR-72 repository only after PR-74 validation succeeds; missing/invalid config creates no DB connection.
- R3: Command is read-only toward Bronze/Silver/local Gold and must not build, mirror, prune, reconcile source providers, or provision roles/schemas with admin credentials.
- R4: Use existing logging utilities/shared configured `.logs` root with module logger `postgres-gold-sync`; never print password/DSN and do not create a separate logging subsystem.
- R5: Emit deterministic success JSON/log fields: command, status, lineages processed, inserted, updated, deleted, unchanged, and existing elapsed metadata convention.
- R6: Return stable non-zero exit categories for configuration, current-Gold inventory, compatibility/schema, PostgreSQL, and verification errors; success/no-op returns zero.
- R7: Provide manual retry path running only `gold-sync-postgres`; PostgreSQL outage after Gold publication must not require rebuilding Bronze/Silver/Gold.
- R8: Add parser/composition/no-side-effect/result/redaction/error tests with deterministic fakes and no network.

Acceptance:
- A1 (verifies R1): parser exposes exactly `gold-sync-postgres`, expected arguments, and `--debug`.
- A2 (verifies R2): composition spy sees no DB factory call for invalid config and exact validated dependencies for valid fixture.
- A3 (verifies R3): spies prove no build/mirror/prune/provider/provisioning call is reachable.
- A4 (verifies R4): command logs through existing utilities and fake secrets/full DSN are absent from output.
- A5 (verifies R5): success/no-op fixtures emit exact aggregate fields/counts.
- A6 (verifies R6): each failure class returns deterministic non-zero status and cannot claim success.
- A7 (verifies R7): retry test invokes only sync composition and converges against prior checkpoints.
- A8 (verifies R8): all listed command tests pass offline.

---

## PR-77: Medallion PostgreSQL Gold Sync Integration

PR name: `medallion-postgres-gold-sync`
Status: Planned
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr77-medallion-postgres-gold-sync`
Git status: `planned; start only when git status --short is empty`
Agent lane: Final integration/operations; one weak agent
Depends on: PR-73, PR-76
Commit: `feat(PR-77): sync Gold to PostgreSQL after Medallion Gold`
Allowed files: `scripts/run_medallion_pipeline.py`, `README.md`, `ARCHITECTURE.md`, `tests/test_run_medallion_pipeline.py`, `tests/test_postgres_sync_medallion.py`

Description:
- R1: Append one `postgres-gold-sync` pipeline step immediately after configured successful Gold step; invoke existing Python entrypoint plus `gold-sync-postgres` using same config and `lake/gold`.
- R2: Gate PostgreSQL on Gold success: Bronze/Silver/Gold failure prevents sync; successful Gold always attempts sync in the same Medallion invocation.
- R3: PostgreSQL failure makes Medallion non-zero and logs failed active step, but already published local Gold and existing NAS mirror remain untouched/authoritative.
- R4: Preserve existing Sunday cron schedule; no second PostgreSQL cron job. Every manual Medallion invocation receives same post-Gold behavior.
- R5: Use PR-74 protected runtime configuration only; no PostgreSQL/admin password in pipeline script, README, ARCHITECTURE, tests, command line, or logged plan.
- R6: Document PostgreSQL as rebuildable Gold-only serving replica, exact endpoint/user/schema names, first-full/later-delta behavior, current-version-only selection, manual retry, and schema-migration failure semantics.
- R7: Add deterministic Medallion tests for exact order `bronze -> silver -> gold -> postgres-gold-sync`, Gold-failure gating, PostgreSQL-failure propagation, no-op, retry without rebuilding Gold, and dry-run inclusion.
- R8: Run complete configured quality suite (Ruff lint/format, Mypy, Pyright, ty, import-linter, config validation, docs inventory validation, Pytest, coverage) and record any environment-only check that cannot run.

Acceptance:
- A1 (verifies R1): generated pipeline contains one and only one PostgreSQL sync directly after Gold.
- A2 (verifies R2): injected Bronze/Silver/Gold failures produce zero PostgreSQL calls; Gold success produces exactly one sync call.
- A3 (verifies R3): injected PostgreSQL failure returns non-zero while local Gold and NAS mirror are not reverted/deleted.
- A4 (verifies R4): no second scheduled PostgreSQL cron is introduced and docs state existing Sunday Medallion run owns scheduling.
- A5 (verifies R5): touched-file scans contain no operational/admin credential literal and dry-run output has no password/DSN.
- A6 (verifies R6): README/ARCHITECTURE contain all listed serving-plane, delta, current-version, retry, and migration rules without claiming PostgreSQL is canonical.
- A7 (verifies R7): all ordering/failure/no-op/retry/dry-run tests pass offline.
- A8 (verifies R8): final PR records full quality-gate result and preserves or improves repository coverage.

## PostgreSQL stack completion definition

The stack is complete only when:

- Gold Parquet remains canonical and PostgreSQL contains no Bronze/Silver serving tables from this stack.
- Exact dedicated runtime role `crypto-history-loader` exists with least privilege on only `crypto_history_gold` and `crypto_history_sync` in the configured database.
- Every current materialized registered Gold lineage has exactly one current PostgreSQL representation; retained old Gold versions are not duplicated.
- First sync performs complete lineage bootstrap; later runs write only accumulated INSERT/UPDATE/DELETE deltas.
- Historical corrections and deletes are detected without a timestamp watermark.
- Consumer rows, row digests, and sync checkpoint are atomic per lineage and retry-safe.
- Unchanged source fingerprint performs no consumer-row rewrite.
- Every successful Medallion Gold step is followed by PostgreSQL sync, including the existing Sunday run.
- PostgreSQL outage never invalidates or rolls back already-published local Gold; manual `gold-sync-postgres` retry is sufficient after connectivity returns.
- No operational password, administrator credential, or credential-bearing DSN exists in Git history, tracked files, logs, persisted sync metadata, or test snapshots.

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

Completed ticket index: `PR-01` through `PR-46`, `QC-01` through `QC-06`, and `PR-54` through `PR-66`.

Latest completed backlog ticket: PR-66, merged as GitHub pull request #173 on 2026-08-04.

## Superseded, not completed

PR-47 through PR-53 were intentionally removed/superseded by later correctness and performance work. They are not counted as completed PRs and must not be resurrected unless a new backlog ticket explicitly redefines the missing work.
