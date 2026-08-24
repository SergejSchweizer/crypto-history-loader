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
- R4: Define exact runtime role `crypto-loader`, consumer schema `crypto_loader`, internal schema `crypto_loader_sync`, deterministic table naming, and logical row key `(exchange, symbol, timestamp_m1)`.
- R5: Define first-full/later-delta reconciliation using source fingerprints plus complete row digests for changed lineages, including insert/update/delete, missed-run catch-up, and historical corrections; timestamp watermarks are forbidden.
- R6: Define atomic lineage transactions, advisory locks, schema-mismatch failure behavior, and forbidden destructive SQL during normal sync.
- R7: Define Medallion ordering `Bronze -> Silver -> Gold -> PostgreSQL`, Gold-success gating, non-rollback of already-published Gold, and sync-only retry semantics.
- R8: Move completed backlog history out of the active section and summarize completed work only at the end of this file; explicitly distinguish superseded PR-47 through PR-53 from completed work.
- R9: Make the timestamp boundary exactly type-compatible with `regime-loader` and `xetra-loader`: canonical source `timestamp_m1` is `Datetime(us, UTC)`, all PostgreSQL timestamp/datetime columns are `TIMESTAMPTZ(6)`, and every sync session is UTC.

Acceptance:
- A1 (verifies R1): repository root contains `BACKLOG.md` and no `BACKLOG_POSTGRES.md`; repository contains no second backlog source of truth.
- A2 (verifies R2): PR-68 through PR-77 each appear exactly once in the active section and each contains `Git branch`, `Git status`, `Allowed files`, matching R/A IDs, and exact dependencies.
- A3 (verifies R3): endpoint and Gold-only serving-plane rules are explicit and PostgreSQL is never described as canonical storage.
- A4 (verifies R4): exact role/schema names, deterministic table mapping, and the exact three-column logical key are explicit; no operational password literal is present.
- A5 (verifies R5): bootstrap, no-op, accumulated delta, missed-run, update, delete, and historical-revision semantics are explicit and no last-timestamp watermark is permitted.
- A6 (verifies R6): atomic lineage transaction, advisory lock, migration-required schema mismatch, and forbidden destructive SQL are explicit.
- A7 (verifies R7): post-Gold ordering, failure propagation, local-Gold non-rollback, and retry-only-sync semantics are explicit.
- A8 (verifies R8): completed PRs are represented in the final summary section and PR-47 through PR-53 are marked superseded/not completed.
- A9 (verifies R9): backlog explicitly requires `Datetime(time_unit="us", time_zone="UTC")` -> `TIMESTAMPTZ(6)`, UTC PostgreSQL sessions, exact microsecond round-trip, and forbids timezone-naive or alternate-precision timestamp storage.

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
- R2: Define exact constants for host `10.10.1.3`, port `54321`, role `crypto-loader`, consumer schema `crypto_loader`, sync schema `crypto_loader_sync`, state table `gold_sync_state`, and digest table `gold_row_hashes`.
- R3: Define deterministic dataset-ID -> consumer-table mapping by replacing `.` with `_`; reject invalid characters, collisions, names longer than 63 bytes, or mapping outside `crypto_loader`.
- R4: Define publishable Gold row key exactly as `(exchange, symbol, timestamp_m1)` and require `timestamp_m1` canonical source type exactly `Polars Datetime(time_unit="us", time_zone="UTC")`; no current Gold contract may be silently excluded.
- R5: Define application-layer `GoldSyncRepository` Protocol for reading sync state/digests/target summary, validating/creating consumer storage, and applying one lineage delta atomically; `application/` must not import psycopg or `infra`.
- R6: Define source compatibility fields: dataset ID, exchange, symbol, source artifact path, source fingerprint, schema signature, row count, timestamp min/max, and stable source version/build identity when present.
- R7: Keep application/domain contracts credential-free; password, administrator credentials, raw DSN, connection object, and cursor must not appear in dataclasses/results/errors.
- R8: Define one timestamp compatibility constant/policy shared by later PRs: source timestamp unit `us`, source timezone `UTC`, PostgreSQL timestamp type `TIMESTAMPTZ(6)`, PostgreSQL session timezone `UTC`; no naive timestamp or alternate PostgreSQL timestamp type is valid.

Acceptance:
- A1 (verifies R1): tests instantiate all six immutable contracts and verify exact fields/count semantics.
- A2 (verifies R2): tests assert every endpoint/role/schema/internal-table constant exactly.
- A3 (verifies R3): all current Gold dataset IDs map uniquely/deterministically; invalid/colliding/overlong fixtures fail before SQL generation.
- A4 (verifies R4): registry test iterates every current Gold build ID and fails if any cannot provide `exchange`, `symbol`, and `timestamp_m1` with exact `Datetime(us, UTC)` key type.
- A5 (verifies R5): fake repository satisfies the Protocol and import-boundary tests find no psycopg/infra import in `application/postgres_sync`.
- A6 (verifies R6): source snapshot fixtures serialize all compatibility fields deterministically.
- A7 (verifies R7): contract introspection proves no credential/DSN/connection/cursor field exists.
- A8 (verifies R8): contract tests assert exact `us`/`UTC`/`TIMESTAMPTZ(6)`/UTC-session values and reject naive/alternate-precision fixtures.

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
- R1: Implement deterministic SHA-256 row hashing over exact source column order with type tags/null markers, UTC epoch-microsecond datetime encoding, canonical finite floating-point encoding, and `-0.0 -> 0.0`; `timestamp_m1` is already canonical `Datetime(us, UTC)` and the planner must reject rather than silently reinterpret naive/non-UTC timestamp values.
- R2: Implement pure complete-state comparison keyed by `(exchange, symbol, timestamp_m1)` producing disjoint, deterministically sorted insert/update/delete/unchanged key sets.
- R3: Bootstrap rule: empty sync state plus empty digest state classifies every current source row as insert and no row as update/delete.
- R4: Reject ambiguous bootstrap when authoritative sync state is absent but lineage digest state is non-empty.
- R5: Classify identical key/hash as unchanged, changed hash as update, source-only key as insert, and target-only key as delete.
- R6: Do not use a timestamp watermark or previous-Gold-build dependency; arbitrarily old corrections and additions accumulated over multiple missed weeks must be discoverable whenever the source fingerprint changes.
- R7: Keep this module side-effect free: no filesystem, Polars scan, PostgreSQL, logging, wall-clock, or environment access.

Acceptance:
- A1 (verifies R1): equal canonical rows hash identically; one value change changes digest; null/value differs; `-0.0` equals `0.0`; invalid non-finite and naive/non-UTC timestamp fixtures fail deterministically.
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
- R4: Require valid source fingerprint, schema signature, row count, timestamp min/max, and canonical `timestamp_m1` source dtype `Datetime(us, UTC)` from validated artifact metadata/schema; missing/inconsistent metadata or timestamp type fails the lineage rather than guessing/coercing.
- R5: Return lineages in stable `(dataset_id, exchange, symbol)` order and reject duplicate current candidates.
- R6: Keep selector read-only: no Gold build, NAS mirror, retention/pruning, manifest mutation, or PostgreSQL connection.
- R7: Add fixtures for one current plus retained old versions, multiple datasets/symbols/timeframes, unregistered artifacts, duplicate-current ambiguity, missing/corrupt metadata, and wrong timestamp unit/timezone.

Acceptance:
- A1 (verifies R1): fixtures produce exactly one snapshot for every expected current lineage.
- A2 (verifies R2): every materialized registered Gold fixture is selected and Bronze/Silver/unregistered fixtures are absent.
- A3 (verifies R3): changing file mtimes does not alter selection and retained old versions are never selected.
- A4 (verifies R4): missing/corrupt fingerprint/schema/count/bounds or non-`Datetime(us, UTC)` `timestamp_m1` fails deterministically with no guessed values.
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
- R1: Generate deterministic quoted PostgreSQL DDL for one table in `crypto_loader` using source column order; primary key exactly `(exchange, symbol, timestamp_m1)`.
- R2: Match the shared `pg-temporal-v1` storage contract: canonical `timestamp_m1` source type is `Polars Datetime(time_unit="us", time_zone="UTC")`; every source timestamp/datetime column maps to PostgreSQL `TIMESTAMPTZ(6)` and every true date maps to `DATE`. Map string/categorical/enum -> `TEXT`, bool -> `BOOLEAN`, signed integer -> `BIGINT`, UInt64 -> `NUMERIC(20,0)`, float -> `DOUBLE PRECISION`, decimal -> exact `NUMERIC`, binary -> `BYTEA`, list/struct-like -> `JSONB`; reject unknown/ambiguous dtypes and reject naive/non-UTC timestamp source types rather than silently changing semantics.
- R3: Quote every schema/table/column identifier safely; dataset IDs and source column names are never interpolated unquoted.
- R4: Generate deterministic schema signature from ordered `(column_name, normalized_source_type, postgres_type, nullable)` entries plus primary-key contract.
- R5: Require `exchange`, `symbol`, `timestamp_m1` to exist and be non-nullable at the logical-key boundary; do not invent surrogate IDs or row-position keys.
- R6: Normal-sync DDL may create missing schemas/tables/indexes idempotently but must not emit `DROP`, `TRUNCATE`, table replacement, or destructive automatic `ALTER`.
- R7: Test mapper against every current Gold schema fixture constructible from repository tests, including nested fields mapped to JSONB and every timestamp/datetime field.

Acceptance:
- A1 (verifies R1): generated DDL has exact qualified table name, source column order, and composite primary key.
- A2 (verifies R2): canonical `Datetime(us, UTC)` fixtures map exactly to `TIMESTAMPTZ(6)`; all timestamp/datetime consumer columns use that exact PostgreSQL type; date uses `DATE`; naive/non-UTC timestamp and unknown dtype fixtures fail.
- A3 (verifies R3): adversarial identifiers stay quoted and cannot inject SQL statements.
- A4 (verifies R4): equal ordered schemas yield equal signatures and any column/type/nullability/key change changes signature.
- A5 (verifies R5): missing or nullable logical-key fields fail before DDL is returned.
- A6 (verifies R6): generated SQL contains no destructive operation and has no automatic destructive migration path.
- A7 (verifies R7): mapper coverage passes for all current Gold contract schema fixtures and asserts no PostgreSQL timestamp type other than `TIMESTAMPTZ(6)` is emitted for datetime fields.

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
- R2: Create connections only from injected `PGHOST/PGPORT/PGUSER/PGDATABASE/PGPASSWORD`; require exact host `10.10.1.3`, port `54321`, user `crypto-loader`, and force session timezone UTC before data operations.
- R3: Idempotently create/validate consumer tables from PR-71 DDL plus internal `crypto_loader_sync.gold_sync_state` and `crypto_loader_sync.gold_row_hashes`; internal timestamp fields are exactly `TIMESTAMPTZ(6)`: state `min_timestamp`, `max_timestamp`, `synced_at_utc`, and digest `timestamp_m1`; internal tables are not consumer Gold tables.
- R4: Read per-lineage sync state, target summary `(count,min_timestamp,max_timestamp)`, and complete `(exchange,symbol,timestamp_m1,row_sha256)` digest state without fetching unchanged feature payloads.
- R5: Implement one-lineage `apply_delta` under deterministic lineage-scoped `pg_advisory_xact_lock`: consumer mutations -> digest mutations -> sync-state write -> summary verification -> commit.
- R6: Roll back consumer rows, digest rows, and sync state together on SQL/verification error; retry against same source converges without duplicates.
- R7: Bootstrap may insert complete validated lineage; non-bootstrap writes exactly supplied delta and never `TRUNCATE`, `DROP`, delete-all, table swap, or full-table replacement.
- R8: Detect source/existing consumer schema-signature mismatch before row mutation and raise sanitized migration-required error; never auto-alter a live table destructively.
- R9: Redact runtime/admin secrets and credential-bearing DSNs from repr/errors/logs; persist no credentials in internal tables.
- R10: Add deterministic adapter tests with connection/cursor fakes for endpoint, timezone, DDL validation, lock/order, mixed delta counts, rollback, retry, schema mismatch, forbidden SQL, redaction, and microsecond timestamp round-trip.

Acceptance:
- A1 (verifies R1): dependency inspection finds psycopg and no newly added ORM/second driver.
- A2 (verifies R2): connection spy observes exact host/port/user, injected database/password, and UTC session timezone; wrong endpoint/user fails before data SQL.
- A3 (verifies R3): DDL tests create/validate exact consumer/internal identities; every internal datetime field is exactly `TIMESTAMPTZ(6)` and sync metadata stays out of consumer tables.
- A4 (verifies R4): query trace reads only state, summary, and key/hash digests for comparison.
- A5 (verifies R5): trace order is advisory lock -> consumer mutations -> digest mutations -> state write -> summary verification -> commit.
- A6 (verifies R6): injected failure leaves prior committed consumer/digest/state unchanged and retry succeeds once.
- A7 (verifies R7): bootstrap N rows produces N inserts; later `2 insert + 1 update + 1 delete` executes exactly those mutations and no full reload.
- A8 (verifies R8): schema mismatch causes zero consumer-row mutations and returns migration-required category.
- A9 (verifies R9): fake secrets/full DSN never appear in diagnostics or persisted parameters.
- A10 (verifies R10): all listed adapter cases pass offline without a live PostgreSQL server, including an aware UTC timestamp with non-zero microseconds that round-trips with identical instant and microsecond value.

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
- R1: Add idempotent operator provisioning targeting exactly `10.10.1.3:54321` that creates/validates LOGIN role exactly `crypto-loader`; static SQL must quote the hyphenated role name.
- R2: Receive administrator username/password and application-role password only from protected environment/runtime input; no secret in tracked files, process-list-visible arguments, examples, logs, or exception text.
- R3: Enforce role attributes exactly `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`.
- R4: Create/validate schemas `crypto_loader` and `crypto_loader_sync` owned by or granting only sufficient `USAGE/CREATE` rights to `crypto-loader`; no rights on other repository schemas.
- R5: Keep administrator credentials separate from application runtime credentials and never export admin credentials into Medallion/CLI runtime configuration.
- R6: Make repeated provisioning idempotent; incompatible pre-existing role attributes/schema ownership fail safely instead of broadening privileges silently.
- R7: Require `PGDATABASE` as protected operator input; do not guess or hard-code a database name.
- R8: Add offline command/SQL contract tests for endpoint/role/attributes/schemas, secret placeholders, idempotency, quoted role identity, and absence of literal credentials.

Acceptance:
- A1 (verifies R1): command/SQL fixtures resolve exact endpoint and exact role `crypto-loader`.
- A2 (verifies R2): tracked content contains only environment references/test placeholders and process commands never embed a password argument.
- A3 (verifies R3): SQL contract asserts all six exact least-privilege attributes.
- A4 (verifies R4): only `crypto_loader` and `crypto_loader_sync` rights are provisioned for the application role.
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
- R1: Define typed runtime configuration resolving exact `PGHOST=10.10.1.3`, `PGPORT=54321`, `PGUSER=crypto-loader`, required non-empty `PGDATABASE`, and protected `PGPASSWORD` from environment or already-ignored runtime config; tracked source/docs contain no password value.
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
- R3: On absent sync state plus empty digest state, load complete current source lineage, validate canonical `timestamp_m1` as UTC-aware microsecond precision, normalize every timestamp/datetime payload to aware UTC microsecond semantics, compute digests, and submit every row as bootstrap insert; true date fields remain dates.
- R4: If synchronized source fingerprint/schema/count/bounds equal current snapshot, perform zero consumer/digest row mutations and verify target summary.
- R5: If source fingerprint changed, read complete current lineage, enforce the same UTC/microsecond timestamp boundary before hashing, compute complete current digests, compare through PR-69, and submit only planned insert/update/delete payloads.
- R6: Preserve accumulated-delta semantics across any number of missed runs and historical corrections; timestamp-watermark optimization is forbidden.
- R7: After repository commit, require final target row count/min/max to equal source snapshot before reporting synchronized; verification failure must not advance authoritative sync checkpoint.
- R8: Stop on first lineage failure, return non-success with failing lineage/category, keep already committed earlier lineages valid, leave later lineages untouched, and make retry resume idempotently.
- R9: Return aggregate/per-lineage inserted/updated/deleted/unchanged counts plus source identities with no credential fields.
- R10: Add offline fake-inventory/repository/source-reader tests for bootstrap, unchanged fast path, mixed delta, historical update, delete, three missed runs, schema mismatch, verification failure, partial-progress retry, empty inventory, naive timestamp rejection, and microsecond preservation.

Acceptance:
- A1 (verifies R1): spies prove only inventory/source-read/repository interfaces are called.
- A2 (verifies R2): shuffled input produces stable sorted processing order and no concurrent DB writes.
- A3 (verifies R3): empty target plus N rows yields exactly N bootstrap inserts; timestamp payloads reach the repository as aware UTC microsecond values and date fields remain dates.
- A4 (verifies R4): unchanged fingerprint/state yields zero consumer/digest mutations while target summary is checked.
- A5 (verifies R5): fixture `2 new + 1 changed + 1 stale + 100 unchanged` submits exactly 2 inserts, 1 update, 1 delete and no unchanged payloads; timestamp normalization does not change instants or truncate microseconds.
- A6 (verifies R6): old correction and three missed-week additions reconcile fully in one later run.
- A7 (verifies R7): summary mismatch fails and cannot advance sync state to new source fingerprint.
- A8 (verifies R8): failure on lineage 2 preserves committed lineage 1, leaves lineage 3 untouched, and retry converges without duplicates.
- A9 (verifies R9): aggregate/per-lineage result fields/counts are exact and credential-free.
- A10 (verifies R10): every listed use-case scenario passes offline, including rejection of naive/non-UTC timestamps and preservation of non-zero microseconds.

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
- R6: Document PostgreSQL as rebuildable Gold-only serving replica, exact endpoint/user/schema names, first-full/later-delta behavior, current-version-only selection, manual retry, schema-migration failure semantics, and exact timestamp compatibility with the shared `pg-temporal-v1` convention: source `Datetime(us, UTC)`, PostgreSQL `TIMESTAMPTZ(6)`, UTC session.
- R7: Add deterministic Medallion tests for exact order `bronze -> silver -> gold -> postgres-gold-sync`, Gold-failure gating, PostgreSQL-failure propagation, no-op, retry without rebuilding Gold, dry-run inclusion, and timestamp-contract preservation.
- R8: Run complete configured quality suite (Ruff lint/format, Mypy, Pyright, ty, import-linter, config validation, docs inventory validation, Pytest, coverage) and record any environment-only check that cannot run.

Acceptance:
- A1 (verifies R1): generated pipeline contains one and only one PostgreSQL sync directly after Gold.
- A2 (verifies R2): injected Bronze/Silver/Gold failures produce zero PostgreSQL calls; Gold success produces exactly one sync call.
- A3 (verifies R3): injected PostgreSQL failure returns non-zero while local Gold and NAS mirror are not reverted/deleted.
- A4 (verifies R4): no second scheduled PostgreSQL cron is introduced and docs state existing Sunday Medallion run owns scheduling.
- A5 (verifies R5): touched-file scans contain no operational/admin credential literal and dry-run output has no password/DSN.
- A6 (verifies R6): README/ARCHITECTURE contain all listed serving-plane, delta, current-version, retry, migration, and exact `Datetime(us, UTC)` -> `TIMESTAMPTZ(6)` rules without claiming PostgreSQL is canonical.
- A7 (verifies R7): all ordering/failure/no-op/retry/dry-run tests pass offline and a regression fixture confirms no timestamp type/precision/timezone drift from `pg-temporal-v1`.
- A8 (verifies R8): final PR records full quality-gate result and preserves or improves repository coverage.

---

## Repository Audit Corrective Program

The 2026-08-24 repository audit supersedes the earlier interpretation that temporal conformance alone is sufficient to certify the PostgreSQL serving plane. The audit found independent correctness gaps in CI, backlog governance, ingestion failure semantics, payload validation, Gold artifact attestation, Medallion sequencing, PostgreSQL transaction boundaries, schema verification, runtime privileges, target-integrity verification, and production observability. PR-102 is the only destructive PostgreSQL reconstruction authority in this program. PR-100 may reload source/lake data only when PR-99 produces evidence of failed or unverified historical intervals.

Shared PostgreSQL temporal contract `pg-temporal-v1`:

- every persisted instant is exactly `TIMESTAMPTZ(6)`;
- every PostgreSQL session is UTC;
- the persistence/read boundary accepts only timezone-aware zero-offset UTC datetimes;
- true calendar dates remain `DATE`;
- `TIMESTAMP WITHOUT TIME ZONE` and timestamp precision other than six digits are forbidden;
- diagnostic serialization, where required outside PostgreSQL, is `YYYY-MM-DDTHH:MM:SS.ffffffZ`.

Verified audit findings that drive the work orders below:

- `.github/workflows/ci.yml` names the pull-request coverage check `pr-coverage-95`, while both that job and `pyproject.toml` currently enforce 90%; merged PR #158 explicitly established 95% as the repository contract.
- required integration jobs do not provision a real PostgreSQL server, so current PostgreSQL tests cannot prove server catalog, transaction, advisory-lock, timezone, or privilege semantics.
- `PostgresGoldRepository.ensure_lineage()` relies on `CREATE TABLE IF NOT EXISTS` plus saved sync-state signatures instead of independently validating the actual PostgreSQL catalog.
- reconciliation reads state/digests and builds the delta plan before `apply_delta()` acquires its transaction advisory lock, allowing two writers to plan from the same stale target state.
- the unchanged-fingerprint fast path verifies only target count/min/max; changed-path planning trusts the digest table rather than proving the consumer table, digest table, and checkpoint are mutually consistent.
- PostgreSQL connections have no explicit connect/statement/lock/idle-in-transaction timeout contract.
- PostgreSQL row decoding checks Python `datetime` type but does not independently reject naive/non-zero-offset values on reads.
- the runtime `crypto-loader` role owns both schemas and receives `CREATE`, while normal runtime synchronization also performs DDL; this is broader privilege than a DML-only serving writer requires.
- Gold input fingerprints hash exact Silver artifacts, but the PostgreSQL inventory does not require an independently verified output-Parquet SHA-256 before publishing a lineage.
- Medallion configuration accepts arbitrary layer order even though correctness requires Bronze before Silver before Gold, and PostgreSQL synchronization discovers all supported current filesystem artifacts rather than being explicitly scoped to the successfully published/fresh Gold set from that invocation.
- `fetch_candles_range`, funding fetches, and open-interest fetches can translate transport/provider failures into an empty result, making failed retrieval indistinguishable from confirmed no-data.
- trade parsing supplies zero/empty/`unknown` defaults for malformed required payload fields; OHLC candle parsing converts values without enforcing finite/positive/high-low/time-window invariants.
- the shared HTTP client embeds the full query string in raised error messages, creating an avoidable future secret/credential-bearing diagnostic surface.
- `BACKLOG_POSTGRES.md` is present again even though merged PR #174 explicitly consolidated PostgreSQL planning into `BACKLOG.md`; PR-67..PR-77 delivery metadata also needs reconciliation with merged PRs #174..#184.

Corrective dependency graph:

```text
PR-78 audit plan
  |-- PR-79 backlog/docs reconciliation
  |-- PR-80 coverage test gap -> PR-81 restore 95% enforcement
  |-- PR-82 real PostgreSQL CI
  |-- PR-83 strict PostgreSQL read boundary
  |-- PR-91 Medallion order contract
  |-- PR-93 OHLC fetch failures -> PR-97 OHLC semantic validation
  |-- PR-94 funding failure contract
  |-- PR-95 open-interest failure contract
  |-- PR-96 strict trade parsing
  |-- PR-98 HTTP diagnostic redaction
  |-- PR-89 Gold output SHA attestation -> PR-90 PostgreSQL artifact verification

PR-82 -> PR-84 lock-before-plan transaction UoW
PR-82 + PR-83 -> PR-85 PostgreSQL catalog/schema verification
PR-85 -> PR-86 admin DDL/runtime DML split
PR-84 + PR-85 -> PR-87 consumer/digest/state integrity
PR-82 + PR-84 -> PR-88 PostgreSQL timeout policy
PR-90 + PR-91 -> PR-92 fresh/current Medallion PostgreSQL scope
PR-93 + PR-94 + PR-95 + PR-96 + PR-97 + PR-98 -> PR-99 historical lake completeness audit
PR-89 + PR-90 + PR-99 -> PR-100 targeted source reconcile and certified Gold rebuild
PR-81 + PR-82 + PR-83 + PR-84 + PR-85 + PR-86 + PR-87 + PR-88 + PR-90 + PR-92 + PR-100 -> PR-101 live PostgreSQL conformance
PR-79 + PR-100 + PR-101 -> PR-102 authoritative PostgreSQL reconstruction
```

Safe first parallel wave after PR-78: PR-79, PR-80, PR-82, PR-83, PR-89, PR-91, PR-93, PR-94, PR-95, PR-96, and PR-98. A ticket may start only after every explicit dependency is merged.

---

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
- R4: Record that current branch protection is enabled and requires `pr-quality` plus `pr-coverage-95`, but the actual coverage threshold underneath that name is only 90%; do not silently choose a different target because merged PR #158 is the evidence for restoring 95%.
- R5: Repair only the two pre-existing CI blockers exposed by this documentation branch: isolate `tests/test_cli_lock.py` from the newly mandatory external `config.yaml` dependency and Ruff-format `tests/test_postgres_gold_repository.py`; do not change production semantics in this planning PR.
- R6: Define final production completion as PR-100 certified lake/Gold state plus PR-101 live PostgreSQL `PASS` plus PR-102 owned-schema reconstruction and zero-mutation replay.

Acceptance:
- A1 (verifies R1): findings are traceable to current code/settings and no unsupported live-data corruption statement is made.
- A2 (verifies R2): PR-79..PR-102 are contiguous and no earlier ticket authorizes destructive PostgreSQL reconstruction.
- A3 (verifies R3): each work order contains complete Git/ownership/dependency/R-A metadata suitable for a weak agent.
- A4 (verifies R4): the plan explicitly distinguishes the `pr-coverage-95` check name from the current 90% implementation and cites 95% as an already merged repository decision.
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

## PR-80: Raise Measured Production Coverage To At Least 95 Percent

PR name: `coverage-95-test-gap`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr80-coverage-95-test-gap`
Git status: `planned; start only when git status --short is empty`
Agent lane: Tests only; one weak agent
Depends on: PR-78
Commit: `test(PR-80): close coverage gap to 95 percent`
Allowed files: production-focused test files and test fixtures only; no production code, workflow, or threshold changes

Description:
- R1: Measure current production-code line coverage using the existing repository coverage configuration and record the exact uncovered lines/modules.
- R2: Add deterministic tests for real production behavior until total measured coverage is at least 95.00%; do not exclude production files, use pragma escapes, or lower coverage scope.
- R3: Keep tests offline except for existing explicitly marked network tests and preserve all current behavioral assertions.

Acceptance:
- A1 (verifies R1): before/after coverage evidence names exact totals and the highest-value uncovered branches covered.
- A2 (verifies R2): `coverage report --fail-under=95` passes at 95.00% or higher without production exclusions/threshold edits.
- A3 (verifies R3): the full required offline suite remains deterministic and network-independent.

---

## PR-81: Restore The Canonical 95 Percent Coverage Gate

PR name: `coverage-95-enforcement`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr81-coverage-95-enforcement`
Git status: `planned; start only when git status --short is empty`
Agent lane: CI/governance; one weak agent
Depends on: PR-80
Commit: `ci(PR-81): restore 95 percent coverage enforcement`
Allowed files: `.github/workflows/ci.yml`, `pyproject.toml`, `scripts/github/apply_quality_gates.sh`, coverage/quality contract tests, `README.md`, `ARCHITECTURE.md`

Description:
- R1: Restore `tool.coverage.report.fail_under=95` and make the `pr-coverage-95` job execute `--cov-fail-under=95`; remove text that incorrectly describes that job as a 90% gate.
- R2: Keep branch protection requiring `pr-quality` and `pr-coverage-95`, verify the exact read-back from GitHub, and ensure the setup script cannot silently configure a contradictory coverage context.
- R3: Add executable contract tests proving 94.99% fails and 95.00% passes and that workflow name, threshold, docs, and branch-protection context agree.

Acceptance:
- A1 (verifies R1): pyproject/workflow both enforce exactly 95 and no required-gate message says 90.
- A2 (verifies R2): GitHub read-back shows the intended required contexts and setup is idempotent.
- A3 (verifies R3): negative/positive threshold fixtures and cross-file consistency tests pass.

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
- A1 (verifies R1): target interval set equals PR-99 non-PASS intervals exactly and PASS input is a no-op.
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
- R4: Emit a sanitized `artifacts/acceptance/postgres-live-conformance-v2.json` with `PASS|FAIL`; any schema/role/data/temporal/permission/timeout discrepancy is FAIL.

Acceptance:
- A1 (verifies R1): compatible target passes and deliberate catalog/role/grant/timeout drift fails.
- A2 (verifies R2): same-count payload tamper, missing/extra key, digest mismatch, and stale checkpoint fixtures all fail.
- A3 (verifies R3): exact six-digit UTC instants round-trip and runtime DML succeeds while prohibited DDL fails; probes leave no durable mutation.
- A4 (verifies R4): report contains no password/DSN/raw market payload and cannot be PASS when any check fails.

---

## PR-102: Reconstruct Owned PostgreSQL Schemas From Certified Gold

PR name: `postgres-authoritative-reconstruction`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `codex/pr102-postgres-authoritative-reconstruction`
Git status: `planned; start only when git status --short is empty`
Agent lane: Production reconstruction; one agent only
Depends on: PR-79, PR-100, PR-101
Commit: `chore(PR-102): reconstruct PostgreSQL from certified Gold`
Allowed files: guarded production reconstruction command/runbook, final acceptance report, focused operator tests; no provider/business-feature code

Description:
- R1: Disable the scheduled Medallion publication path and acquire both the host pipeline lock and a dedicated PostgreSQL reconstruction lock; verify exact endpoint/database and prohibit concurrent normal writers.
- R2: Before destructive work, create and validate a timestamped backup of only `crypto_loader` and `crypto_loader_sync`, capture private restore/checksum evidence plus sanitized pre-state catalog/count/digest summaries, and stop if backup verification fails.
- R3: Recreate/migrate only the two loader-owned serving/sync schemas through the PR-85/PR-86 administrator path, preserving all unrelated schemas/roles/data exactly and provisioning the runtime role with DML-only privileges.
- R4: Bootstrap every PR-92 eligible current certified Gold lineage from PR-100 certified Gold using the locked normal PR-84 repository path; write exact consumer rows, digest index, and checkpoints under `pg-temporal-v1`.
- R5: Run PR-101 independently after reconstruction and require PASS plus exact source/consumer keys/digests/rows/bounds/versions/output hashes and role/permission/schema/timeout/temporal equality.
- R6: Immediately run unchanged `gold-sync-postgres` and require zero inserts, updates, deletes, digest changes, checkpoint semantic rewrites, or timestamp rewrites; re-enable scheduling only after all evidence is PASS.
- R7: Commit only a sanitized `artifacts/acceptance/postgres-production-reconstruction-v2.json`; any backup, source-certification, schema, role, data, temporal, permission, timeout, unrelated-object, or replay mismatch blocks completion.

Acceptance:
- A1 (verifies R1): scheduler/host/DB lock evidence proves no normal writer overlaps reconstruction.
- A2 (verifies R2): validated restore evidence exists before the first destructive statement and covers both owned schemas.
- A3 (verifies R3): before/after catalog proves only `crypto_loader` and `crypto_loader_sync` were reconstructed and runtime has no DDL privilege.
- A4 (verifies R4): every eligible certified Gold lineage is fully present with exact canonical keys/data/digests/checkpoint state and `TIMESTAMPTZ(6)` UTC semantics.
- A5 (verifies R5): PR-101 independently returns PASS with zero symmetric differences and all operational contracts satisfied.
- A6 (verifies R6): immediate replay is a semantic no-op and schedule restoration occurs only after PASS.
- A7 (verifies R7): final report is sanitized, names `pg-temporal-v1`, and any injected mismatch prevents completion.

## Corrected production completion definition

The PostgreSQL serving plane is not production-certified merely because PR-67 through PR-77 are merged or because unit/fake-connection checks pass. Completion requires: PR-79 establishes one accurate backlog source; PR-81 restores the already-approved 95% quality gate; PR-82 proves real PostgreSQL behavior in CI; PR-83 through PR-98 close the temporal, concurrency, schema, privilege, integrity, orchestration, ingestion-validation, and diagnostic gaps; PR-99 certifies historical lake completeness; PR-100 performs only evidence-driven source reconciliation and produces certified current Gold; PR-101 independently verifies the live target; and PR-102 reconstructs only the owned PostgreSQL schemas from certified Gold, proves exact equivalence, and obtains a zero-mutation replay. Until PR-102 PASS, existing PostgreSQL data must be treated as a serving replica whose full current correctness has not been independently certified.

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
