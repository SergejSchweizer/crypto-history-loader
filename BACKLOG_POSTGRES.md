# PostgreSQL Gold Sync Backlog

This backlog extension is the implementation source of truth for replicating the current Gold serving state from
`crypto-loader` into PostgreSQL. It intentionally follows the proven PostgreSQL Gold-sync architecture in
`market-regime-loader`, but adapts it to this repository's larger, heterogeneous Gold contract set.

The PostgreSQL endpoint is exactly `10.10.1.3:54321`. Parquet Gold under the repository Gold lake remains the
canonical source of truth; PostgreSQL is a rebuildable serving-plane replica. Only registered Gold datasets are
eligible for replication. Bronze and Silver data must never be written to PostgreSQL by this stack.

The dedicated PostgreSQL LOGIN role is exactly `crypto-loader`. Because the role name contains hyphens,
provisioning SQL must quote it as `"crypto-loader"`. The operational password is supplied out-of-band via
protected runtime configuration/environment and must never be committed, printed, logged, embedded in examples, or
persisted in sync metadata. Administrator credentials used to create the role are separate from application runtime
credentials.

PostgreSQL consumer data lives in schema `crypto_loader_gold`. Synchronization state lives separately in schema
`crypto_loader_sync`. Every registered Gold dataset maps one-to-one to a consumer table whose name is derived
deterministically from the dataset ID by replacing `.` with `_`, for example
`gold.market.regime_features.m1 -> crypto_loader_gold.gold_market_regime_features_m1`. All current registered Gold
dataset IDs must map uniquely and fit PostgreSQL's identifier length limit; collisions or overlong names are hard
errors, never silently shortened.

Each consumer table mirrors the current Parquet Gold row schema and uses the composite logical key
`(exchange, symbol, timestamp_m1)`. Every currently registered Gold contract must therefore expose those three
columns before it is considered publishable. PostgreSQL types are derived deterministically from the source schema;
unsupported or ambiguous types fail before any write. Existing table/source schema-signature mismatch fails with a
migration-required error; normal sync must never `DROP`, `TRUNCATE`, replace a table, or silently mutate its schema.

Synchronization semantics are state reconciliation, not a timestamp-watermark feed:

- The first successful sync for a `(dataset_id, exchange, symbol)` lineage inserts the complete current Gold history.
- A later sync first checks the current Gold manifest/source fingerprint against the last successful sync state.
- If the fingerprint is unchanged, no consumer-row mutation is allowed; target count/min/max are still verified.
- If the fingerprint changed, the current complete Gold lineage is hashed row-by-row and compared with the complete
  PostgreSQL digest state for that lineage.
- Only the accumulated `INSERT`, `UPDATE`, and `DELETE` delta is transmitted to the consumer table; unchanged rows
  are never rewritten.
- This catches historical corrections, deleted rows, and any number of missed Sunday runs. A last-timestamp
  watermark is explicitly forbidden because it cannot detect historical revisions or deletions.
- Each lineage is applied in one PostgreSQL transaction under a lineage-scoped advisory transaction lock. The
  consumer rows, digest rows, and sync-state checkpoint commit together or roll back together.
- The current Gold artifact selector must use repository contracts/manifests and must select exactly one current
  artifact per `(dataset_id, exchange, symbol)` lineage. Retained older Gold versions are not synchronized.

The existing Medallion pipeline remains responsible for Bronze -> Silver -> Gold. PostgreSQL sync is appended only
after a successful Gold step. Therefore every successful Medallion invocation, including the existing Sunday cron
invocation, attempts Gold -> PostgreSQL synchronization. PostgreSQL failure makes the Medallion invocation non-zero
but must not roll back already-published local Gold. A retry runs only the PostgreSQL sync and converges from the last
successful per-lineage checkpoints.

## Mandatory Agent Git Protocol

Every implementation PR below has an explicit `Git branch` and `Git status` field because the work will be delegated
to weak agents. Agents must use a separate checkout/worktree per PR; parallel agents must never share one working
tree.

Before editing any PR:

```bash
git fetch origin
git switch main
git pull --ff-only
git status --short
```

`git status --short` must produce no output. Then create/switch to the exact branch named in that ticket. Before
handoff, run `git status --short` again and replace the ticket's planned Git-status note in the PR description or
handoff evidence with the exact output. A dirty worktree before starting is a hard stop.

All implementation commits and squash-merge titles must include the backlog identifier in the existing repository
convention, for example `feat(PR-68): ...`.

## Parallel Delivery Waves

```text
PR-67 postgres-gold-sync-backlog
   |
   +------------------------+------------------------+
   v                        v                        v
PR-68 contracts         PR-73 role provisioning  PR-74 runtime config
   |
   +----------------+----------------+
   v                v                v
PR-69 delta        PR-70 inventory  PR-71 SQL schema mapper
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

- Wave 1: PR-68, PR-73, PR-74 in parallel after PR-67 is merged.
- Wave 2: PR-69, PR-70, PR-71 in parallel after PR-68 is merged.
- Wave 3: PR-72 after PR-68 and PR-71.
- Wave 4: PR-75 after PR-69, PR-70, PR-72.
- Wave 5: PR-76 after PR-74 and PR-75.
- Wave 6: PR-77 after PR-73 and PR-76.

Agents must not broaden scope to work owned by another PR. If a dependency is missing, stop and report the missing
contract instead of reimplementing it locally.

---

## PR-67: PostgreSQL Gold Sync Backlog

PR name: `postgres-gold-sync-backlog`
Status: In Progress
Updated: 2026-08-22
PR: TBD
Git branch: `codex/pr67-postgres-gold-sync-backlog`
Git status: `planned/clean; git status --short must be empty before handoff`
Agent lane: Planning/governance; one agent only
Depends on: none
Commit: `docs(PR-67): add PostgreSQL Gold sync backlog`
Allowed files: `BACKLOG_POSTGRES.md` only

Description:
- R1: Define PR-67 through PR-77 with exact dependencies, branch names, Git-status fields, file ownership, requirements,
  acceptance criteria, and parallel waves for weak agents.
- R2: Define the serving-plane contract: only registered current Gold is replicated to `10.10.1.3:54321`; Parquet
  Gold remains authoritative and Bronze/Silver replication is forbidden.
- R3: Define the exact runtime role `crypto-loader`, consumer schema `crypto_loader_gold`, internal schema
  `crypto_loader_sync`, and strict no-secret-in-Git/logging policy.
- R4: Define deterministic one-table-per-Gold-dataset mapping and composite row identity
  `(exchange, symbol, timestamp_m1)`.
- R5: Define first-full/later-delta reconciliation using current Gold fingerprints plus complete row digests for changed
  lineages, including inserts, updates, deletes, missed runs, and historical corrections; timestamp watermarks are
  forbidden.
- R6: Define transactional/advisory-lock semantics, schema-mismatch failure behavior, and no `DROP`/`TRUNCATE`/table
  replacement during normal sync.
- R7: Define Medallion ordering `Bronze -> Silver -> Gold -> PostgreSQL`, where PostgreSQL runs only after Gold success
  and PostgreSQL failure never rolls back already-published local Gold.

Acceptance:
- A1 (verifies R1): the document contains exactly PR-67 through PR-77 once each and every ticket contains `Git branch`
  and `Git status` fields plus one-to-one numbered R/A items.
- A2 (verifies R2): the endpoint and Gold-only serving-plane rules are explicit and PostgreSQL is never described as
  canonical storage.
- A3 (verifies R3): the exact role and schema names are present and no operational/admin password literal is present.
- A4 (verifies R4): table-name mapping and the exact three-column logical key are explicit and deterministic.
- A5 (verifies R5): bootstrap, no-op, accumulated delta, missed-run, update, delete, and historical-revision semantics
  are explicit and no last-timestamp watermark is permitted.
- A6 (verifies R6): atomic lineage transaction, advisory lock, schema mismatch failure, and forbidden destructive SQL
  are explicit.
- A7 (verifies R7): the post-Gold ordering, failure propagation, and retry semantics are explicit.

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
Allowed files: `application/postgres_sync/__init__.py`, `application/postgres_sync/contracts.py`,
`tests/test_postgres_sync_contracts.py`

Description:
- R1: Add immutable typed contracts for `GoldLineage`, `GoldSourceSnapshot`, `GoldSyncState`, `GoldRowDigest`,
  `GoldDeltaPlan`, and `GoldSyncResult`; counts must include inserted/updated/deleted/unchanged.
- R2: Define exact constants for host `10.10.1.3`, port `54321`, role `crypto-loader`, consumer schema
  `crypto_loader_gold`, sync schema `crypto_loader_sync`, state table `gold_sync_state`, and digest table
  `gold_row_hashes`.
- R3: Define deterministic dataset-ID -> consumer-table mapping by replacing `.` with `_`; reject invalid characters,
  collisions, names longer than 63 bytes, or mapping outside `crypto_loader_gold`.
- R4: Define the publishable Gold row key exactly as `(exchange, symbol, timestamp_m1)` and add a contract check proving
  every current `supported_gold_build_ids()` dataset can satisfy this key; no current Gold contract may be silently
  excluded.
- R5: Define an application-layer `GoldSyncRepository` Protocol for reading sync state/digests/target summary,
  validating/creating consumer storage, and applying one lineage delta atomically; `application/` must not import
  psycopg or `infra`.
- R6: Define source compatibility fields: `dataset_id`, `exchange`, `symbol`, source artifact path, source fingerprint,
  schema signature, row count, timestamp min/max, and stable source version/build identity when present.
- R7: Define credential-free error/result contracts; password, administrator credentials, raw DSN, connection object,
  and SQL cursor must not appear in domain/application dataclasses.

Acceptance:
- A1 (verifies R1): tests instantiate all six immutable contracts and verify exact fields plus count semantics.
- A2 (verifies R2): tests assert every endpoint/role/schema/internal-table constant exactly.
- A3 (verifies R3): all current Gold dataset IDs map uniquely and deterministically; invalid/colliding/overlong fixtures
  fail before SQL is generated.
- A4 (verifies R4): a registry test iterates every current Gold build ID and fails if any cannot provide
  `exchange`, `symbol`, and `timestamp_m1`.
- A5 (verifies R5): a fake repository satisfies the Protocol and import-boundary tests find no psycopg/infra import in
  `application/postgres_sync`.
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
- R1: Implement deterministic SHA-256 row hashing over the exact source column order with explicit type tags/null
  markers, UTC epoch-microsecond datetime encoding, canonical finite floating-point encoding, and `-0.0 -> 0.0`;
  reject NaN/infinity in values that cannot be represented deterministically.
- R2: Implement pure complete-state comparison keyed by `(exchange, symbol, timestamp_m1)` producing disjoint,
  deterministically sorted insert/update/delete/unchanged key sets.
- R3: For first bootstrap, empty sync state plus empty digest state classifies every current source row as insert and
  no row as update/delete.
- R4: Reject ambiguous bootstrap when authoritative sync state is absent but digest state for that lineage is non-empty.
- R5: Identical key/hash pairs are unchanged only; changed hashes are updates; source-only keys are inserts;
  target-only keys are deletes.
- R6: The planner must have no timestamp-watermark or previous-Gold-build dependency; an arbitrarily old corrected row
  and rows accumulated over multiple missed weeks must be detected whenever the source fingerprint changes.
- R7: Keep this module side-effect free: no filesystem, Polars scan, PostgreSQL, logging, wall-clock, or environment
  access.

Acceptance:
- A1 (verifies R1): equal canonical rows hash identically; one value change changes the digest; null/value differs;
  `-0.0` equals `0.0`; invalid non-finite fixtures fail deterministically.
- A2 (verifies R2): mixed fixtures yield exact mutually exclusive ordered key sets with no key present in two sets.
- A3 (verifies R3): N source rows and empty target state yield exactly N inserts.
- A4 (verifies R4): digest rows without sync state fail before a delta plan is returned.
- A5 (verifies R5): dedicated fixtures separately prove insert, update, delete, and unchanged classification.
- A6 (verifies R6): tests detect a historical correction and three missed-run additions without using a last-sync
  timestamp.
- A7 (verifies R7): import/monkeypatch tests prove the planner has no external side effects and repeated calls are
  byte-for-byte deterministic in serialized output.

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
- R1: Build a read-only inventory over `lake/gold` using existing Gold contracts/manifests/discovery semantics and
  return exactly one current source snapshot per `(dataset_id, exchange, symbol)` lineage.
- R2: Include every current materialized registered Gold dataset regardless of timeframe (`m1`, `m5`, `m30`, `h1`,
  history, live, core, regime, and other registered families); Bronze/Silver and unregistered files are never
  publishable.
- R3: Select current artifacts by repository version/manifest semantics, not filesystem mtime/ctime or arbitrary
  lexicographic recency; retained older Gold versions must not appear in the inventory.
- R4: Require a valid source fingerprint, schema signature, row count, and timestamp min/max from validated manifest
  or deterministic existing metadata; missing/inconsistent metadata fails that lineage rather than guessing.
- R5: Return lineages in stable `(dataset_id, exchange, symbol)` order and reject duplicate current candidates.
- R6: The selector is read-only and must not build Gold, mirror Gold to `/volume1/Temp/gold`, prune versions, mutate
  manifests, or open a PostgreSQL connection.
- R7: Add fixtures covering one current plus two retained old versions, multiple datasets/symbols/timeframes, an
  unregistered artifact, duplicate-current ambiguity, and missing/corrupt metadata.

Acceptance:
- A1 (verifies R1): fixtures produce exactly one snapshot for every expected current lineage.
- A2 (verifies R2): every materialized registered Gold fixture is selected and Bronze/Silver/unregistered fixtures are
  absent.
- A3 (verifies R3): changing file mtimes does not alter selection and retained old versions are never selected.
- A4 (verifies R4): missing/corrupt fingerprint/schema/count/bounds fails deterministically with no guessed values.
- A5 (verifies R5): output ordering is stable and duplicate current candidates fail.
- A6 (verifies R6): side-effect spies prove no build/mirror/prune/write/DB call occurs.
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
- R1: Implement deterministic, quoted PostgreSQL DDL generation for one consumer table in `crypto_loader_gold` using
  the PR-68 table-name mapping and source column order; the primary key is exactly
  `(exchange, symbol, timestamp_m1)`.
- R2: Map datetime to `TIMESTAMPTZ(6)` in UTC, date to `DATE`, string/categorical/enum to `TEXT`, boolean to `BOOLEAN`,
  signed integers to `BIGINT`, UInt64 to `NUMERIC(20,0)`, float to `DOUBLE PRECISION`, decimal to exact `NUMERIC`,
  binary to `BYTEA`, and list/struct-like values to `JSONB`; reject unknown/ambiguous dtypes.
- R3: Quote all schema/table/column identifiers safely; dataset IDs and source column names are never interpolated into
  SQL unquoted.
- R4: Generate a deterministic schema signature from ordered `(column_name, normalized_source_type, postgres_type,
  nullable)` entries plus primary-key contract.
- R5: Require `exchange`, `symbol`, `timestamp_m1` to exist and be non-nullable at the logical-key boundary; do not
  invent surrogate IDs or use row position.
- R6: Normal sync DDL may create missing schemas/tables/indexes idempotently but must not emit `DROP`, `TRUNCATE`,
  table replacement, or automatic destructive `ALTER`; incompatible existing signature is handled as a hard
  migration-required error by the adapter.
- R7: Test the mapper against every current Gold schema fixture that repository tests can construct, including any
  JSONB-mapped nested fields.

Acceptance:
- A1 (verifies R1): generated DDL has exact qualified table name, source column order, and composite primary key.
- A2 (verifies R2): one fixture per listed dtype produces the exact PostgreSQL type and unknown dtype fails.
- A3 (verifies R3): adversarial identifier fixtures remain quoted and cannot inject additional SQL statements.
- A4 (verifies R4): equal ordered schemas yield equal signatures and any column/type/nullability/key change changes the
  signature.
- A5 (verifies R5): missing or nullable logical-key fields fail before DDL is returned.
- A6 (verifies R6): generated SQL contains no destructive token and incompatibility has no auto-migration path.
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
Allowed files: `infra/postgres/__init__.py`, `infra/postgres/gold_repository.py`, `pyproject.toml`, `uv.lock`,
`tests/test_postgres_gold_repository.py`

Description:
- R1: Add `psycopg` as the only new PostgreSQL runtime client; do not add SQLAlchemy, an ORM, or a second PostgreSQL
  driver.
- R2: Implement connection creation from injected `PGHOST/PGPORT/PGUSER/PGDATABASE/PGPASSWORD`, require exact host
  `10.10.1.3`, port `54321`, user `crypto-loader`, and force session timezone UTC before any data operation.
- R3: Implement idempotent creation/validation of consumer tables from PR-71 DDL plus internal tables
  `crypto_loader_sync.gold_sync_state` and `crypto_loader_sync.gold_row_hashes`; internal tables are not exposed as
  consumer Gold tables.
- R4: Read per-lineage sync state, target summary `(count,min_timestamp,max_timestamp)`, and complete
  `(exchange,symbol,timestamp_m1,row_sha256)` digest state without fetching unchanged consumer feature payloads.
- R5: Implement one-lineage `apply_delta` under a deterministic lineage-scoped `pg_advisory_xact_lock`: insert new
  rows, update changed rows, delete stale rows, mutate digest rows, write sync state last, verify summary, then commit.
- R6: On any SQL/verification error roll back consumer rows, digest rows, and sync state together; a retry against the
  same source must converge without duplicate keys.
- R7: First bootstrap may insert the complete validated lineage; non-bootstrap writes exactly the supplied delta and
  never executes `TRUNCATE`, `DROP`, delete-all, table swap, or full-table replacement.
- R8: Detect source/existing consumer schema-signature mismatch before row mutation and raise a sanitized
  migration-required error; never auto-alter a live Gold table destructively.
- R9: Redact `PGPASSWORD`, administrator secrets, and credential-bearing DSNs from repr/errors/log messages; do not
  persist credentials in either internal table.
- R10: Add deterministic adapter tests with connection/cursor fakes for endpoint identity, timezone, DDL validation,
  lock/order, mixed delta counts, rollback, retry, schema mismatch, forbidden SQL, and redaction.

Acceptance:
- A1 (verifies R1): dependency inspection finds psycopg and no newly added ORM/second driver.
- A2 (verifies R2): connection spy observes exact host/port/user, injected database/password, and UTC session timezone;
  wrong endpoint/user fails before data SQL.
- A3 (verifies R3): DDL tests create/validate exact consumer/internal identities and keep sync metadata out of consumer
  tables.
- A4 (verifies R4): query trace reads only state, summary, and key/hash digests for comparison.
- A5 (verifies R5): trace order is advisory lock -> consumer mutations -> digest mutations -> state write -> summary
  verification -> commit.
- A6 (verifies R6): injected failure leaves the prior committed consumer/digest/state snapshot unchanged and retry
  succeeds once.
- A7 (verifies R7): bootstrap N rows produces N inserts; later `2 insert + 1 update + 1 delete` executes exactly those
  mutations and no full-reload SQL.
- A8 (verifies R8): schema mismatch causes zero consumer-row mutation and returns a deterministic migration-required
  category.
- A9 (verifies R9): test secrets/full DSN never appear in diagnostics or persisted parameters.
- A10 (verifies R10): all listed offline adapter cases pass without a live PostgreSQL server.

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
Allowed files: `scripts/provision_postgres_sync_role.py`, `infra/postgres/provisioning.sql`,
`tests/test_postgres_role_provisioning.py`

Description:
- R1: Add an idempotent operator provisioning command targeting exactly `10.10.1.3:54321` that creates or validates
  the LOGIN role exactly `crypto-loader`; static SQL must quote the hyphenated role name.
- R2: Receive administrator username/password and application-role password only from protected environment/runtime
  input; no operational secret may occur in tracked files, command arguments visible in process listings, examples,
  logs, or exception text.
- R3: Enforce exact role attributes `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`.
- R4: Create/validate schemas `crypto_loader_gold` and `crypto_loader_sync` owned by or granting only sufficient
  `USAGE/CREATE` rights to `crypto-loader`; do not grant rights on schemas owned by other repositories.
- R5: Keep administrator credentials completely separate from application runtime credentials and never export admin
  credentials into Medallion/CLI runtime configuration.
- R6: Make repeated provisioning idempotent; incompatible pre-existing role attributes/schema ownership fail safely
  instead of escalating or broadening privileges silently.
- R7: Require `PGDATABASE` as protected operator input because the user did not specify a database name; no database
  name is guessed or hard-coded.
- R8: Add offline command/SQL contract tests for exact endpoint/role/attributes/schemas, secret placeholders,
  idempotency behavior, quoted role identity, and absence of literal credentials.

Acceptance:
- A1 (verifies R1): command/SQL fixtures resolve exact endpoint and exact role `crypto-loader`.
- A2 (verifies R2): tracked content contains only environment variable references/test placeholders and process-command
  construction never embeds a password argument.
- A3 (verifies R3): SQL contract asserts all six exact least-privilege attributes.
- A4 (verifies R4): only `crypto_loader_gold` and `crypto_loader_sync` rights are provisioned for the application role.
- A5 (verifies R5): admin inputs are distinct and absent from application-role output/config objects.
- A6 (verifies R6): second-run fixture is a no-op/validation pass while incompatible state fails without privilege
  escalation.
- A7 (verifies R7): missing/blank database input fails before attempting a connection.
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
Allowed files: `application/postgres_sync/config.py`, `scripts/runtime_config.py`,
`tests/test_postgres_sync_config.py`, `tests/test_runtime_config.py`

Description:
- R1: Define a typed runtime configuration loader resolving exact `PGHOST=10.10.1.3`, `PGPORT=54321`,
  `PGUSER=crypto-loader`, required non-empty `PGDATABASE`, and required protected `PGPASSWORD` from environment
  or the repository's already-ignored runtime config; tracked source/docs must contain no password value.
- R2: Preserve existing logging/runtime behavior in `scripts/runtime_config.py`; PostgreSQL support is additive and
  must not change existing log-path resolution.
- R3: Environment variables, when explicitly set, override ignored runtime-config PostgreSQL values; partial mixed
  configuration is allowed only when the final resolved five-variable set is complete and exact.
- R4: Validate endpoint/user/database/password before adapter construction; wrong host/port/user or blank
  database/password fails deterministically without opening a connection.
- R5: Redact the password and any credential-bearing DSN from validation errors, dataclass repr, debug logs, and JSON
  result/error payloads.
- R6: Provide a method returning the five standard `PG*` values for subprocess/CLI composition without exposing admin
  provisioning credentials.
- R7: Add deterministic tests for environment-only, ignored-config-only, override precedence, invalid identity,
  missing values, shell-special password handling, and redaction using fake secrets only.

Acceptance:
- A1 (verifies R1): valid fixture resolves exact host/port/user plus injected database/password and no tracked fixture
  contains an operational secret.
- A2 (verifies R2): all pre-existing runtime/log configuration tests remain unchanged and passing.
- A3 (verifies R3): precedence fixtures produce the exact final five-variable mapping.
- A4 (verifies R4): each invalid/missing required field fails before a mocked connection factory is called.
- A5 (verifies R5): fake password/full DSN is absent from repr, errors, logs, and serialized payloads.
- A6 (verifies R6): exported runtime mapping contains only `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD` and
  no admin variables.
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
- R1: Implement a deterministic application service that receives the PR-70 current-lineage inventory and a
  `GoldSyncRepository`; it must not invoke Bronze/Silver/Gold build, mirror, retention, provider, or PostgreSQL
  provisioning operations.
- R2: Process lineages sequentially in stable `(dataset_id, exchange, symbol)` order so restart behavior and logs are
  deterministic and PostgreSQL load is bounded.
- R3: On absent sync state plus empty digest state, load the complete current source lineage, compute digests, and
  submit every row as bootstrap inserts.
- R4: If synchronized source fingerprint/schema/count/bounds equal the current snapshot, perform zero consumer/digest
  row mutations and verify target summary; do not reload/rewrite the full table.
- R5: If the source fingerprint changed, read the complete current lineage, compute complete current row digests,
  compare against complete target digest state through PR-69, and submit only planned inserts/updates/deletes.
- R6: Preserve accumulated-delta semantics across any number of missed runs and historical corrections; a
  timestamp-watermark optimization is forbidden.
- R7: After repository commit, require final target row count/min/max to equal current source snapshot before reporting
  that lineage synchronized; failures leave the previous authoritative sync checkpoint in force.
- R8: Stop on the first lineage failure, return a non-success result with the failing lineage/category, keep already
  committed earlier lineages valid, and make retry resume idempotently from per-lineage state.
- R9: Return aggregate and per-lineage inserted/updated/deleted/unchanged counts plus source identities with no
  credential fields.
- R10: Add offline fake-inventory/repository/source-reader tests for bootstrap, unchanged fast path, mixed delta,
  historical update, delete, three missed runs, schema mismatch, verification failure, partial-progress retry, and
  empty inventory.

Acceptance:
- A1 (verifies R1): spies prove only inventory/source-read/repository interfaces are called.
- A2 (verifies R2): shuffled input produces stable sorted processing order and no concurrent DB writes.
- A3 (verifies R3): empty target plus N rows yields exactly N bootstrap inserts.
- A4 (verifies R4): unchanged fingerprint/state yields zero consumer/digest mutations while target summary is checked.
- A5 (verifies R5): fixture `2 new + 1 changed + 1 stale + 100 unchanged` submits exactly 2 inserts, 1 update, 1 delete
  and no unchanged row payloads.
- A6 (verifies R6): an old row correction and three missed-week additions are fully reconciled in one later run.
- A7 (verifies R7): summary mismatch fails and cannot advance sync state to the new source fingerprint.
- A8 (verifies R8): failure on lineage 2 preserves committed lineage 1, does not touch lineage 3, and retry converges
  without duplicates.
- A9 (verifies R9): aggregate/per-lineage result fields and counts are exact and credential-free.
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
- R1: Add exactly one operational CLI command `gold-sync-postgres` with `--gold-root`, `--debug`, and existing global
  `--config` behavior; default Gold root is `lake/gold`.
- R2: Compose PR-70 inventory, source reader, PR-75 service, and PR-72 repository only after PR-74 configuration
  validation succeeds; missing/invalid config must create no DB connection.
- R3: The command is read-only toward Bronze/Silver/local Gold and must not build, mirror, prune, reconcile source
  providers, or provision PostgreSQL roles/schemas with admin credentials.
- R4: Use the repository's existing logging utilities/shared configured `.logs` root with module logger
  `postgres-gold-sync`; never print a password/DSN and do not create an unrelated logging subsystem.
- R5: Emit deterministic success JSON/log fields: command, status, lineages processed, inserted, updated, deleted,
  unchanged, and elapsed metadata already supported by repository logging conventions.
- R6: Return stable non-zero exit codes/categories for configuration, current-Gold inventory, compatibility/schema,
  PostgreSQL, and verification errors; success/no-op returns zero.
- R7: Provide a manual retry path that runs only `gold-sync-postgres`, so a PostgreSQL outage after Gold publication
  never requires rebuilding Bronze/Silver/Gold.
- R8: Add parser/composition/no-side-effect/result/redaction/error tests with deterministic fakes and no network.

Acceptance:
- A1 (verifies R1): parser exposes exactly `gold-sync-postgres`, expected arguments, and `--debug`.
- A2 (verifies R2): composition spy sees no DB factory call for invalid config and exact validated dependencies for a
  valid fixture.
- A3 (verifies R3): side-effect spies prove no build/mirror/prune/provider/provisioning call is reachable.
- A4 (verifies R4): command logs through existing logging utilities and fake secrets/full DSN are absent from output.
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
Allowed files: `scripts/run_medallion_pipeline.py`, `README.md`, `ARCHITECTURE.md`,
`tests/test_run_medallion_pipeline.py`, `tests/test_postgres_sync_medallion.py`

Description:
- R1: Append a `postgres-gold-sync` pipeline step immediately after the configured successful Gold step; exact command
  is the existing Python entrypoint plus `gold-sync-postgres` using the same config and `lake/gold` root.
- R2: Gate the PostgreSQL step on Gold success: Bronze or Silver or Gold failure prevents PostgreSQL sync; successful
  Gold always attempts PostgreSQL sync in the same Medallion invocation.
- R3: PostgreSQL sync failure makes the Medallion command non-zero and logs the failed active step, but already
  published local Gold and its existing NAS mirror remain untouched and authoritative.
- R4: Preserve the existing Sunday cron schedule outside this stack; because the cron already invokes the Medallion
  runner, no second cron job is created. Every manual Medallion invocation also gets the same post-Gold sync behavior.
- R5: Use PR-74 protected runtime configuration only; no PostgreSQL/admin password is written into the pipeline script,
  README, ARCHITECTURE, tests, command line, or logged plan.
- R6: Document PostgreSQL as a rebuildable Gold-only serving replica, exact endpoint/user/schema names, first-full versus
  later-delta behavior, current-version-only selection, manual `gold-sync-postgres` retry, and schema-migration failure
  semantics.
- R7: Add deterministic Medallion tests for exact order `bronze -> silver -> gold -> postgres-gold-sync`, Gold failure
  gating, PostgreSQL failure propagation, successful no-op, retry without rebuilding Gold, and dry-run plan inclusion.
- R8: Run the complete configured quality suite (Ruff lint/format, Mypy, Pyright, ty, import-linter, config validation,
  documentation inventory validation, Pytest, and coverage) and record any environment-only check that cannot run.

Acceptance:
- A1 (verifies R1): generated pipeline steps contain one and only one PostgreSQL sync directly after Gold.
- A2 (verifies R2): injected Bronze/Silver/Gold failures produce zero PostgreSQL calls; Gold success produces exactly
  one sync call.
- A3 (verifies R3): injected PostgreSQL failure returns non-zero while pre-existing/new local Gold files and NAS mirror
  state are not reverted/deleted.
- A4 (verifies R4): no second scheduled PostgreSQL cron is introduced and docs state the existing Sunday Medallion run
  is the owner of scheduling.
- A5 (verifies R5): repository scans of touched files contain no operational/admin credential literal and dry-run output
  has no password/DSN.
- A6 (verifies R6): README/ARCHITECTURE contain all listed serving-plane, delta, current-version, retry, and migration
  rules without claiming PostgreSQL is canonical.
- A7 (verifies R7): all listed ordering/failure/no-op/retry/dry-run tests pass offline.
- A8 (verifies R8): the final PR records the full quality-gate result and preserves or improves repository coverage.

---

## Completion Definition

The PostgreSQL Gold serving-plane stack is complete only when all of the following are true:

- `crypto-loader` Gold Parquet remains canonical and PostgreSQL contains no Bronze/Silver serving tables from
  this stack.
- The exact dedicated runtime role `crypto-loader` exists with least privilege on only
  `crypto_loader_gold` and `crypto_loader_sync` within the configured database.
- Every current materialized registered Gold lineage has exactly one current PostgreSQL representation; retained old
  Gold versions are not duplicated into PostgreSQL.
- First sync performs a complete lineage bootstrap; later runs write only accumulated INSERT/UPDATE/DELETE deltas.
- Historical corrections and deletes are detected without a timestamp watermark.
- Consumer rows, row digests, and sync checkpoint are atomic per lineage and retry-safe.
- An unchanged source fingerprint performs no consumer-row rewrite.
- Every successful Medallion Gold step is followed by PostgreSQL sync, including the existing Sunday run.
- A PostgreSQL outage never invalidates or rolls back already-published local Gold; manual `gold-sync-postgres` retry
  is sufficient after connectivity returns.
- No operational password, administrator credential, or credential-bearing DSN exists in Git history, tracked files,
  logs, persisted sync metadata, or test snapshots.
