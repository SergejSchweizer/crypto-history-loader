# PostgreSQL Gold Sync Runbook

## Security boundary

`scripts/provision_postgres_sync_role.py` is the only operational entrypoint that
creates or migrates the `crypto_loader` and `crypto_loader_sync` schemas and their
tables. Run it with a PostgreSQL administrator identity. The schemas and tables
remain owned by that administrator.

Normal `gold-sync-postgres` execution connects only as `crypto-loader`. It validates
the PR-85 catalog metadata and performs DML; it never creates, alters, or drops a
schema or table.

The runtime role is `LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
NOREPLICATION NOBYPASSRLS`. It has:

- `USAGE` on `crypto_loader` and `crypto_loader_sync`, without `CREATE`.
- `SELECT, INSERT, UPDATE, DELETE` on registered consumer tables.
- `SELECT, INSERT, UPDATE, DELETE` on `crypto_loader_sync.gold_row_hashes`.
- `SELECT, INSERT, UPDATE` on `crypto_loader_sync.gold_sync_state`.

No runtime role membership or `PUBLIC` table grant is permitted in the two owned
schemas.

## Administrator bootstrap

Export the protected settings in the administrator's process environment:

```bash
export PGHOST=10.10.1.3
export PGPORT=54321
export PGDATABASE=market_data
export PGADMINUSER=postgres-admin
export PGADMINPASSWORD
export PGPASSWORD
export GOLD_ROOT=/absolute/path/to/lake/gold
python scripts/provision_postgres_sync_role.py
```

`PGADMINPASSWORD` authenticates the administrator connection. `PGPASSWORD` is the
runtime role password to install; neither value is accepted as a command argument,
rendered in logs, or stored in migration metadata. `GOLD_ROOT` must contain the
certified current registered Gold artifacts used to generate PR-85 bootstrap
metadata.

Provisioning is transactional and idempotent. It transfers legacy schemas owned by
`crypto-loader` to the connected administrator, applies deterministic table DDL,
replaces runtime grants, and reads the role, ownership, and grants back from the
catalog before commit. Unexpected ownership, role membership, or grants fail closed.

Run administrator provisioning after a registered Gold schema change and before
starting runtime synchronization. A runtime catalog mismatch is a migration-required
error; do not grant `CREATE` or move DDL into `gold-sync-postgres` as a workaround.

## Permission verification

The real permission probes require isolated test credentials:

```bash
export TEST_POSTGRES_ADMIN_DSN
export TEST_POSTGRES_RUNTIME_DSN
python -m pytest --no-cov -q tests/test_postgres_runtime_privileges.py
```

The runtime DSN must authenticate exactly as `crypto-loader`. The probes create an
administrator-owned temporary consumer table, verify runtime insert/update/select/
delete, prove runtime create/alter/drop failures, and remove the probe table. They
also read back exact schema ownership, role attributes, and table grants.