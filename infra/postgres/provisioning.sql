-- PostgreSQL Gold sync administrator bootstrap contract.
-- Run only as the schema-owning administrator. Consumer tables are generated from
-- PR-85 bootstrap metadata by scripts/provision_postgres_sync_role.py.

-- Required role identity/attributes:
-- CREATE ROLE "crypto-loader" WITH
--   LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

CREATE SCHEMA IF NOT EXISTS "crypto_loader" AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS "crypto_loader_sync" AUTHORIZATION CURRENT_USER;

REVOKE ALL ON SCHEMA "crypto_loader" FROM "crypto-loader";
REVOKE ALL ON SCHEMA "crypto_loader_sync" FROM "crypto-loader";
REVOKE CREATE ON SCHEMA "crypto_loader" FROM PUBLIC;
REVOKE CREATE ON SCHEMA "crypto_loader_sync" FROM PUBLIC;
GRANT USAGE ON SCHEMA "crypto_loader" TO "crypto-loader";
GRANT USAGE ON SCHEMA "crypto_loader_sync" TO "crypto-loader";

-- Exact per-table grants are applied after administrator CREATE TABLE migrations:
-- consumer tables: SELECT, INSERT, UPDATE, DELETE
-- crypto_loader_sync.gold_row_hashes: SELECT, INSERT, UPDATE, DELETE
-- crypto_loader_sync.gold_sync_state: SELECT, INSERT, UPDATE
