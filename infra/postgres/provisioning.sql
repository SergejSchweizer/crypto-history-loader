-- PostgreSQL Gold sync least-privilege contract.
-- Passwords are intentionally absent. scripts/provision_postgres_sync_role.py binds
-- the application password as a runtime parameter and never renders it into SQL text.

-- Required role identity/attributes:
-- CREATE ROLE "crypto-history-loader" WITH
--   LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

CREATE SCHEMA IF NOT EXISTS "crypto_history_gold" AUTHORIZATION "crypto-history-loader";
CREATE SCHEMA IF NOT EXISTS "crypto_history_sync" AUTHORIZATION "crypto-history-loader";

GRANT USAGE, CREATE ON SCHEMA "crypto_history_gold" TO "crypto-history-loader";
GRANT USAGE, CREATE ON SCHEMA "crypto_history_sync" TO "crypto-history-loader";
