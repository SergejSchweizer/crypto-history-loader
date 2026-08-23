-- PostgreSQL Gold sync least-privilege contract.
-- Passwords are intentionally absent. scripts/provision_postgres_sync_role.py binds
-- the application password as a runtime parameter and never renders it into SQL text.

-- Required role identity/attributes:
-- CREATE ROLE "crypto-loader" WITH
--   LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

CREATE SCHEMA IF NOT EXISTS "crypto_loader" AUTHORIZATION "crypto-loader";
CREATE SCHEMA IF NOT EXISTS "crypto_loader_sync" AUTHORIZATION "crypto-loader";

GRANT USAGE, CREATE ON SCHEMA "crypto_loader" TO "crypto-loader";
GRANT USAGE, CREATE ON SCHEMA "crypto_loader_sync" TO "crypto-loader";
