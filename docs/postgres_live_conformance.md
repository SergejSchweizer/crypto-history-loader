# PostgreSQL Live Conformance

Run the read-only verifier after certified current Gold artifacts exist:

```bash
.venv/bin/python main.py postgres-live-conformance
```

It writes `artifacts/acceptance/postgres-live-conformance-v2.json`. The report is
sanitized and contains only endpoint identity, check names, and stable failure
categories; it never includes a password, DSN, or market rows.

`FAIL` is valid pre-reconstruction evidence. The command never commits a data
mutation: its UTC/DML/DDL permission probes run in one transaction and are
rolled back. A successful report requires exact current-Gold catalog, rows,
logical keys, deterministic digests, checkpoints, UTC precision, runtime-role,
privilege, and timeout conformance.
