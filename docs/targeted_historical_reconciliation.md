# Targeted Historical Reconciliation Runbook

This command is the only PR-100 source-recovery entrypoint. It consumes a saved PR-99
`historical-lake-completeness-v1` report and derives the source mutation set from its
`RECONCILE_REQUIRED` intervals. Operators must not edit the report or expand intervals.

## Preconditions

1. Stop scheduled Bronze, Silver, Gold, and PostgreSQL publication jobs and acquire the host pipeline lock.
2. Save the latest PR-99 report outside the lake and verify its provenance.
3. Configure an operator adapter implementing `ReconciliationAdapter`. The adapter must use existing PR-93 through PR-98 source validators, verified partition backups, dependency metadata for Silver lookback, PR-89 Gold publication, and PR-90 inventory validation.
4. Choose state and report paths under the configured `.logs` root. These files contain identifiers and statuses only.

## Execute

```bash
python main.py --debug historical-reconcile \
  --pr99-report artifacts/acceptance/historical-lake-completeness-v1.json \
  --state-file .logs/historical-reconciliation-state-v1.json \
  --report-file .logs/historical-reconciliation-report-v1.json \
  --adapter-factory operator_reconciliation:build_adapter
```

The adapter factory receives keyword arguments `args`, `config`, and `logger`. Source
reload is forbidden when the input report is `PASS`. Gold certification-only republish
remains required for serving-eligible current artifacts that lack PR-89 attestation; it
must read existing Silver/Gold inputs and must not call a provider.

## Guard Order

For a non-PASS report the command snapshots unaffected Bronze bytes, creates and verifies
affected partition/manifest backups, reloads and validates each exact interval, then proves
bytes outside those intervals are unchanged. It next rebuilds dependency-reachable Silver
with required lookback, rebuilds affected Gold, and certification-only republishes current
legacy Gold. Finally it reruns PR-99, Gold freshness/input fingerprints, and PR-90 inventory.

## Completion And Recovery

Only a terminal report with `status: PASS` and `downstream_blocked: false` authorizes the
PR-101 live PostgreSQL verifier. `FAIL`, a missing report, `NOT_RUN`, an uncertified serving
lineage, or an interrupted `RUNNING` state blocks PR-101 and PR-102. Preserve backups and
the failed state/report, correct the adapter or source failure, rerun PR-99, and restart
this command from the original immutable PR-99 evidence. This command performs no
PostgreSQL DDL, DML, reconstruction, or cleanup.