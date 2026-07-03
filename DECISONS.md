# Decisions

This file is generated from first-parent `git log` evidence.

Update command:

```bash
uv run python scripts/update_project_history_docs.py
```

Rules:

- Keep decisions tied to commits, not personal memory.
- Update this file in the same change set as architecture, dataset, or operational contract changes.
- The filename `DECISONS.md` follows the repository request; treat it as the canonical decisions ledger.

## D001. Use explicit medallion dataset contracts

Decision: Dataset identity, schema expectations, and medallion source requirements are treated as explicit contracts instead of implicit path or CLI conventions.

Consequence: New datasets need registry and contract updates before storage, Silver, or Gold code can safely depend on them.

Evidence:

- 2026-07-03 `91d7475` Rename perpetual OHLCV dataset
- 2026-06-27 `a44abc8` Extract dataset transformation contracts (#19)
- 2026-06-11 `514d528` Use full-history medallion start bounds
- 2026-05-25 `b8b5b82` Refine raw dataset docs and Deribit endpoint sections (#7)
- 2026-05-25 `3e96121` Refactor README and align dataset/CLI/runtime updates (#5)
- 2026-05-17 `32c3d28` Remove option instruments dataset, suppress heartbeat logs, and stabilize coverage

## D002. Keep Bronze orchestration registry-driven

Decision: Bronze fetching is coordinated through dataset task planning, checkpoint keys, and bounded runtime services rather than ad hoc command branches.

Consequence: Fetch behavior should be extended by adding dataset specs and task handlers, not by duplicating CLI scheduling logic.

Evidence:

- 2026-07-01 `4e203fb` [codex] Complete Bronze refactor stack (#51)
- 2026-06-29 `f008cf6` Extract loader parser registration (#43)
- 2026-06-29 `23082ec` Extract loader symbol fetch adapters
- 2026-06-28 `ecbc7e8` Extract Bronze checkpoint key helpers
- 2026-05-25 `3e96121` Refactor README and align dataset/CLI/runtime updates (#5)
- 2026-05-25 `6bd2781` chore: update loader naming, spot start dates, and CI checks

## D003. Isolate lake storage layout behind helpers

Decision: Lake partition paths, parquet reads and writes, and sidecar repair are owned by dedicated ingestion/application helpers.

Consequence: Callers should not assemble lake paths directly unless they are inside the storage layout boundary.

Evidence:

- 2026-07-01 `225a01e` Update README coverage statistics
- 2026-06-29 `a8b033d` Extract lake read helpers (#44)
- 2026-06-29 `ebbe10b` Extract lake dataframe reader
- 2026-06-29 `60fcfcb` Remove README missing-day detail label
- 2026-06-29 `0d1ce23` Fix README table of contents
- 2026-06-28 `660f9f2` Deduplicate README table of contents

## D004. Favor restart-safe backfills over shortest happy path

Decision: Historical loading prioritizes complete, resumable backfills with retries, deterministic start bounds, and explicit handling of route or exchange failures.

Consequence: Speed work must preserve checkpoint semantics, idempotent writes, and observable retry behavior.

Evidence:

- 2026-06-29 `7232cc4` Extract fetch head gap planning (#42)
- 2026-06-27 `deca10a` Route lake access through application services
- 2026-06-23 `ac5a7cf` Ensure complete trade backfill windows (#14)
- 2026-06-22 `3667041` Speed up trade backfill
- 2026-06-11 `514d528` Use full-history medallion start bounds
- 2026-06-11 `4fe87d7` Improve trade fetch reliability and parallel tests

## D005. Keep quality gates strict and local-first

Decision: Linting, formatting, typing, import boundaries, tests, and coverage are expected to run locally with the same intent as CI.

Consequence: Refactors should include tests and keep tooling strict instead of suppressing failures or weakening checks.

Evidence:

- 2026-07-01 `225a01e` Update README coverage statistics
- 2026-07-01 `a2287a1` Align architecture and coverage refactor gates
- 2026-06-27 `931263d` Align validation gates (#17)
- 2026-05-23 `7f21bf2` Improve project quality gates and config scripts (#2)
- 2026-05-17 `32c3d28` Remove option instruments dataset, suppress heartbeat logs, and stabilize coverage
- 2026-05-17 `174ec90` Add option instruments bronze ingestion, unify fetch logs, and raise coverage

## D006. Treat README and generated history docs as operational contracts

Decision: Repository documentation records current dataset coverage, command contracts, project decisions, risks, and chronology.

Consequence: Documentation updates belong in the same change set as behavior, dataset, or operational contract changes.

Evidence:

- 2026-07-01 `225a01e` Update README coverage statistics
- 2026-06-29 `60fcfcb` Remove README missing-day detail label
- 2026-06-29 `0d1ce23` Fix README table of contents
- 2026-06-28 `660f9f2` Deduplicate README table of contents
- 2026-06-26 `40cc90e` Merge branch 'codex/docs-update-missing-values'
- 2026-05-25 `b8b5b82` Refine raw dataset docs and Deribit endpoint sections (#7)
