# Decisions

This file is generated from first-parent `git log` evidence.

Update command:

```bash
uv run python scripts/update_project_history_docs.py
```

Rules:

- Keep decisions tied to commits, not personal memory.
- Update this file in the same change set as architecture, dataset, or operational contract changes.
- The filename `DECISIONS.md` is the canonical decisions ledger.

## D001. Use explicit medallion dataset contracts

Decision: Dataset identity, schema expectations, and medallion source requirements are treated as explicit contracts instead of implicit path or CLI conventions.

Consequence: New datasets need registry and contract updates before storage, Silver, or Gold code can safely depend on them.

Evidence:

- 2026-07-10 `8902f6b` Merge pull request #77 from SergejSchweizer/codex/pr18-gold-regime-feature-contract
- 2026-07-09 `c43e3e3` Merge pull request #58 from SergejSchweizer/codex/pr01-silver-contract-registry-baseline
- 2026-07-09 `2a57684` Extend volatility medallion coverage
- 2026-07-04 `ca0e922` Rename option trades dataset to options_trades
- 2026-07-04 `ab5543d` Rename open_interest dataset to open interest
- 2026-07-04 `11da15c` Rename options trades and perps OHLCV datasets

## D002. Keep Bronze orchestration registry-driven

Decision: Bronze fetching is coordinated through dataset task planning, checkpoint keys, and bounded runtime services rather than ad hoc command branches.

Consequence: Fetch behavior should be extended by adding dataset specs and task handlers, not by duplicating CLI scheduling logic.

Evidence:

- 2026-07-01 `4e203fb` [codex] Complete Bronze refactor stack (#51)
- 2026-06-29 `f008cf6` Extract loader parser registration (#43)
- 2026-06-29 `23082ec` Extract loader symbol fetch adapters
- 2026-06-28 `ecbc7e8` Extract Bronze checkpoint key helpers
- 2026-05-25 `3e96121` Refactor README and align dataset/CLI/runtime updates (#5)
- 2026-05-25 `6bd2781` chore: update loader naming, spot_ohlcv start dates, and CI checks

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

- 2026-07-10 `850150b` Add GitHub quality gate script
- 2026-07-10 `c0fca84` Sync stacked PR validation policy
- 2026-07-09 `2a57684` Extend volatility medallion coverage
- 2026-07-01 `225a01e` Update README coverage statistics
- 2026-07-01 `a2287a1` Align architecture and coverage refactor gates
- 2026-06-27 `931263d` Align validation gates (#17)

## D006. Treat README and generated history docs as operational contracts

Decision: Repository documentation records current dataset coverage, command contracts, project decisions, risks, and chronology.

Consequence: Documentation updates belong in the same change set as behavior, dataset, or operational contract changes.

Evidence:

- 2026-07-09 `45fd589` Rename decisions ledger
- 2026-07-01 `225a01e` Update README coverage statistics
- 2026-06-29 `60fcfcb` Remove README missing-day detail label
- 2026-06-29 `0d1ce23` Fix README table of contents
- 2026-06-28 `660f9f2` Deduplicate README table of contents
- 2026-06-26 `40cc90e` Merge branch 'codex/docs-update-missing-values'
