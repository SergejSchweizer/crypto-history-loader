# Risks

This file is generated from recurring themes in first-parent `git log`.

Update command:

```bash
uv run python scripts/update_project_history_docs.py
```

Risk review rules:

- Update risks when commits introduce or retire operational, data correctness, or architecture risks.
- Prefer concrete mitigations that map to tests, logs, contracts, or docs.
- Keep stale risks only if the mitigation still needs active attention.

## R001. Exchange API reliability can silently reduce historical completeness

Status: Active

Signal: Deribit route errors, retry behavior, and long-running trade backfills appear repeatedly in the history.

Mitigation: Keep debug logs, checkpoint keys, deterministic windows, and completeness reports aligned before changing fetch execution.

Evidence:

- 2026-07-10 `ccb6962` Merge pull request #72 from SergejSchweizer/codex/pr15-recent-trade-snapshot-silver
- 2026-07-04 `ca0e922` Rename option trades dataset to options_trades
- 2026-07-04 `11da15c` Rename options trades and perps OHLCV datasets
- 2026-07-03 `4393c40` Rename perpetual trades dataset
- 2026-07-01 `f55d766` [codex] Extract OHLCV symbol fetch planning (#46)
- 2026-06-29 `7232cc4` Extract fetch head gap planning (#42)

## R002. Dataset naming drift can break Bronze, Silver, and Gold joins

Status: Active

Signal: Dataset names have changed over time, including volatility cleanup and explicit OHLCV dataset naming.

Mitigation: Rename work must update registry specs, lake paths, contracts, CLI choices, manifests, tests, and docs in one change.

Evidence:

- 2026-07-10 `8902f6b` Merge pull request #77 from SergejSchweizer/codex/pr18-gold-regime-feature-contract
- 2026-07-09 `c43e3e3` Merge pull request #58 from SergejSchweizer/codex/pr01-silver-contract-registry-baseline
- 2026-07-04 `ca0e922` Rename option trades dataset to options_trades
- 2026-07-04 `ab5543d` Rename open_interest dataset to open interest
- 2026-07-04 `11da15c` Rename options trades and perps OHLCV datasets
- 2026-07-03 `c9d39e1` Rename spot_ohlcv OHLCV dataset

## R003. Large refactors can blur architecture boundaries

Status: Active

Signal: The log contains many extraction commits across loader, lake, Silver, and Gold services.

Mitigation: Keep dependency direction and side effects explicit; verify with architecture/import checks and focused regression tests.

Evidence:

- 2026-07-04 `ba93394` Add architecture documentation
- 2026-07-01 `4e203fb` [codex] Complete Bronze refactor stack (#51)
- 2026-07-01 `f55d766` [codex] Extract OHLCV symbol fetch planning (#46)
- 2026-07-01 `a2287a1` Align architecture and coverage refactor gates
- 2026-07-01 `a674417` Consolidate refactor boundary work
- 2026-06-29 `febd87e` Extract silver volatility transformation (#45)

## R004. Coverage and strict typing can drift after broad edits

Status: Active

Signal: Quality-gate commits show that type coverage and test coverage are active project risks.

Mitigation: Run focused tests first, then full pytest, Ruff, and type checks before merging behavior or boundary changes.

Evidence:

- 2026-07-10 `850150b` Add GitHub quality gate script
- 2026-07-10 `c0fca84` Sync stacked PR validation policy
- 2026-07-09 `2a57684` Extend volatility medallion coverage
- 2026-07-01 `225a01e` Update README coverage statistics
- 2026-07-01 `a2287a1` Align architecture and coverage refactor gates
- 2026-06-27 `931263d` Align validation gates (#17)

## R005. Documentation snapshots can become stale relative to the lake

Status: Active

Signal: README coverage statistics and missing-day details have been refreshed several times.

Mitigation: Regenerate or explicitly date coverage snapshots when lake content, dataset names, or coverage reporting changes.

Evidence:

- 2026-07-01 `225a01e` Update README coverage statistics
- 2026-06-29 `60fcfcb` Remove README missing-day detail label
- 2026-06-29 `0d1ce23` Fix README table of contents
- 2026-06-28 `660f9f2` Deduplicate README table of contents
- 2026-06-26 `40cc90e` Merge branch 'codex/docs-update-missing-values'
- 2026-05-25 `b8b5b82` Refine raw dataset docs and Deribit endpoint sections (#7)
