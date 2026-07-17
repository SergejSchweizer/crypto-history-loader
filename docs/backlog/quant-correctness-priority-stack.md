# Quantitative Correctness Priority Stack

Last updated: 2026-07-17

## Purpose And Ordering

This backlog addendum records correctness blockers found during the 2026-07-17 repository review. These items take priority over the existing architecture-refactor sequence beginning with PR-47. The existing PR-47 through PR-53 stack should resume only after QC-01 through QC-03 are merged and affected Silver and Gold artifacts have a documented rebuild plan.

Priority order:

1. QC-01: Normalize implied- and realized-volatility semantics.
2. QC-02: Preserve rolling state across monthly partitions.
3. QC-03: Prevent row-wise spot/perpetual source switching in realized volatility.
4. QC-04: Add quantitative semantics to dataset contracts.
5. QC-05: Validate documented CLI commands as executable contracts.
6. QC-06: Align documented and enforced quality gates.
7. Resume the existing PR-47 through PR-53 architecture-refactor stack.

## QC-01: Normalize IV/RV Units, Horizons, And Annualization

Priority: P0 — data correctness blocker

Status: Planned

Suggested branch: `codex/qc01-normalize-iv-rv-semantics`

Depends on: none

### Problem

`volatility_index_1m_feature.iv_close` represents an annualized implied-volatility index in percentage points, while `realized_volatility_1m_feature.rv_1h` and `rv_1d` currently represent non-annualized square-root sums of squared decimal log returns. Direct subtraction and division therefore mix incompatible units and horizons.

Current expressions such as:

```text
iv_minus_rv_1h = iv_close - rv_1h
iv_minus_rv_1d = iv_close - rv_1d
iv_rv_ratio_1h = iv_close / rv_1h
iv_rv_ratio_1d = iv_close / rv_1d
```

do not have a stable financial interpretation until both sides use an explicitly compatible unit, horizon, and annualization convention.

### Goal

Make every IV/RV comparison financially interpretable and encode the convention in contracts, manifests, tests, and column names.

### Scope

- Define the canonical IV unit as annualized volatility percentage points.
- Define the canonical annualization basis explicitly, with crypto calendar-time defaulting to 365 days unless a contract states otherwise.
- Add annualized realized-volatility fields with explicit names, for example:
  - `rv_1h_annualized_pct`
  - `rv_1d_annualized_pct`
  - `rv_30d_annualized_pct`
- Prefer a horizon-compatible 30-day realized-volatility comparison for the volatility index:

```text
rv_30d_annualized_pct = sqrt(sum(last_30d_log_returns^2)) * sqrt(365 / 30) * 100
iv_rv_spread_30d_pct = iv_30d_annualized_pct - rv_30d_annualized_pct
iv_rv_ratio_30d = iv_30d_annualized_pct / rv_30d_annualized_pct
```

- Either remove ambiguous `iv_minus_rv_1h`, `iv_minus_rv_1d`, `iv_rv_ratio_1h`, and `iv_rv_ratio_1d` fields or version them with precisely documented semantics.
- Update Silver and Gold contracts, manifests, README tables, architecture documentation, and rebuild notes.
- Add a migration or compatibility policy for existing materialized artifacts.

### Out Of Scope

- Do not tune trading thresholds or model hyperparameters.
- Do not create forward-looking labels.
- Do not silently reinterpret existing persisted columns without a schema/version change.

### Acceptance

- Every IV and RV output declares unit, horizon, annualization status, annualization basis, and estimator.
- Unit tests use realistic decimal-return and volatility-index values rather than treating values such as `10.0` as a one-hour decimal RV.
- A deterministic reference test verifies annualization formulas against hand-calculated results.
- Division-by-zero and insufficient-history policies are explicit and tested.
- Gold feature-only datasets expose no mixed-unit IV/RV subtraction or ratio.
- A rebuild note identifies every affected Silver and Gold dataset.

## QC-02: Preserve Rolling State Across Monthly Partitions

Priority: P0 — data correctness blocker

Status: Planned

Suggested branch: `codex/qc02-cross-month-rolling-state`

Depends on: QC-01

### Problem

Rolling returns, realized volatility, z-scores, changes, and percentiles are calculated independently inside monthly processing loops. At the beginning of each month, the calculation loses the previous close and all required trailing observations. Storage partition boundaries therefore alter feature values.

Affected feature families include at least:

- `realized_volatility_1m_feature`
- `volatility_index_1m_feature`
- `iv_rv_1m_feature`

Potentially affected calculations include:

- first return of each month;
- `5m`, `15m`, `1h`, `4h`, and `1d` RV windows;
- `1d` and `7d` z-scores;
- `30d` percentiles;
- IV change windows;
- jump proxies and any downstream Gold rolling features.

### Goal

Make feature values invariant to monthly storage partitioning.

### Scope

- Declare the maximum required lookback for each builder or feature family.
- Load sufficient prior-partition context before calculating a target month.
- Calculate on the buffered frame, then trim output back to the requested target month.
- Preserve the previous valid close across month and year boundaries.
- Centralize buffered monthly reads in a reusable helper rather than implementing one-off overlap logic in each builder.
- Keep writes monthly and deterministic.
- Add manifest metadata recording the calculation lookback used.

Recommended processing pattern:

```text
calculation_start = target_month_start - required_lookback
output_start = target_month_start
output_end = target_month_end
calculate on [calculation_start, output_end]
write only [output_start, output_end]
```

### Out Of Scope

- Do not change partition layout.
- Do not add future observations or centered windows.
- Do not use forward fills that cross source-availability rules.

### Acceptance

- Regression tests cover January 31 to February 1 and December 31 to January 1.
- Building one long unpartitioned fixture and building the same fixture month-by-month produce equal values for all target-month rows.
- The first valid minute of a month uses the final valid prior-month close when the contract permits it.
- A `30d` percentile on the first day of a month includes eligible prior-month observations.
- Re-running a month with unchanged inputs produces byte-stable or value-stable deterministic output according to the existing storage contract.
- Affected historical artifacts are explicitly marked for rebuild.

## QC-03: Separate Spot And Perpetual RV Sources

Priority: P0 — data correctness blocker

Status: Planned

Suggested branch: `codex/qc03-separate-spot-perp-rv`

Depends on: QC-02

### Problem

The current realized-volatility builder creates one synthetic price stream by coalescing perpetual OHLCV over spot OHLCV row by row. If a perpetual minute is absent, the stream can switch to spot and then switch back. The spot-perpetual basis is then misclassified as a price return, contaminating RV and jump features.

### Goal

Prevent source switching from producing artificial returns while retaining explicit source availability.

### Scope

- Calculate spot and perpetual returns and RV features as separate source families.
- Prefer explicit fields such as:
  - `spot_rv_1h`, `spot_rv_1d`, `spot_rv_30d_annualized_pct`
  - `perps_rv_1h`, `perps_rv_1d`, `perps_rv_30d_annualized_pct`
- Define one canonical IV/RV comparison source, preferably through an explicit contract or configuration rather than row-wise fallback.
- If a stitched canonical series remains necessary, require an explicit basis-adjusted stitching method and emit source-transition flags.
- Preserve `spot_available` and `perps_available`, and add source identity fields where a canonical RV is published.
- Add data-quality counters for source gaps and attempted source transitions.

### Out Of Scope

- Do not hide missing perpetual data by silently substituting spot.
- Do not treat the spot-perpetual basis as ordinary underlying return.
- Do not remove either source family from research outputs.

### Acceptance

- A regression fixture with alternating spot/perpetual availability produces no artificial basis jump in either source-specific return series.
- Source-specific RV values match independent hand calculations.
- Canonical IV/RV features declare which RV source is used.
- Missing canonical-source observations remain explicitly unavailable unless a documented stitching policy applies.
- Gold manifests report source availability and the canonical RV source policy.

## QC-04: Extend Dataset Contracts With Quantitative Semantics

Priority: P1 — contract integrity

Status: Planned

Suggested branch: `codex/qc04-quant-semantic-contracts`

Depends on: QC-01 through QC-03

### Goal

Extend typed dataset contracts beyond column shape so economically meaningful fields cannot be combined without explicit semantic metadata.

### Scope

For quantitative feature fields or feature families, add typed metadata for:

- unit: decimal, percentage points, price, quantity, notional, count, or dimensionless;
- horizon or tenor;
- annualized flag;
- annualization basis;
- estimator or construction method;
- required lookback;
- source-selection policy;
- null and insufficient-history policy.

Add contract tests proving IV/RV comparisons use compatible semantics. Emit relevant metadata into Silver and Gold manifests.

### Acceptance

- A contract test fails when an IV/RV spread attempts to combine incompatible units or horizons.
- All volatility feature families declare their estimator, horizon, unit, and annualization convention.
- Required lookbacks are machine-readable and used by buffered partition reads.
- Documentation is generated from or validated against the canonical contracts where practical.

## QC-05: Validate Documentation Commands As Executable Contracts

Priority: P1 — operational correctness

Status: Planned

Suggested branch: `codex/qc05-executable-doc-commands`

Depends on: none

### Problem

README examples can drift from the actual parser surface. A documented Bronze command currently uses a stale argument name while the parser exposes `--dataset`.

### Scope

- Correct stale README command examples.
- Extract or represent canonical example argument vectors in a testable form.
- Add parser-level tests proving documented commands are accepted.
- Keep examples synchronized with dataset registry choices and configuration aliases.

### Acceptance

- Every canonical README command parses successfully without network or lake access.
- CI fails when a documented flag or dataset choice is removed without updating documentation.
- README, `config.yaml`, and parser choices use the same canonical vocabulary.

## QC-06: Align Documented And Enforced Quality Gates

Priority: P2 — governance consistency

Status: Planned

Suggested branch: `codex/qc06-align-quality-gates`

Depends on: none

### Problem

`AGENTS.md`, architecture documentation, pre-commit configuration, and GitHub Actions do not currently describe exactly the same quality-gate suite. In particular, documented docstring tools must either be enforced or removed from the mandatory policy.

### Scope

- Inventory mandatory checks declared by `AGENTS.md`, `ARCHITECTURE.md`, `.pre-commit-config.yaml`, `Makefile`, and `.github/workflows/ci.yml`.
- Choose one canonical enforced suite.
- Add missing tools such as `interrogate` and `pydoclint` only if they are intentionally mandatory.
- Otherwise revise policy text so it matches the enforced suite.
- Add a lightweight consistency test for named mandatory checks where practical.

### Acceptance

- Local `make check`, pre-commit, and CI perform the same logical mandatory checks.
- No tool is described as mandatory without being enforced.
- CI remains the final merge-readiness authority.

## Required Rebuild And Release Gate

QC-01 through QC-03 change historical feature values and require a controlled rebuild. Before downstream research or model training treats the outputs as corrected:

- rebuild affected Silver volatility datasets;
- rebuild dependent Gold datasets;
- publish schema or feature-set version changes;
- compare old and corrected distributions;
- document expected discontinuities;
- verify no reusable feature dataset contains forward-looking labels;
- record the effective corrected-data start/version in manifests.

The existing architecture-refactor stack may proceed in parallel only where it cannot change, obscure, or lock in the affected quantitative behavior. Data correctness takes precedence over structural cleanup.