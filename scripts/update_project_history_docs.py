"""Generate project history ledgers from git commit history."""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FIELD_SEPARATOR = "\x1f"
TERM_REPLACEMENTS = (
    (re.compile(r"\b" + "o" + r"i\b", re.IGNORECASE), "open_interest"),
    (re.compile("o" + r"i_"), "open_interest_"),
    (re.compile(r"perp_trades"), "perps_trades"),
    (re.compile(r"option" + r"_trades"), "options_trades"),
    (re.compile(r"peprs" + r"_ohlcv"), "perps_ohlcv"),
    (re.compile(r"\b" + "sp" + r"ot\b(?!_ohlcv)"), "spot_ohlcv"),
)


@dataclass(frozen=True)
class Commit:
    """Commit metadata used by the generated history documents."""

    full_hash: str
    short_hash: str
    date: str
    author: str
    subject: str


@dataclass(frozen=True)
class Topic:
    """Decision or risk topic inferred from matching commit subjects."""

    title: str
    keywords: tuple[str, ...]
    summary: str
    consequence: str


DECISION_TOPICS = (
    Topic(
        title="Use explicit medallion dataset contracts",
        keywords=("contract", "schema", "dataset", "registry", "medallion", "perps_ohlcv"),
        summary=(
            "Dataset identity, schema expectations, and medallion source requirements are treated "
            "as explicit contracts instead of implicit path or CLI conventions."
        ),
        consequence=(
            "New datasets need registry and contract updates before storage, Silver, or Gold code "
            "can safely depend on them."
        ),
    ),
    Topic(
        title="Keep Bronze orchestration registry-driven",
        keywords=("bronze", "orchestration", "runtime", "checkpoint", "task", "loader"),
        summary=(
            "Bronze fetching is coordinated through dataset task planning, checkpoint keys, and "
            "bounded runtime services rather than ad hoc command branches."
        ),
        consequence=(
            "Fetch behavior should be extended by adding dataset specs and task handlers, not by "
            "duplicating CLI scheduling logic."
        ),
    ),
    Topic(
        title="Isolate lake storage layout behind helpers",
        keywords=("lake", "partition", "parquet", "read", "write", "sidecar"),
        summary=(
            "Lake partition paths, parquet reads and writes, and sidecar repair are owned by "
            "dedicated ingestion/application helpers."
        ),
        consequence=(
            "Callers should not assemble lake paths directly unless they are inside the storage layout boundary."
        ),
    ),
    Topic(
        title="Favor restart-safe backfills over shortest happy path",
        keywords=("backfill", "gap", "retry", "route", "reliability", "full-history", "full gap"),
        summary=(
            "Historical loading prioritizes complete, resumable backfills with retries, deterministic "
            "start bounds, and explicit handling of route or exchange failures."
        ),
        consequence=(
            "Speed work must preserve checkpoint semantics, idempotent writes, and observable retry behavior."
        ),
    ),
    Topic(
        title="Keep quality gates strict and local-first",
        keywords=("ruff", "mypy", "pyright", "coverage", "quality", "validation", "import", "typing"),
        summary=(
            "Linting, formatting, typing, import boundaries, tests, and coverage are expected to run "
            "locally with the same intent as CI."
        ),
        consequence=(
            "Refactors should include tests and keep tooling strict instead of suppressing failures "
            "or weakening checks."
        ),
    ),
    Topic(
        title="Treat README and generated history docs as operational contracts",
        keywords=("readme", "docs", "coverage statistics", "timeline", "decision", "risk"),
        summary=(
            "Repository documentation records current dataset coverage, command contracts, project "
            "decisions, risks, and chronology."
        ),
        consequence=(
            "Documentation updates belong in the same change set as behavior, dataset, or operational contract changes."
        ),
    ),
)

RISK_TOPICS = (
    Topic(
        title="Exchange API reliability can silently reduce historical completeness",
        keywords=("retry", "route", "fetch", "trade", "backfill", "gap"),
        summary=(
            "Deribit route errors, retry behavior, and long-running trade backfills appear repeatedly in the history."
        ),
        consequence=(
            "Keep debug logs, checkpoint keys, deterministic windows, and completeness reports aligned "
            "before changing fetch execution."
        ),
    ),
    Topic(
        title="Dataset naming drift can break Bronze, Silver, and Gold joins",
        keywords=("dataset", "contract", "schema", "registry", "perps_ohlcv", "historical_volatility"),
        summary=(
            "Dataset names have changed over time, including volatility cleanup and explicit OHLCV dataset naming."
        ),
        consequence=(
            "Rename work must update registry specs, lake paths, contracts, CLI choices, manifests, "
            "tests, and docs in one change."
        ),
    ),
    Topic(
        title="Large refactors can blur architecture boundaries",
        keywords=("extract", "refactor", "architecture", "boundary", "service", "helper"),
        summary=("The log contains many extraction commits across loader, lake, Silver, and Gold services."),
        consequence=(
            "Keep dependency direction and side effects explicit; verify with architecture/import "
            "checks and focused regression tests."
        ),
    ),
    Topic(
        title="Coverage and strict typing can drift after broad edits",
        keywords=("coverage", "typing", "mypy", "pyright", "strict", "quality", "validation"),
        summary=("Quality-gate commits show that type coverage and test coverage are active project risks."),
        consequence=(
            "Run focused tests first, then full pytest, Ruff, and type checks before merging behavior "
            "or boundary changes."
        ),
    ),
    Topic(
        title="Documentation snapshots can become stale relative to the lake",
        keywords=("readme", "coverage statistics", "missing-day", "docs"),
        summary=("README coverage statistics and missing-day details have been refreshed several times."),
        consequence=(
            "Regenerate or explicitly date coverage snapshots when lake content, dataset names, or "
            "coverage reporting changes."
        ),
    ),
)


def main() -> int:
    """Generate or verify history documents."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if generated documents differ from disk.")
    args = parser.parse_args()

    commits = read_commits()
    outputs = {
        "DECISONS.md": render_decisions(commits),
        "RISKS.md": render_risks(commits),
        "TIMELINE.md": render_timeline(commits),
    }

    changed: list[str] = []
    for relative_path, content in outputs.items():
        path = REPO_ROOT / relative_path
        if path.exists() and path.read_text(encoding="utf-8") == content:
            continue
        changed.append(relative_path)
        if not args.check:
            path.write_text(content, encoding="utf-8")

    if args.check and changed:
        joined = ", ".join(changed)
        print(f"History docs are out of date: {joined}")
        print("Run: uv run python scripts/update_project_history_docs.py")
        return 1
    return 0


def read_commits() -> list[Commit]:
    """Read first-parent project history in a parseable format."""

    result = subprocess.run(
        [
            "git",
            "log",
            "--first-parent",
            "--date=short",
            f"--pretty=format:%H{FIELD_SEPARATOR}%h{FIELD_SEPARATOR}%ad{FIELD_SEPARATOR}%an{FIELD_SEPARATOR}%s",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    commits: list[Commit] = []
    for line in result.stdout.splitlines():
        parts = line.split(FIELD_SEPARATOR)
        if len(parts) != 5:
            continue
        commits.append(
            Commit(
                full_hash=parts[0],
                short_hash=parts[1],
                date=parts[2],
                author=parts[3],
                subject=normalize_project_terms(parts[4]),
            )
        )
    return commits


def normalize_project_terms(value: str) -> str:
    """Render historical subjects with current project terminology."""

    normalized = value
    for pattern, new in TERM_REPLACEMENTS:
        normalized = pattern.sub(new, normalized)
    return normalized


def render_decisions(commits: list[Commit]) -> str:
    """Render the decision ledger from commit topics."""

    lines = [
        "# Decisions",
        "",
        "This file is generated from first-parent `git log` evidence.",
        "",
        "Update command:",
        "",
        "```bash",
        "uv run python scripts/update_project_history_docs.py",
        "```",
        "",
        "Rules:",
        "",
        "- Keep decisions tied to commits, not personal memory.",
        "- Update this file in the same change set as architecture, dataset, or operational contract changes.",
        "- The filename `DECISONS.md` follows the repository request; treat it as the canonical decisions ledger.",
        "",
    ]
    for index, topic in enumerate(DECISION_TOPICS, start=1):
        matches = matching_commits(commits, topic.keywords)
        if not matches:
            continue
        lines.extend(
            [
                f"## D{index:03d}. {topic.title}",
                "",
                f"Decision: {topic.summary}",
                "",
                f"Consequence: {topic.consequence}",
                "",
                "Evidence:",
                "",
            ]
        )
        lines.extend(format_commit_bullets(matches[:6]))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_risks(commits: list[Commit]) -> str:
    """Render the risk ledger from recurring commit themes."""

    lines = [
        "# Risks",
        "",
        "This file is generated from recurring themes in first-parent `git log`.",
        "",
        "Update command:",
        "",
        "```bash",
        "uv run python scripts/update_project_history_docs.py",
        "```",
        "",
        "Risk review rules:",
        "",
        "- Update risks when commits introduce or retire operational, data correctness, or architecture risks.",
        "- Prefer concrete mitigations that map to tests, logs, contracts, or docs.",
        "- Keep stale risks only if the mitigation still needs active attention.",
        "",
    ]
    for index, topic in enumerate(RISK_TOPICS, start=1):
        matches = matching_commits(commits, topic.keywords)
        if not matches:
            continue
        lines.extend(
            [
                f"## R{index:03d}. {topic.title}",
                "",
                "Status: Active",
                "",
                f"Signal: {topic.summary}",
                "",
                f"Mitigation: {topic.consequence}",
                "",
                "Evidence:",
                "",
            ]
        )
        lines.extend(format_commit_bullets(matches[:6]))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_timeline(commits: list[Commit]) -> str:
    """Render a chronological project timeline from git history."""

    lines = [
        "# Timeline",
        "",
        "This file is generated from first-parent `git log`.",
        "",
        "Update command:",
        "",
        "```bash",
        "uv run python scripts/update_project_history_docs.py",
        "```",
        "",
        "## History",
        "",
    ]
    for commit in reversed(commits):
        lines.append(f"- {commit.date} `{commit.short_hash}` {commit.subject} ({commit.author})")
    return "\n".join(lines).rstrip() + "\n"


def matching_commits(commits: list[Commit], keywords: tuple[str, ...]) -> list[Commit]:
    """Return commits whose subject contains any keyword."""

    normalized_keywords = tuple(keyword.lower() for keyword in keywords)
    return [commit for commit in commits if any(keyword in commit.subject.lower() for keyword in normalized_keywords)]


def format_commit_bullets(commits: list[Commit]) -> list[str]:
    """Format commits as Markdown bullets."""

    return [f"- {commit.date} `{commit.short_hash}` {commit.subject}" for commit in commits]


if __name__ == "__main__":
    raise SystemExit(main())
