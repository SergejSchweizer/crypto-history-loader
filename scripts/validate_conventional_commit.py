"""Validate Conventional Commit subjects for local hooks and CI."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ALLOWED_TYPES: tuple[str, ...] = (
    "build",
    "chore",
    "ci",
    "docs",
    "feat",
    "fix",
    "perf",
    "refactor",
    "revert",
    "style",
    "test",
)
CONVENTIONAL_COMMIT_RE = re.compile(rf"^({'|'.join(ALLOWED_TYPES)})(\([a-z0-9][a-z0-9._-]*\))?!?: .+")


def validate_subject(subject: str) -> str | None:
    """Return an error message when a commit subject is not Conventional Commit compliant.

    Args:
        subject: First line of a commit message, PR title, or squash commit title.

    Returns:
        ``None`` when valid, otherwise a human-readable validation error.
    """

    normalized = subject.strip()
    if not normalized:
        return "commit subject must not be empty"
    if "\n" in normalized:
        normalized = normalized.splitlines()[0].strip()
    if CONVENTIONAL_COMMIT_RE.fullmatch(normalized):
        return None
    allowed = ", ".join(ALLOWED_TYPES)
    return (
        "commit subject must follow Conventional Commits: "
        "<type>[optional-scope][!]: <description>. "
        f"Allowed types: {allowed}. Received: {normalized!r}"
    )


def _subjects_from_range(commit_range: str) -> list[str]:
    completed = subprocess.run(
        ["git", "log", "--format=%s", commit_range],
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _latest_subject() -> str:
    completed = subprocess.run(
        ["git", "log", "-1", "--format=%s"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _message_file_subject(path: Path) -> str:
    return path.read_text(encoding="utf-8").splitlines()[0].strip()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--message-file", type=Path, help="Commit message file passed by a commit-msg hook.")
    source.add_argument("--title", help="Single PR title or squash commit title to validate.")
    source.add_argument("--range", dest="commit_range", help="Git revision range whose commit subjects are checked.")
    source.add_argument("--latest", action="store_true", help="Validate the latest local commit subject.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the Conventional Commit validator."""

    args = _parser().parse_args(argv)
    if args.message_file is not None:
        subjects = [_message_file_subject(args.message_file)]
    elif args.title is not None:
        subjects = [args.title]
    elif args.commit_range is not None:
        subjects = _subjects_from_range(args.commit_range)
    else:
        subjects = [_latest_subject()]

    failures = [error for subject in subjects if (error := validate_subject(subject)) is not None]
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
