"""Run a deterministic shard of the repository pytest suite."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def discover_test_files(test_root: Path) -> list[Path]:
    """Return repository test files in stable order."""

    return sorted(path for path in test_root.rglob("test_*.py") if path.is_file())


def select_shard(files: list[Path], *, shard_index: int, shard_count: int) -> list[Path]:
    """Select one 1-based deterministic shard from ``files``."""

    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    if shard_index < 1 or shard_index > shard_count:
        raise ValueError("shard_index must be between 1 and shard_count")
    return [path for index, path in enumerate(files) if index % shard_count == shard_index - 1]


def parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""

    command_parser = argparse.ArgumentParser(description=__doc__)
    command_parser.add_argument("--shard-index", type=int, required=True, help="1-based shard index to run")
    command_parser.add_argument("--shard-count", type=int, required=True, help="Total number of shards")
    command_parser.add_argument("--test-root", type=Path, default=Path("tests"), help="Root directory containing tests")
    command_parser.add_argument("pytest_args", nargs=argparse.REMAINDER, help="Arguments passed through to pytest")
    return command_parser


def main(argv: list[str] | None = None) -> int:
    """Run pytest for the selected test-file shard."""

    args = parser().parse_args(argv)
    pytest_args = list(args.pytest_args)
    if pytest_args and pytest_args[0] == "--":
        pytest_args = pytest_args[1:]

    files = discover_test_files(args.test_root)
    selected = select_shard(files, shard_index=args.shard_index, shard_count=args.shard_count)
    if not selected:
        print(f"No tests selected for shard {args.shard_index}/{args.shard_count}")
        return 0

    print(f"Running pytest shard {args.shard_index}/{args.shard_count}: {len(selected)} files")
    command = [sys.executable, "-m", "pytest", *pytest_args, *[str(path) for path in selected]]
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
