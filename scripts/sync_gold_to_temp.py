"""Mirror the current Gold lake state to the NAS staging path."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from application.services.runtime_service import SingleInstanceError, SingleInstanceLock
from scripts.logging_utils import configure_logger


@dataclass(frozen=True)
class SyncReport:
    """Summary of one Gold lake mirror run."""

    source_root: str
    destination_root: str
    copied_files: int
    skipped_files: int
    deleted_files: int

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-friendly payload for logging or callers."""

        return {
            "source_root": self.source_root,
            "destination_root": self.destination_root,
            "copied_files": self.copied_files,
            "skipped_files": self.skipped_files,
            "deleted_files": self.deleted_files,
        }


def _default_repo_root() -> Path:
    """Return repository root inferred from this script location."""

    return Path(__file__).resolve().parents[1]


def _default_config_path(repo_root: Path) -> Path:
    """Return the most appropriate runtime config path for cron execution."""

    runtime_config = repo_root / ".run" / "cron-config.yaml"
    if runtime_config.exists():
        return runtime_config
    return repo_root / "config.yaml"


def _resolve_path(path_value: str, *, repo_root: Path) -> Path:
    """Resolve a configured path relative to the repository root when needed."""

    path = Path(path_value.strip()).expanduser()
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _is_nested_path(*, parent: Path, child: Path) -> bool:
    """Return whether one path would recursively contain the other."""

    return parent == child or parent in child.parents or child in parent.parents


def _copy_file(source_file: Path, destination_file: Path) -> None:
    """Copy one file while preserving metadata."""

    destination_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_file, destination_file)


def mirror_gold_tree(*, source_root: Path, destination_root: Path, logger: logging.Logger) -> SyncReport:
    """Mirror the source Gold lake into the destination path.

    Parameters:
        source_root: Canonical Gold lake root to mirror.
        destination_root: Local NAS path that should become a current-state copy.
        logger: Logger used for progress and diagnostics.

    Returns:
        A summary of copied, skipped, and deleted files.

    Raises:
        FileNotFoundError: If the source root does not exist.
        NotADirectoryError: If the source root is not a directory.
        ValueError: If the destination would recurse into the source tree.
    """

    if not source_root.exists():
        raise FileNotFoundError(f"gold source root does not exist: {source_root}")
    if not source_root.is_dir():
        raise NotADirectoryError(f"gold source root must be a directory: {source_root}")
    if _is_nested_path(parent=source_root.resolve(), child=destination_root.resolve()):
        raise ValueError("destination root must not be the source root or a subdirectory of it")

    destination_root.mkdir(parents=True, exist_ok=True)
    source_files: set[Path] = set()
    source_dirs: set[Path] = {Path(".")}
    copied_files = 0
    skipped_files = 0
    deleted_files = 0

    for source_file in source_root.rglob("*"):
        if not source_file.is_file():
            continue
        relative_file = source_file.relative_to(source_root)
        source_files.add(relative_file)
        parent = relative_file.parent
        while True:
            source_dirs.add(parent)
            if parent == Path("."):
                break
            parent = parent.parent

        destination_file = destination_root / relative_file
        if destination_file.exists():
            source_stat = source_file.stat()
            destination_stat = destination_file.stat()
            if (
                source_stat.st_size == destination_stat.st_size
                and source_stat.st_mtime_ns == destination_stat.st_mtime_ns
            ):
                skipped_files += 1
                continue
        _copy_file(source_file, destination_file)
        copied_files += 1

    destination_entries = sorted(destination_root.rglob("*"), key=lambda path: len(path.parts), reverse=True)
    for destination_entry in destination_entries:
        relative_entry = destination_entry.relative_to(destination_root)
        if destination_entry.is_file() or destination_entry.is_symlink():
            if relative_entry not in source_files:
                destination_entry.unlink()
                deleted_files += 1
            continue
        if relative_entry not in source_dirs and not any(destination_entry.iterdir()):
            destination_entry.rmdir()
            deleted_files += 1

    logger.info(
        "Gold sync completed source_root=%s destination_root=%s copied_files=%s skipped_files=%s deleted_files=%s",
        source_root,
        destination_root,
        copied_files,
        skipped_files,
        deleted_files,
    )
    return SyncReport(
        source_root=str(source_root),
        destination_root=str(destination_root),
        copied_files=copied_files,
        skipped_files=skipped_files,
        deleted_files=deleted_files,
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the Gold mirror job."""

    repo_root = _default_repo_root()
    parser = argparse.ArgumentParser(description="Mirror the current Gold lake to /volume1/Temp/gold")
    parser.add_argument("--repo-root", default=str(repo_root), help="Repository root path")
    parser.add_argument(
        "--config",
        default=str(_default_config_path(repo_root)),
        help="Path to config.yaml or cron-config.yaml for shared logging settings",
    )
    parser.add_argument("--source-root", default="lake/gold", help="Canonical Gold lake root")
    parser.add_argument(
        "--destination-root",
        default="/volume1/Temp/gold",
        help="Mirror destination root, typically on the NAS volume",
    )
    parser.add_argument("--lock-file", default=str(repo_root / ".run" / "sync-gold-to-temp.lock"))
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    return parser.parse_args()


def main() -> int:
    """Run the Gold lake mirror job."""

    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    config_path = Path(args.config).resolve()
    source_root = _resolve_path(str(args.source_root), repo_root=repo_root)
    destination_root = _resolve_path(str(args.destination_root), repo_root=repo_root)
    lock_file = Path(args.lock_file).resolve()
    logger = configure_logger("sync-gold-to-temp", config_path)
    logger.setLevel(logging.DEBUG if bool(args.debug) else logging.INFO)

    logger.info(
        "Gold sync start source_root=%s destination_root=%s lock_file=%s",
        source_root,
        destination_root,
        lock_file,
    )

    try:
        with SingleInstanceLock(str(lock_file)):
            report = mirror_gold_tree(source_root=source_root, destination_root=destination_root, logger=logger)
    except SingleInstanceError:
        logger.warning("Gold sync skipped because another sync job is already running")
        return 1

    logger.info("Gold sync report=%s", report.to_dict())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
