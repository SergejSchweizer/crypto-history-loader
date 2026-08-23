"""Runtime helpers for CLI locking, logging, and environment tuning."""

from __future__ import annotations

import fcntl
import gzip
import logging
import os
import re
import shutil
from datetime import UTC, date, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

from application.services.fetch_runtime_policy import (
    DEFAULT_FETCH_CONCURRENCY as _DEFAULT_FETCH_CONCURRENCY,
)
from application.services.fetch_runtime_policy import (
    MAX_FETCH_CONCURRENCY as _MAX_FETCH_CONCURRENCY,
)
from application.services.fetch_runtime_policy import (
    fetch_concurrency as _fetch_concurrency,
)

LOGGER_NAME = "crypto_loader"
DEFAULT_LOG_DIR = ".logs"
DEFAULT_LOG_FILE = "crypto-loader.log"
DEFAULT_FETCH_CONCURRENCY = _DEFAULT_FETCH_CONCURRENCY
MAX_FETCH_CONCURRENCY = _MAX_FETCH_CONCURRENCY
LOG_PLAIN_DAILY_FILES = 5
LOG_ARCHIVE_RETENTION_DAYS = 90
POLARS_MAX_THREADS = 4

_ROTATED_LOG_RE = re.compile(r"^(?P<base>.+\.log)\.(?P<date>\d{4}-\d{2}-\d{2})(?:\.(?P<time>\d{6}))?(?P<gzip>\.gz)?$")


def _rotated_log_sort_key(path: Path) -> tuple[date, str]:
    match = _ROTATED_LOG_RE.match(path.name)
    if match is None:
        return (date.min, path.name)
    try:
        rotated_date = date.fromisoformat(match.group("date"))
    except ValueError:
        return (date.min, path.name)
    return (rotated_date, path.name)


def _gzip_log_file(path: Path) -> Path:
    archive_path = path.with_name(f"{path.name}.gz")
    if archive_path.exists():
        path.unlink(missing_ok=True)
        return archive_path
    source_stat = path.stat()
    with path.open("rb") as source, gzip.open(archive_path, "wb") as archive:
        shutil.copyfileobj(source, archive)
    os.utime(archive_path, (source_stat.st_atime, source_stat.st_mtime))
    path.unlink(missing_ok=True)
    return archive_path


def enforce_log_retention(
    log_path: Path,
    *,
    plain_daily_files: int = LOG_PLAIN_DAILY_FILES,
    archive_retention_days: int = LOG_ARCHIVE_RETENTION_DAYS,
    today: date | None = None,
) -> None:
    """Keep recent daily logs plain, gzip older logs, and delete stale archives."""

    if plain_daily_files < 0:
        raise ValueError("plain_daily_files must be greater than or equal to 0")
    if archive_retention_days < 1:
        raise ValueError("archive_retention_days must be greater than 0")

    log_dir = log_path.parent
    if not log_dir.exists():
        return

    current_date = today or datetime.now(UTC).date()
    cutoff = current_date.toordinal() - archive_retention_days
    plain_rotations: list[Path] = []
    archives: list[Path] = []
    for candidate in log_dir.iterdir():
        match = _ROTATED_LOG_RE.match(candidate.name)
        if match is None or match.group("base") != log_path.name:
            continue
        if match.group("gzip"):
            archives.append(candidate)
        else:
            plain_rotations.append(candidate)

    plain_rotations.sort(key=_rotated_log_sort_key, reverse=True)
    archives.extend(_gzip_log_file(path) for path in plain_rotations[plain_daily_files:])
    for archive in archives:
        match = _ROTATED_LOG_RE.match(archive.name)
        if match is None:
            continue
        try:
            archive_date = date.fromisoformat(match.group("date"))
        except ValueError:
            continue
        if archive_date.toordinal() <= cutoff:
            archive.unlink(missing_ok=True)


class RetentionTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Timed rotating file handler that applies repository log retention after rollover."""

    def __init__(
        self,
        *args: Any,
        plain_daily_files: int = LOG_PLAIN_DAILY_FILES,
        archive_retention_days: int = LOG_ARCHIVE_RETENTION_DAYS,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._plain_daily_files = plain_daily_files
        self._archive_retention_days = archive_retention_days

    def doRollover(self) -> None:  # noqa: N802
        """Rotate the active log and apply daily plain/archive retention."""

        super().doRollover()
        enforce_log_retention(
            Path(self.baseFilename),
            plain_daily_files=self._plain_daily_files,
            archive_retention_days=self._archive_retention_days,
        )


def load_env_file(path: str = ".env") -> None:
    """Load simple KEY=VALUE pairs from a local environment file."""

    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = _strip_env_quotes(value.strip())


def _strip_env_quotes(value: str) -> str:
    """Remove matching single or double quotes from an env value."""

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment variable with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    """Read a float environment variable with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_list(name: str, default: list[str]) -> list[str]:
    """Read a whitespace or comma-delimited string list from the environment."""

    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.replace(",", " ")
    values = [item.strip() for item in normalized.split() if item.strip()]
    return values or default


def env_str(name: str, default: str) -> str:
    """Read a string environment variable with fallback."""

    return os.getenv(name, default)


def apply_repository_runtime_limits() -> None:
    """Clamp runtime parallelism to the repository-wide Polars ceiling.

    Polars initializes its thread pool on first use, so this must run before
    any Polars import or operation in the current process. The repository-wide
    ceiling keeps batch jobs predictable and prevents local shell defaults from
    silently expanding worker fan-out beyond the intended four cores.
    """

    raw_threads = os.getenv("POLARS_MAX_THREADS")
    if raw_threads is None:
        os.environ["POLARS_MAX_THREADS"] = str(POLARS_MAX_THREADS)
        return

    try:
        configured_threads = int(raw_threads)
    except ValueError:
        os.environ["POLARS_MAX_THREADS"] = str(POLARS_MAX_THREADS)
        return

    os.environ["POLARS_MAX_THREADS"] = str(min(configured_threads, POLARS_MAX_THREADS))


class SingleInstanceError(RuntimeError):
    """Raised when another CLI instance is already running."""


class SingleInstanceLock:
    """Non-blocking process lock backed by a lock file."""

    def __init__(self, lock_path: str) -> None:
        self.lock_path = Path(lock_path)
        self._fd: int | None = None

    def __enter__(self) -> SingleInstanceLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = None
            raise SingleInstanceError("Another crypto-loader instance is already running. Exiting.") from exc
        os.ftruncate(self._fd, 0)
        os.write(self._fd, str(os.getpid()).encode("utf-8"))
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None


def _safe_log_module_name(module_name: str) -> str:
    """Return a filesystem-safe log module name."""

    normalized = module_name.strip().replace("/", "-").replace("\\", "-")
    return normalized or "crypto-loader"


def configure_logging(module_name: str = "crypto-loader", *, debug: bool = False) -> logging.Logger:
    """Configure process logging with daily rotation and repository retention."""

    safe_module_name = _safe_log_module_name(module_name)
    logger = logging.getLogger(f"{LOGGER_NAME}.{safe_module_name}")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    log_file_env = os.getenv("DEPTH_SYNC_LOG_FILE", "").strip()
    use_explicit_log_file = bool(log_file_env) and safe_module_name in {"", "crypto-loader"}
    if use_explicit_log_file:
        log_path = Path(log_file_env)
    else:
        log_dir = Path(os.getenv("DEPTH_SYNC_LOG_DIR", DEFAULT_LOG_DIR))
        default_name = f"{safe_module_name}.log" if safe_module_name else DEFAULT_LOG_FILE
        log_path = log_dir / default_name
    file_handler: TimedRotatingFileHandler | None = None
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RetentionTimedRotatingFileHandler(
            filename=log_path,
            when="midnight",
            interval=1,
            backupCount=0,
            encoding="utf-8",
            utc=True,
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        enforce_log_retention(log_path)
    except OSError:
        logger.warning("Falling back to stderr logging; cannot create log path '%s'", log_path)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(logging.DEBUG if debug else logging.INFO)
        root_logger.addHandler(file_handler if file_handler is not None else stream_handler)
        if file_handler is not None:
            root_logger.addHandler(stream_handler)

    if debug:
        logger.debug("Debug logging enabled module=%s", safe_module_name)

    return logger


def fetch_concurrency() -> int:
    """Return bounded fetch concurrency from environment."""

    return _fetch_concurrency()
