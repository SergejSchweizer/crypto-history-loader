"""Fetch the canonical all-ISIN reference dataset from EODHD."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import logging
import os
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

try:
    from scripts.logging_utils import configure_logger
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from scripts.logging_utils import configure_logger

EODHD_BASE_URL = "https://eodhd.com/api"
DEFAULT_CONFIG_SECTION = "fetch_all_isins"
LEGACY_CONFIG_SECTION = "eodhd-isin-fetch"
CSV_FIELDS = [
    "isin",
    "code",
    "name",
    "exchange",
    "currency",
    "country",
    "type",
    "list_status",
    "source_exchange_code",
    "fetched_at_utc",
]

JsonFetcher = Callable[[str, dict[str, str], float], Any]


@dataclass(frozen=True)
class FetchConfig:
    """Configuration for one EODHD ISIN reference fetch."""

    enabled: bool
    token_env: str
    secret_config: Path
    output_csv: Path
    manifest_json: Path
    exchanges: tuple[str, ...]
    include_delisted: bool
    skip_without_token: bool
    timeout_s: float
    sleep_s: float
    lock_file: Path
    no_json_output: bool


@dataclass(frozen=True)
class IsinRow:
    """Normalized EODHD ISIN row preserved as a listed instrument reference."""

    isin: str
    code: str
    name: str
    exchange: str
    currency: str
    country: str
    type: str
    list_status: str
    source_exchange_code: str
    fetched_at_utc: str

    def csv_row(self) -> dict[str, str]:
        """Return the deterministic CSV representation."""

        return {
            "isin": self.isin,
            "code": self.code,
            "name": self.name,
            "exchange": self.exchange,
            "currency": self.currency,
            "country": self.country,
            "type": self.type,
            "list_status": self.list_status,
            "source_exchange_code": self.source_exchange_code,
            "fetched_at_utc": self.fetched_at_utc,
        }


def _utc_now() -> str:
    """Return the current UTC timestamp for deterministic run metadata."""

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML mapping from disk."""

    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to load EODHD ISIN fetch config.") from exc

    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError("config file must contain a top-level mapping")
    return loaded


def _section(config_data: dict[str, Any]) -> dict[str, Any]:
    """Return the configured canonical all-ISIN fetch section."""

    raw = config_data.get(DEFAULT_CONFIG_SECTION, config_data.get(LEGACY_CONFIG_SECTION, {}))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{DEFAULT_CONFIG_SECTION} must be a mapping")
    return raw


def _bool_value(value: object, *, default: bool) -> bool:
    """Return a bool from permissive config values."""

    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _float_value(value: object, *, default: float) -> float:
    """Return a positive float from config values."""

    if value is None:
        return default
    if not isinstance(value, str | int | float):
        raise ValueError("duration values must be strings or numbers")
    parsed = float(value)
    if parsed < 0:
        raise ValueError("duration values must be non-negative")
    return parsed


def _string_tuple(value: object) -> tuple[str, ...]:
    """Return a normalized tuple of non-empty strings."""

    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError("configured exchanges must be a list")
    return tuple(str(item).strip().upper() for item in value if str(item).strip())


def _resolve_path(value: object, *, default: Path, repo_root: Path) -> Path:
    """Resolve a configured path against the repository root."""

    raw = str(value).strip() if value is not None else str(default)
    path = Path(raw)
    return path if path.is_absolute() else repo_root / path


def build_fetch_config(args: argparse.Namespace, *, repo_root: Path, config_data: dict[str, Any]) -> FetchConfig:
    """Build the effective fetch configuration from CLI and YAML."""

    cfg = _section(config_data)
    output_csv = _resolve_path(
        args.output_csv or cfg.get("output_csv"),
        default=Path("lake/reference/all_isins/all_isins.csv"),
        repo_root=repo_root,
    )
    manifest_json = _resolve_path(
        args.manifest_json or cfg.get("manifest_json"),
        default=Path("lake/reference/all_isins/all_isins.json"),
        repo_root=repo_root,
    )
    lock_file = _resolve_path(
        args.lock_file or cfg.get("lock_file"),
        default=Path(".run/fetch-all-isins.lock"),
        repo_root=repo_root,
    )
    secret_config = _resolve_path(
        cfg.get("secret_config"),
        default=Path(".secrets/eodhd.yaml"),
        repo_root=repo_root,
    )
    exchanges = (
        tuple(code.strip().upper() for code in args.exchange_codes if code.strip())
        if args.exchange_codes
        else _string_tuple(cfg.get("exchanges"))
    )
    include_delisted = args.include_delisted
    if include_delisted is None:
        include_delisted = _bool_value(cfg.get("include_delisted"), default=True)

    return FetchConfig(
        enabled=_bool_value(cfg.get("enabled"), default=True),
        token_env=str(cfg.get("api_token_env", "EODHD_API_TOKEN")).strip() or "EODHD_API_TOKEN",
        secret_config=secret_config.resolve(),
        output_csv=output_csv.resolve(),
        manifest_json=manifest_json.resolve(),
        exchanges=exchanges,
        include_delisted=include_delisted,
        skip_without_token=_bool_value(cfg.get("skip_without_token"), default=True),
        timeout_s=_float_value(args.timeout_s if args.timeout_s is not None else cfg.get("timeout_s"), default=60.0),
        sleep_s=_float_value(args.sleep_s if args.sleep_s is not None else cfg.get("sleep_s"), default=0.2),
        lock_file=lock_file.resolve(),
        no_json_output=args.no_json_output or _bool_value(cfg.get("no_json_output"), default=False),
    )


def _api_token_from_mapping(config_data: dict[str, Any]) -> str:
    """Read an EODHD API token from a local secret config mapping."""

    for key in ("api_key", "api_token"):
        value = config_data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    section = config_data.get(DEFAULT_CONFIG_SECTION)
    if isinstance(section, dict):
        for key in ("api_key", "api_token"):
            value = section.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    legacy_section = config_data.get(LEGACY_CONFIG_SECTION)
    if isinstance(legacy_section, dict):
        for key in ("api_key", "api_token"):
            value = legacy_section.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    eodhd_section = config_data.get("eodhd")
    if isinstance(eodhd_section, dict):
        for key in ("api_key", "api_token"):
            value = eodhd_section.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    return ""


def read_api_token(config: FetchConfig) -> tuple[str, str]:
    """Read the EODHD API token from local secret config first, then environment fallback."""

    if config.secret_config.exists():
        token = _api_token_from_mapping(_load_yaml(config.secret_config))
        if token:
            return token, str(config.secret_config)

    env_token = os.environ.get(config.token_env, "").strip()
    if env_token:
        return env_token, config.token_env

    return "", f"{config.secret_config} or {config.token_env}"


def _request_json(path: str, params: dict[str, str], timeout_s: float) -> Any:
    """Fetch JSON from EODHD using stdlib networking only."""

    url = f"{EODHD_BASE_URL}{path}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "crypto-history-loader/fetch-all-isins"})
    try:
        with urlopen(request, timeout=timeout_s) as response:
            charset = response.headers.get_content_charset("utf-8")
            return json.loads(response.read().decode(charset))
    except HTTPError as exc:
        raise RuntimeError(f"EODHD request failed with HTTP {exc.code}: {path}") from exc
    except URLError as exc:
        raise RuntimeError(f"EODHD request failed: {path}: {exc.reason}") from exc


def _extract_exchange_codes(payload: object) -> tuple[str, ...]:
    """Extract exchange codes from the EODHD exchanges-list response."""

    if not isinstance(payload, list):
        raise ValueError("EODHD exchanges-list response must be a list")
    codes: set[str] = set()
    for item in payload:
        if isinstance(item, dict):
            code = str(item.get("Code", "")).strip().upper()
            if code:
                codes.add(code)
    if not codes:
        raise ValueError("EODHD exchanges-list response did not contain exchange codes")
    return tuple(sorted(codes))


def _normalise_symbol_rows(
    payload: object,
    *,
    source_exchange_code: str,
    list_status: str,
    fetched_at_utc: str,
) -> Iterator[IsinRow]:
    """Normalize EODHD symbol-list rows, keeping only rows with an ISIN."""

    if not isinstance(payload, list):
        raise ValueError(f"EODHD symbol-list response for {source_exchange_code} must be a list")
    for item in payload:
        if not isinstance(item, dict):
            continue
        isin = str(item.get("Isin", "")).strip().upper()
        if not isin:
            continue
        exchange = str(item.get("Exchange", "") or source_exchange_code).strip().upper()
        yield IsinRow(
            isin=isin,
            code=str(item.get("Code", "")).strip(),
            name=str(item.get("Name", "")).strip(),
            exchange=exchange,
            currency=str(item.get("Currency", "")).strip().upper(),
            country=str(item.get("Country", "")).strip(),
            type=str(item.get("Type", "")).strip(),
            list_status=list_status,
            source_exchange_code=source_exchange_code,
            fetched_at_utc=fetched_at_utc,
        )


def fetch_isin_rows(
    config: FetchConfig,
    *,
    api_token: str,
    fetched_at_utc: str,
    fetch_json: JsonFetcher = _request_json,
    logger: logging.Logger | None = None,
) -> list[IsinRow]:
    """Fetch and normalize active plus optional delisted EODHD ISIN rows."""

    log = logger or logging.getLogger(__name__)
    exchange_codes = config.exchanges
    if not exchange_codes:
        log.info("Fetching EODHD exchange list")
        payload = fetch_json("/exchanges-list/", {"api_token": api_token, "fmt": "json"}, config.timeout_s)
        exchange_codes = _extract_exchange_codes(payload)

    rows_by_key: dict[tuple[str, str, str], IsinRow] = {}
    for exchange_code in exchange_codes:
        endpoint = f"/exchange-symbol-list/{quote(exchange_code, safe='')}"
        requests = [("active", {"api_token": api_token, "fmt": "json"})]
        if config.include_delisted:
            # EODHD returns inactive tickers only when delisted=1, so full coverage requires both calls.
            requests.append(("delisted", {"api_token": api_token, "fmt": "json", "delisted": "1"}))

        for list_status, params in requests:
            log.info("Fetching EODHD ISIN symbols exchange=%s status=%s", exchange_code, list_status)
            payload = fetch_json(endpoint, params, config.timeout_s)
            for row in _normalise_symbol_rows(
                payload,
                source_exchange_code=exchange_code,
                list_status=list_status,
                fetched_at_utc=fetched_at_utc,
            ):
                key = (row.isin, row.exchange, row.code)
                existing = rows_by_key.get(key)
                if existing is None or existing.list_status == "delisted":
                    rows_by_key[key] = row
            if config.sleep_s:
                time.sleep(config.sleep_s)

    return sorted(rows_by_key.values(), key=lambda row: (row.exchange, row.isin, row.code))


def _atomic_write_text(path: Path, text: str) -> None:
    """Write text atomically into the target path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, newline="") as temp_handle:
        temp_handle.write(text)
        temp_name = temp_handle.name
    os.replace(temp_name, path)


def write_outputs(config: FetchConfig, *, rows: Iterable[IsinRow], fetched_at_utc: str) -> dict[str, Any]:
    """Write CSV and manifest outputs atomically."""

    rows_list = list(rows)
    config.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w", encoding="utf-8", dir=config.output_csv.parent, delete=False, newline=""
    ) as temp_handle:
        writer = csv.DictWriter(temp_handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows_list:
            writer.writerow(row.csv_row())
        temp_name = temp_handle.name
    os.replace(temp_name, config.output_csv)

    manifest = {
        "dataset": "all_isins",
        "source": "eodhd",
        "fetched_at_utc": fetched_at_utc,
        "row_count": len(rows_list),
        "exchange_count": len({row.source_exchange_code for row in rows_list}),
        "include_delisted": config.include_delisted,
        "output_csv": str(config.output_csv),
        "fields": CSV_FIELDS,
    }
    _atomic_write_text(config.manifest_json, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


@contextmanager
def _locked(lock_file: Path) -> Iterator[bool]:
    """Acquire a non-blocking file lock for cron-safe execution."""

    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o644)
    acquired = False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        if acquired:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI options."""

    parser = argparse.ArgumentParser(description="Fetch the canonical all-ISIN reference data from EODHD.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--output-csv", help="Output CSV path")
    parser.add_argument("--manifest-json", help="Output manifest JSON path")
    parser.add_argument(
        "--exchange-codes", nargs="*", default=None, help="Exchange codes to fetch; defaults to config/all"
    )
    parser.add_argument("--timeout-s", type=float, default=None, help="HTTP timeout in seconds")
    parser.add_argument("--sleep-s", type=float, default=None, help="Sleep between EODHD requests")
    parser.add_argument("--lock-file", help="Non-blocking lock file path")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress stdout summary JSON")
    delisted = parser.add_mutually_exclusive_group()
    delisted.add_argument(
        "--include-delisted", action="store_true", default=None, help="Fetch active and delisted tickers"
    )
    delisted.add_argument(
        "--no-include-delisted", action="store_false", dest="include_delisted", help="Fetch active tickers only"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the cron-safe EODHD ISIN reference fetch."""

    args = parse_args(argv)
    config_path = Path(args.config).resolve()
    repo_root = config_path.parent
    logger = configure_logger("fetch-all-isins", config_path)
    config_data = _load_yaml(config_path) if config_path.exists() else {}
    config = build_fetch_config(args, repo_root=repo_root, config_data=config_data)

    if not config.enabled:
        logger.info("EODHD ISIN fetch disabled by config")
        return 0

    api_token, token_source = read_api_token(config)
    if not api_token:
        message = f"Missing EODHD API token in {token_source}"
        if config.skip_without_token:
            logger.warning("%s; skipping EODHD ISIN fetch", message)
            return 0
        logger.error(message)
        return 2

    with _locked(config.lock_file) as acquired:
        if not acquired:
            logger.warning("EODHD ISIN fetch already running lock=%s", config.lock_file)
            return 1
        fetched_at_utc = _utc_now()
        logger.info(
            "EODHD ISIN fetch started output=%s include_delisted=%s token_source=%s",
            config.output_csv,
            config.include_delisted,
            token_source,
        )
        rows = fetch_isin_rows(config, api_token=api_token, fetched_at_utc=fetched_at_utc, logger=logger)
        manifest = write_outputs(config, rows=rows, fetched_at_utc=fetched_at_utc)
        logger.info("EODHD ISIN fetch complete rows=%s exchanges=%s", manifest["row_count"], manifest["exchange_count"])

    if not config.no_json_output:
        print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
