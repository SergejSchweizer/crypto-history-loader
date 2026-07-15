"""Tests for the EODHD ISIN cron fetcher script."""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_fetch_module() -> Any:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "fetch_eodhd_isins.py"
    spec = importlib.util.spec_from_file_location("fetch_eodhd_isins", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_fetch_isin_rows_fetches_active_and_delisted_without_duplicate(tmp_path: Path) -> None:
    module = _load_fetch_module()
    config = module.FetchConfig(
        enabled=True,
        token_env="EODHD_API_TOKEN",
        output_csv=tmp_path / "eodhd_isins.csv",
        manifest_json=tmp_path / "eodhd_isins.json",
        exchanges=("US",),
        include_delisted=True,
        skip_without_token=True,
        timeout_s=1.0,
        sleep_s=0.0,
        lock_file=tmp_path / "fetch.lock",
        no_json_output=True,
    )
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_fetch(path: str, params: dict[str, str], timeout_s: float) -> list[dict[str, str]]:
        calls.append((path, params))
        if params.get("delisted") == "1":
            return [
                {
                    "Code": "DUP",
                    "Name": "Duplicate delisted",
                    "Exchange": "US",
                    "Currency": "USD",
                    "Country": "USA",
                    "Type": "Common Stock",
                    "Isin": "US0000000001",
                },
                {
                    "Code": "OLD",
                    "Name": "Old Co",
                    "Exchange": "US",
                    "Currency": "USD",
                    "Country": "USA",
                    "Type": "Common Stock",
                    "Isin": "US0000000002",
                },
            ]
        return [
            {
                "Code": "DUP",
                "Name": "Duplicate active",
                "Exchange": "US",
                "Currency": "USD",
                "Country": "USA",
                "Type": "Common Stock",
                "Isin": "US0000000001",
            },
            {
                "Code": "NOISIN",
                "Name": "No ISIN",
                "Exchange": "US",
                "Currency": "USD",
                "Country": "USA",
                "Type": "Common Stock",
                "Isin": "",
            },
        ]

    rows = module.fetch_isin_rows(
        config,
        api_token="token",
        fetched_at_utc="2026-07-15T00:00:00Z",
        fetch_json=fake_fetch,
    )

    assert [(path, params.get("delisted")) for path, params in calls] == [
        ("/exchange-symbol-list/US", None),
        ("/exchange-symbol-list/US", "1"),
    ]
    assert [(row.code, row.list_status, row.name) for row in rows] == [
        ("DUP", "active", "Duplicate active"),
        ("OLD", "delisted", "Old Co"),
    ]


def test_fetch_isin_rows_loads_exchange_list_when_exchanges_are_not_configured(tmp_path: Path) -> None:
    module = _load_fetch_module()
    config = module.FetchConfig(
        enabled=True,
        token_env="EODHD_API_TOKEN",
        output_csv=tmp_path / "eodhd_isins.csv",
        manifest_json=tmp_path / "eodhd_isins.json",
        exchanges=(),
        include_delisted=False,
        skip_without_token=True,
        timeout_s=1.0,
        sleep_s=0.0,
        lock_file=tmp_path / "fetch.lock",
        no_json_output=True,
    )

    def fake_fetch(path: str, params: dict[str, str], timeout_s: float) -> object:
        if path == "/exchanges-list/":
            return [{"Code": "LSE"}, {"Code": "US"}]
        return [
            {
                "Code": f"{path.rsplit('/', maxsplit=1)[-1]}1",
                "Name": "Listed Co",
                "Exchange": path.rsplit("/", maxsplit=1)[-1],
                "Currency": "USD",
                "Country": "USA",
                "Type": "Common Stock",
                "Isin": f"{path.rsplit('/', maxsplit=1)[-1]}ISIN",
            }
        ]

    rows = module.fetch_isin_rows(
        config,
        api_token="token",
        fetched_at_utc="2026-07-15T00:00:00Z",
        fetch_json=fake_fetch,
    )

    assert [row.source_exchange_code for row in rows] == ["LSE", "US"]


def test_write_outputs_creates_csv_and_manifest(tmp_path: Path) -> None:
    module = _load_fetch_module()
    config = module.FetchConfig(
        enabled=True,
        token_env="EODHD_API_TOKEN",
        output_csv=tmp_path / "ref" / "eodhd_isins.csv",
        manifest_json=tmp_path / "ref" / "eodhd_isins.json",
        exchanges=("US",),
        include_delisted=True,
        skip_without_token=True,
        timeout_s=1.0,
        sleep_s=0.0,
        lock_file=tmp_path / "fetch.lock",
        no_json_output=True,
    )
    row = module.IsinRow(
        isin="US0000000001",
        code="ABC",
        name="ABC Corp",
        exchange="US",
        currency="USD",
        country="USA",
        type="Common Stock",
        list_status="active",
        source_exchange_code="US",
        fetched_at_utc="2026-07-15T00:00:00Z",
    )

    manifest = module.write_outputs(config, rows=[row], fetched_at_utc="2026-07-15T00:00:00Z")

    with config.output_csv.open(encoding="utf-8", newline="") as handle:
        csv_rows = list(csv.DictReader(handle))
    assert csv_rows[0]["isin"] == "US0000000001"
    assert manifest["row_count"] == 1
    assert config.manifest_json.exists()
