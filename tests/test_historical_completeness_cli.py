"""Tests for the read-only historical completeness CLI."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pytest

from api import cli
from api.commands import historical_completeness


def _config() -> dict[str, object]:
    return {
        "bronze-build": {
            "exchange": "deribit",
            "dataset": ["perps_trades"],
            "symbols": ["BTC"],
            "symbol_start_dates": ["BTC=2026-01-01"],
        }
    }


def test_parser_exposes_dedicated_audit_arguments() -> None:
    """The audit command accepts a lake root and deterministic UTC upper bound."""

    args = cli.build_parser().parse_args(
        [
            "historical-completeness-audit",
            "--bronze-root",
            "lake/test-bronze",
            "--end-time",
            "2026-01-01T00:00:00Z",
        ]
    )

    assert args.command == "historical-completeness-audit"
    assert args.bronze_root == "lake/test-bronze"
    assert args.end_time == "2026-01-01T00:00:00Z"


def test_command_prints_sanitized_reconcile_report_without_creating_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing acquisition interval is printed, fails closed, and leaves the root absent."""

    bronze_root = tmp_path / "bronze"
    args = cli.build_parser().parse_args(
        [
            "historical-completeness-audit",
            "--bronze-root",
            str(bronze_root),
            "--end-time",
            "2026-01-01T00:00:00Z",
        ]
    )
    logger = logging.getLogger("historical-completeness-cli-test")
    logger.handlers = [logging.NullHandler()]

    report = historical_completeness.run_historical_completeness_audit(
        args=args,
        config=_config(),
        logger=logger,
    )

    payload = json.loads(capsys.readouterr().out)
    assert report.status == "RECONCILE_REQUIRED"
    assert payload["status"] == "RECONCILE_REQUIRED"
    assert payload["intervals"][0]["gap_category"] == "unverified_acquisition"
    assert payload["intervals"][0]["lineage"] == "perps_trades|deribit|perp|BTC-PERPETUAL|tick"
    assert str(tmp_path) not in json.dumps(payload)
    assert not bronze_root.exists()


def test_main_uses_stream_only_logger_and_yaml_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Top-level dispatch avoids the file logger and applies the command config section."""

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """env: {}
export-descriptive-stats:
  lake_root: lake/bronze
  output_csv: out.csv
  start_time: '2026-01-01T00:00:00Z'
  end_time: '2026-01-01T00:00:00Z'
  exchanges: [deribit]
  symbols: [BTC]
  timeframes: [1m]
  instrument_types: [perp]
bronze-build:
  exchange: deribit
  dataset: [perps_trades]
  symbols: [BTC]
  symbol_start_dates: [BTC=2026-01-01]
historical-completeness-audit:
  bronze_root: lake/configured-bronze
  end_time: '2026-01-01T00:00:00Z'
""",
        encoding="utf-8",
    )
    config_path.chmod(0o644)
    captured: dict[str, object] = {}

    def _run(*, args: object, config: dict[str, object], logger: logging.Logger) -> None:
        captured["args"] = args
        captured["config"] = config
        captured["logger"] = logger

    monkeypatch.setattr(historical_completeness, "run_historical_completeness_audit", _run)
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("file logger must not be used")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--config", str(config_path), "historical-completeness-audit"],
    )

    cli.main()

    args = captured["args"]
    assert isinstance(args, argparse.Namespace)
    assert args.bronze_root == "lake/configured-bronze"
    assert args.end_time == "2026-01-01T00:00:00Z"
    assert isinstance(captured["logger"], logging.Logger)
