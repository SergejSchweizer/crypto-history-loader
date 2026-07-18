"""Parser-level coverage for individual CLI arguments."""

from __future__ import annotations

import shlex
import sys
from pathlib import Path

import pytest

from api import cli
from api.cli import build_parser
from application.dataset_contracts import supported_silver_build_ids
from scripts import run_medallion_pipeline

EXPECTED_MEDALLION_SILVER_DATASETS = set(supported_silver_build_ids())


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["bronze-build", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["--debug", "bronze-build", "--exchange", "deribit"], {"debug": True}),
        (["bronze-build", "--exchanges", "deribit"], {"exchanges": ["deribit"]}),
        (["bronze-build", "--dataset", "perps_trades"], {"dataset": ["perps_trades"]}),
        (["bronze-build", "--dataset", "options_trades"], {"dataset": ["options_trades"]}),
        (["bronze-build", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["bronze-build", "--save-parquet-lake"], {"save_parquet_lake": True}),
        (["bronze-build", "--lake-root", "lake/test-bronze"], {"lake_root": "lake/test-bronze"}),
        (["bronze-build", "--no-json-output"], {"no_json_output": True}),
        (["bronze-build"], {"tail_delta_only": False}),
        (["bronze-build", "--tail-delta-only"], {"tail_delta_only": True}),
        (["bronze-build", "--full-gap-fill"], {"tail_delta_only": False}),
        (["bronze-build", "--start-date", "2023-04-24"], {"start_date": "2023-04-24"}),
        (["bronze-build", "--symbol-start-dates", "BTC=2023-04-24"], {"symbol_start_dates": ["BTC=2023-04-24"]}),
        (
            ["bronze-build", "--exchange-symbol-start-dates", "deribit:BTC=2023-04-24"],
            {"exchange_symbol_start_dates": ["deribit:BTC=2023-04-24"]},
        ),
        (["silver-build", "--bronze-root", "lake/test-bronze"], {"bronze_root": "lake/test-bronze"}),
        (["silver-build", "--silver-root", "lake/test-silver"], {"silver_root": "lake/test-silver"}),
        (["silver-build", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["silver-build", "--dataset", "spot_ohlcv"], {"dataset": ["spot_ohlcv"]}),
        (["silver-build", "--dataset", "options_trades"], {"dataset": ["options_trades"]}),
        (["silver-build", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["silver-build", "--timeframe", "1m"], {"timeframe": "1m"}),
        (["silver-build", "--manifest"], {"manifest": True}),
        (["silver-build", "--plot"], {"plot": True}),
        (["silver-build", "--maxprocesses", "4"], {"maxprocesses": 4}),
        (["silver-build", "--no-json-output"], {"no_json_output": True}),
        (["gold-build", "--silver-root", "lake/test-silver"], {"silver_root": "lake/test-silver"}),
        (["gold-build", "--gold-root", "lake/test-gold"], {"gold_root": "lake/test-gold"}),
        (["gold-build", "--l2-root", "lake/test-l2"], {"l2_root": "lake/test-l2"}),
        (["gold-build", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["gold-build", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["gold-build", "--dataset-id", "gold.market.full.m1"], {"dataset_id": "gold.market.full.m1"}),
        (["gold-build", "--dataset-id", "gold.market.perps_trades.m1"], {"dataset_id": "gold.market.perps_trades.m1"}),
        (
            ["gold-build", "--dataset-id", "gold.market.options_trades.m1"],
            {"dataset_id": "gold.market.options_trades.m1"},
        ),
        (["gold-build", "--dataset-version", "v1.2.3"], {"dataset_version": "v1.2.3"}),
        (["gold-build", "--auto-version"], {"auto_version": True}),
        (["gold-build", "--version-base", "v1.0.0"], {"version_base": "v1.0.0"}),
        (["gold-build", "--manifest"], {"manifest": True}),
        (["gold-build", "--plot"], {"plot": True}),
        (["gold-build", "--l2-validation-mode", "lenient"], {"l2_validation_mode": "lenient"}),
        (["gold-build", "--retention-keep-versions", "3"], {"retention_keep_versions": 3}),
        (["gold-build", "--maxprocesses", "4"], {"maxprocesses": 4}),
        (["gold-build", "--no-json-output"], {"no_json_output": True}),
        (["dataset-inventory", "--bronze-root", "lake/test-bronze"], {"bronze_root": "lake/test-bronze"}),
        (["dataset-inventory", "--silver-root", "lake/test-silver"], {"silver_root": "lake/test-silver"}),
        (["dataset-inventory", "--gold-root", "lake/test-gold"], {"gold_root": "lake/test-gold"}),
        (["dataset-inventory", "--output", "docs/inventory.md"], {"output": "docs/inventory.md"}),
        (["dataset-inventory", "--format", "json"], {"format": "json"}),
        (["dataset-inventory", "--builder-commit", "abc123"], {"builder_commit": "abc123"}),
        (["dataset-inventory", "--no-json-output"], {"no_json_output": True}),
        (["list-spot_ohlcv-timeframes", "--exchange", "deribit"], {"exchange": "deribit"}),
        (["list-spot_ohlcv-timeframes", "--exchanges", "deribit"], {"exchanges": ["deribit"]}),
        (["export-descriptive-stats", "--lake-root", "lake/test-bronze"], {"lake_root": "lake/test-bronze"}),
        (["export-descriptive-stats", "--output-csv", "docs/tables/out.csv"], {"output_csv": "docs/tables/out.csv"}),
        (
            ["export-descriptive-stats", "--start-time", "2026-01-01T00:00:00+00:00"],
            {"start_time": "2026-01-01T00:00:00+00:00"},
        ),
        (
            ["export-descriptive-stats", "--end-time", "2026-01-31T23:59:59+00:00"],
            {"end_time": "2026-01-31T23:59:59+00:00"},
        ),
        (["export-descriptive-stats", "--exchanges", "deribit"], {"exchanges": ["deribit"]}),
        (["export-descriptive-stats", "--symbols", "BTC"], {"symbols": ["BTC"]}),
        (["export-descriptive-stats", "--timeframes", "1m"], {"timeframes": ["1m"]}),
        (
            ["export-descriptive-stats", "--instrument-types", "spot_ohlcv"],
            {"instrument_types": ["spot_ohlcv"]},
        ),
        (["export-descriptive-stats", "--no-json-output"], {"no_json_output": True}),
    ],
)
def test_cli_argument_parsing_individual_arguments(argv: list[str], expected: dict[str, object]) -> None:
    """Each CLI argument must parse correctly in isolation."""

    parser = build_parser()
    args = parser.parse_args(argv)
    for field, value in expected.items():
        assert getattr(args, field) == value


def test_medallion_pipeline_cli_args_in_config_are_parser_compatible() -> None:
    """`config.yaml` medallion pipeline cli_args must remain valid for each command parser."""

    yaml = pytest.importorskip("yaml")
    config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    config_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert isinstance(config_data, dict)
    pipeline_cfg = config_data.get("medallion-pipeline")
    assert isinstance(pipeline_cfg, dict)
    order = pipeline_cfg.get("execution_order")
    assert isinstance(order, list) and order

    parser = build_parser()
    for layer in order:
        layer_cfg = pipeline_cfg.get(str(layer))
        assert isinstance(layer_cfg, dict), f"medallion-pipeline.{layer} must be a mapping"
        if not bool(layer_cfg.get("enabled", True)):
            continue
        command = layer_cfg.get("command")
        assert isinstance(command, str) and command.strip(), f"medallion-pipeline.{layer}.command is required"
        cli_args = layer_cfg.get("cli_args", [])
        assert isinstance(cli_args, list), f"medallion-pipeline.{layer}.cli_args must be a list"
        argv = [command, *[str(token) for token in cli_args]]
        parsed = parser.parse_args(argv)
        assert parsed.command == command


def _readme_shell_commands() -> list[list[str]]:
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")
    commands: list[list[str]] = []
    in_bash = False
    current: list[str] = []
    for line in readme.splitlines():
        stripped = line.strip()
        if stripped == "```bash":
            in_bash = True
            current = []
            continue
        if in_bash and stripped == "```":
            in_bash = False
            if current:
                command = " ".join(part.removesuffix("\\").strip() for part in current)
                tokens = shlex.split(command)
                if tokens[:3] == ["uv", "run", "python"] and len(tokens) >= 4:
                    commands.append(tokens[3:])
            current = []
            continue
        if in_bash and stripped:
            current.append(stripped)
    return commands


def test_readme_python_commands_parse_without_lake_or_network_access(monkeypatch: pytest.MonkeyPatch) -> None:
    """QC-05: documented Python commands should stay compatible with parser surfaces."""

    parser = build_parser()
    commands = _readme_shell_commands()
    assert commands

    parsed_main_commands = 0
    parsed_pipeline_commands = 0
    for command in commands:
        if command[0] == "main.py":
            parsed = parser.parse_args(command[1:])
            assert parsed.command in {
                "bronze-build",
                "silver-build",
                "gold-build",
                "dataset-inventory",
            }
            parsed_main_commands += 1
        elif command[0] == "scripts/run_medallion_pipeline.py":
            monkeypatch.setattr(sys, "argv", command)
            parsed = run_medallion_pipeline.parse_args()
            assert parsed.config.endswith("config.yaml")
            parsed_pipeline_commands += 1

    assert parsed_main_commands >= 6
    assert parsed_pipeline_commands == 1


def test_medallion_pipeline_schedules_complete_silver_and_gold_runs() -> None:
    """The cron medallion run should not silently omit supported Silver or Gold families."""

    yaml = pytest.importorskip("yaml")
    config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    config_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert isinstance(config_data, dict)
    pipeline_cfg = config_data.get("medallion-pipeline")
    assert isinstance(pipeline_cfg, dict)

    silver_cfg = pipeline_cfg.get("silver")
    assert isinstance(silver_cfg, dict)
    silver_args = silver_cfg.get("cli_args")
    assert isinstance(silver_args, list)
    silver_dataset_index = silver_args.index("--dataset")
    silver_datasets: set[str] = set()
    for token in silver_args[silver_dataset_index + 1 :]:
        token_value = str(token)
        if token_value.startswith("--"):
            break
        silver_datasets.add(token_value)
    assert silver_datasets == EXPECTED_MEDALLION_SILVER_DATASETS

    gold_cfg = pipeline_cfg.get("gold")
    assert isinstance(gold_cfg, dict)
    gold_args = gold_cfg.get("cli_args")
    assert isinstance(gold_args, list)
    assert "--dataset-id" not in gold_args


def test_apply_yaml_defaults_keeps_save_parquet_cli_only() -> None:
    args = cli.build_parser().parse_args(["bronze-build"])
    config = {
        "global": {
            "save_parquet_lake": True,
            "debug": True,
            "lake_root": "lake/from-config",
        }
    }

    cli._apply_yaml_defaults(args=args, command="bronze-build", config=config, explicit_dests=set())  # type: ignore[attr-defined]

    assert args.save_parquet_lake is False
    assert args.lake_root == "lake/from-config"


def test_resolve_command_config_supports_bronze_aliases() -> None:
    config = {"loader": {"save_parquet_lake": True}, "silver-build": {"market": ["spot_ohlcv"]}}
    assert cli._resolve_command_config("bronze-build", config) == {"save_parquet_lake": True}  # type: ignore[attr-defined]
    assert cli._resolve_command_config("silver-build", config) == {"market": ["spot_ohlcv"]}  # type: ignore[attr-defined]


def test_apply_env_from_config_sets_non_null_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UNIT_TEST_ENV_A", raising=False)
    monkeypatch.delenv("UNIT_TEST_ENV_B", raising=False)
    cli._apply_env_from_config({"env": {"UNIT_TEST_ENV_A": "1", "UNIT_TEST_ENV_B": None}})  # type: ignore[attr-defined]
    assert cli.os.environ["UNIT_TEST_ENV_A"] == "1"
    assert "UNIT_TEST_ENV_B" not in cli.os.environ
