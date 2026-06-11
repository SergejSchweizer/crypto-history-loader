"""Repository-level config contract guards.

These tests validate the committed runtime configuration shape used by the
CLI and medallion pipeline script. They should fail fast when `config.yaml`
is accidentally replaced by unrelated local content.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_repo_config() -> dict[str, Any]:
    yaml = pytest.importorskip("yaml")
    config_path = _repo_root() / "config.yaml"
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict), "config.yaml must contain a top-level mapping"
    return loaded


def _load_pipeline_module():
    script_path = _repo_root() / "scripts" / "run_medallion_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_medallion_pipeline", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_repo_config_has_required_top_level_sections() -> None:
    config = _load_repo_config()
    required = {"global", "env", "export-descriptive-stats", "bronze-build", "medallion-pipeline"}
    missing = sorted(required.difference(config))
    assert not missing, f"config.yaml missing required section(s): {', '.join(missing)}"


def test_repo_config_medallion_pipeline_contract() -> None:
    config = _load_repo_config()
    pipeline_cfg = config["medallion-pipeline"]
    assert isinstance(pipeline_cfg, dict)
    order = pipeline_cfg.get("execution_order")
    assert isinstance(order, list) and order, "medallion-pipeline.execution_order must be a non-empty list"

    allowed = {"bronze", "silver", "gold"}
    unknown = [str(name) for name in order if str(name) not in allowed]
    assert not unknown, f"Unsupported medallion-pipeline layer(s): {unknown}"

    for layer in order:
        layer_name = str(layer)
        layer_cfg = pipeline_cfg.get(layer_name)
        assert isinstance(layer_cfg, dict), f"medallion-pipeline.{layer_name} must be a mapping"
        if not bool(layer_cfg.get("enabled", True)):
            continue
        command = layer_cfg.get("command")
        assert isinstance(command, str) and command.strip(), f"medallion-pipeline.{layer_name}.command is required"
        cli_args = layer_cfg.get("cli_args", [])
        assert isinstance(cli_args, list), f"medallion-pipeline.{layer_name}.cli_args must be a list"


def test_repo_config_builds_pipeline_steps() -> None:
    module = _load_pipeline_module()
    config = _load_repo_config()
    main_path = _repo_root() / "main.py"
    config_path = _repo_root() / "config.yaml"
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=config)
    assert steps, "Expected at least one enabled medallion pipeline step"
    for step in steps:
        assert step.args, f"Pipeline step '{step.name}' must include command args"


def test_repo_config_medallion_bronze_inherits_full_history_start_bounds() -> None:
    module = _load_pipeline_module()
    config = _load_repo_config()
    main_path = _repo_root() / "main.py"
    config_path = _repo_root() / "config.yaml"
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=config)
    bronze_step = next(step for step in steps if step.name == "bronze")

    start_idx = bronze_step.args.index("--start-date")
    assert bronze_step.args[start_idx + 1] == config["bronze-build"]["start_date"]

    symbol_idx = bronze_step.args.index("--symbol-start-dates")
    expected_symbol_dates = config["bronze-build"]["symbol_start_dates"]
    assert bronze_step.args[symbol_idx + 1 : symbol_idx + 1 + len(expected_symbol_dates)] == expected_symbol_dates
