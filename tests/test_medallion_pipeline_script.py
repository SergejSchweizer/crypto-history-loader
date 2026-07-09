"""Integration-like tests for medallion pipeline script config behavior."""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest


def _load_pipeline_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_medallion_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_medallion_pipeline", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_steps_uses_configured_market_args_with_trades(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["bronze", "silver", "gold"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": ["--dataset", "spot_ohlcv", "perps_ohlcv", "open_interest", "funding", "perps_trades"],
            },
            "silver": {"enabled": False, "command": "silver-build", "cli_args": []},
            "gold": {"enabled": False, "command": "gold-build", "cli_args": []},
        }
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    assert len(steps) == 1
    args = steps[0].args
    assert "--dataset" in args
    assert "perps_trades" in args
    assert "volatility_index_data" in args


def test_build_steps_adds_volatility_to_silver_dataset_args(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["silver"],
            "silver": {
                "enabled": True,
                "command": "silver-build",
                "cli_args": ["--dataset", "spot_ohlcv", "perps_ohlcv", "open_interest", "funding"],
            },
        }
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    assert len(steps) == 1
    assert "volatility_index_data" in steps[0].args


def test_build_steps_bronze_inherits_symbol_start_bounds_from_bronze_config(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "bronze-build": {
            "symbol_start_dates": ["BTC=2018-08-14", "ETH=2019-03-14", "SOL=2022-03-15"],
        },
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": ["--dataset", "spot_ohlcv", "perps_ohlcv"],
            },
        },
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    assert len(steps) == 1
    assert "--start-date" not in steps[0].args
    assert "--exchange-symbol-start-dates" not in steps[0].args
    sym_idx = steps[0].args.index("--symbol-start-dates")
    assert steps[0].args[sym_idx + 1 : sym_idx + 4] == [
        "BTC=2018-08-14",
        "ETH=2019-03-14",
        "SOL=2022-03-15",
    ]


def test_build_steps_bronze_preserves_explicit_medallion_start_bounds(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "bronze-build": {
            "start_date": "2018-08-14",
            "symbol_start_dates": ["BTC=2018-08-14"],
            "exchange_symbol_start_dates": ["deribit:SOL=2022-03-15"],
        },
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": [
                    "--start-date",
                    "2020-01-01",
                    "--symbol-start-dates",
                    "BTC=2021-01-01",
                    "ETH=2026-05-10",
                    "--exchange-symbol-start-dates",
                    "deribit:SOL=2020-01-01",
                ],
            },
        },
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    args = steps[0].args
    start_idx = args.index("--start-date")
    assert args[start_idx + 1] == "2020-01-01"
    sym_idx = args.index("--symbol-start-dates")
    assert args[sym_idx + 1] == "BTC=2021-01-01"
    assert args[sym_idx + 2] == "ETH=2026-05-10"
    ex_idx = args.index("--exchange-symbol-start-dates")
    assert args[ex_idx + 1] == "deribit:SOL=2020-01-01"


def test_log_path_from_config_prefers_explicit_log_file(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    cfg = {"env": {"DEPTH_SYNC_LOG_FILE": str(tmp_path / "logs" / "pipeline.log")}}
    out = module._log_path_from_config(config_data=cfg, repo_root=tmp_path)
    assert out == (tmp_path / "logs" / "run-medallion-pipeline.log").resolve()


def test_build_steps_validation_errors(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="config missing required section"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data={})
    with pytest.raises(ValueError, match="execution_order must be a non-empty list"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data={"medallion-pipeline": {}})
    with pytest.raises(ValueError, match="Unsupported pipeline layer"):
        module._build_steps(
            main_path=main_path,
            config_path=config_path,
            config_data={"medallion-pipeline": {"execution_order": ["x"]}},
        )


def test_build_steps_layer_validation_errors(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    base = {
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {"enabled": True, "command": "bronze-build", "cli_args": []},
        }
    }

    bad_layer = {"medallion-pipeline": {"execution_order": ["bronze"], "bronze": []}}
    with pytest.raises(ValueError, match="must be a mapping"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data=bad_layer)

    bad_cmd = {"medallion-pipeline": {"execution_order": ["bronze"], "bronze": {"enabled": True, "cli_args": []}}}
    with pytest.raises(ValueError, match="command is required"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data=bad_cmd)

    bad_args = {
        "medallion-pipeline": {
            "execution_order": ["bronze"],
            "bronze": {"enabled": True, "command": "x", "cli_args": "bad"},
        }
    }
    with pytest.raises(ValueError, match="cli_args must be a list"):
        module._build_steps(main_path=main_path, config_path=config_path, config_data=bad_args)

    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=base)
    assert len(steps) == 1


def test_log_path_from_config_dir_and_default(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    cfg = {"env": {"DEPTH_SYNC_LOG_DIR": str(tmp_path / "xlogs")}}
    out = module._log_path_from_config(config_data=cfg, repo_root=tmp_path)
    assert out == (tmp_path / "xlogs" / "crypto-history-loader.log").resolve()
    out_default = module._log_path_from_config(config_data={}, repo_root=tmp_path)
    assert out_default == (tmp_path / ".run" / "logs" / "crypto-history-loader.log").resolve()


def test_lock_acquire_and_release(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    lock_file = tmp_path / "lock" / "x.lock"
    fd = module._acquire_nonblocking_lock(lock_file)
    assert isinstance(fd, int)
    try:
        assert module._acquire_nonblocking_lock(lock_file) is None
    finally:
        module._release_lock(fd)
    assert module._acquire_nonblocking_lock(lock_file) is not None


def test_rotate_pipeline_log_rotates_and_prunes(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    log_path = tmp_path / "pipeline.log"
    log_path.write_text("a\n", encoding="utf-8")
    today = datetime.now(UTC).date()
    rotated_date = today.fromordinal(today.toordinal() - 1)
    retained_date = today.fromordinal(today.toordinal() - 10)
    stale_date = today.fromordinal(today.toordinal() - 40)
    os.utime(log_path, (datetime.combine(rotated_date, datetime.min.time(), UTC).timestamp(),) * 2)
    stale = tmp_path / f"pipeline.log.{stale_date.isoformat()}"
    stale.write_text("x\n", encoding="utf-8")
    keep = tmp_path / f"pipeline.log.{retained_date.isoformat()}"
    keep.write_text("x\n", encoding="utf-8")
    module._rotate_pipeline_log(log_path, retention_days=30)
    assert not log_path.exists()
    assert (tmp_path / f"pipeline.log.{rotated_date.isoformat()}").exists()
    assert not stale.exists()
    assert keep.exists()


def test_main_missing_paths_returns_2(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pipeline_module()
    config_path = tmp_path / "config.yaml"
    main_path = tmp_path / "main.py"

    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "repo_root": str(tmp_path),
                "config": str(config_path),
                "main_path": str(main_path),
                "lock_file": str(tmp_path / "lock" / "x.lock"),
                "python_bin": sys.executable,
                "log_file": None,
            },
        )(),
    )
    assert module.main() == 2
    config_path.write_text("medallion-pipeline: {}\n", encoding="utf-8")
    assert module.main() == 2


def test_main_lock_already_running_returns_1(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pipeline_module()
    config_path = tmp_path / "config.yaml"
    main_path = tmp_path / "main.py"
    config_path.write_text(
        "medallion-pipeline:\n  execution_order: [bronze]\n  bronze:\n    enabled: false\n", encoding="utf-8"
    )
    main_path.write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "repo_root": str(tmp_path),
                "config": str(config_path),
                "main_path": str(main_path),
                "lock_file": str(tmp_path / "lock" / "x.lock"),
                "python_bin": sys.executable,
                "log_file": None,
            },
        )(),
    )
    monkeypatch.setattr(module, "_acquire_nonblocking_lock", lambda _p: None)
    assert module.main() == 1


def test_main_success_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pipeline_module()
    config_path = tmp_path / "config.yaml"
    main_path = tmp_path / "main.py"
    log_path = tmp_path / "custom.log"
    lock_file = tmp_path / "lock" / "x.lock"
    config_path.write_text(
        "medallion-pipeline:\n  execution_order: [bronze]\n  bronze:\n    enabled: false\n",
        encoding="utf-8",
    )
    main_path.write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "repo_root": str(tmp_path),
                "config": str(config_path),
                "main_path": str(main_path),
                "lock_file": str(lock_file),
                "python_bin": sys.executable,
                "log_file": str(log_path),
            },
        )(),
    )
    monkeypatch.setattr(module, "_acquire_nonblocking_lock", lambda _p: 123)
    monkeypatch.setattr(module, "_release_lock", lambda _fd: None)
    monkeypatch.setattr(module, "_run_pipeline", lambda **kwargs: None)
    assert module.main() == 0
    assert log_path.exists()
