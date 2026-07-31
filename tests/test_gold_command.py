"""Tests for gold command helper behavior."""

from __future__ import annotations

import argparse
import logging

import pytest

from api.commands import gold as gold_cmd
from application.dataset_contracts import supported_gold_dataset_ids


def gold_args(
    *,
    retention_keep_versions: int = 3,
    maxprocesses: int = 4,
    no_json_output: bool = False,
    dataset_id: str = "gold.market.full.m1",
) -> argparse.Namespace:
    """Build the minimal argparse namespace used by gold-build command tests."""

    return argparse.Namespace(
        silver_root="lake/silver",
        gold_root="lake/gold",
        l2_root="remote_l2_m1_features",
        exchange="deribit",
        dataset_id=dataset_id,
        dataset_version="v1.0.0",
        auto_version=False,
        version_base="v1.0.0",
        symbols=None,
        l2_validation_mode="strict",
        retention_keep_versions=retention_keep_versions,
        maxprocesses=maxprocesses,
        no_json_output=no_json_output,
    )


def test_resolve_dataset_ids_returns_single_when_explicit() -> None:
    assert gold_cmd._resolve_dataset_ids("gold.market.full.m1") == ["gold.market.full.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.history.full.m1") == ["gold.history.full.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.history.extended.m1") == ["gold.history.extended.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.history.extended.m5") == ["gold.history.extended.m5"]
    assert gold_cmd._resolve_dataset_ids("gold.history.extended.m30") == ["gold.history.extended.m30"]
    assert gold_cmd._resolve_dataset_ids("gold.history.extended.h1") == ["gold.history.extended.h1"]
    assert gold_cmd._resolve_dataset_ids("gold.history.extended_full.m1") == ["gold.history.extended_full.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.market.regime_features.m1") == ["gold.market.regime_features.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.market.prediction_targets.m1") == ["gold.market.prediction_targets.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.live.volatility_features.m1") == ["gold.live.volatility_features.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.live.microstructure_features.m1") == [
        "gold.live.microstructure_features.m1"
    ]
    assert gold_cmd._resolve_dataset_ids("gold.live.extended.m1") == ["gold.live.extended.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.live.extended.m5") == ["gold.live.extended.m5"]
    assert gold_cmd._resolve_dataset_ids("gold.live.extended.m30") == ["gold.live.extended.m30"]
    assert gold_cmd._resolve_dataset_ids("gold.live.extended.h1") == ["gold.live.extended.h1"]
    assert gold_cmd._resolve_dataset_ids("gold.live.full.m1") == ["gold.live.full.m1"]
    assert gold_cmd._resolve_dataset_ids("gold.live.full.m5") == ["gold.live.full.m5"]
    assert gold_cmd._resolve_dataset_ids("gold.live.full.m30") == ["gold.live.full.m30"]
    assert gold_cmd._resolve_dataset_ids("gold.live.full.h1") == ["gold.live.full.h1"]


def test_resolve_dataset_ids_returns_sorted_supported_when_missing() -> None:
    expected = list(supported_gold_dataset_ids())
    assert gold_cmd._resolve_dataset_ids(None) == expected


def test_resolve_gold_symbols_normalizes_and_deduplicates() -> None:
    symbols = gold_cmd._resolve_gold_symbols(
        symbols=["btc", "BTC-PERPETUAL", "BTC_USDC", "ETH"],
        silver_root="unused",
        exchange="deribit",
    )
    assert symbols == ["BTC", "ETH"]


def test_resolve_gold_symbols_uses_discovery_when_symbols_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gold_cmd,
        "discover_gold_symbols_for_dataset",
        lambda silver_root, exchange, dataset_id: ["BTC", "ETH"],
    )
    symbols = gold_cmd._resolve_gold_symbols(
        symbols=None,
        silver_root="lake/silver",
        exchange="deribit",
        dataset_id="gold.market.full.m1",
    )
    assert symbols == ["BTC", "ETH"]


def test_resolve_gold_symbols_uses_global_discovery_when_dataset_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gold_cmd, "discover_gold_symbols", lambda silver_root, exchange: ["SOL", "XRP"])
    symbols = gold_cmd._resolve_gold_symbols(
        symbols=None,
        silver_root="lake/silver",
        exchange="deribit",
        dataset_id=None,
    )
    assert symbols == ["SOL", "XRP"]


def test_validate_version_args_rejects_invalid_dataset_version() -> None:
    with pytest.raises(ValueError, match="Invalid --dataset-version"):
        gold_cmd._validate_version_args(auto_version=False, dataset_version="1.2.3", version_base="v1.0.0")


def test_validate_version_args_rejects_invalid_version_base_in_auto_mode() -> None:
    with pytest.raises(ValueError, match="Invalid --version-base"):
        gold_cmd._validate_version_args(auto_version=True, dataset_version="v1.2.3", version_base="1.0.0")


def test_run_gold_build_uses_helpers_and_emits_reports(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured_kwargs: dict[str, object] = {}
    monkeypatch.setattr(gold_cmd, "_resolve_gold_symbols", lambda **kwargs: ["BTC"])
    monkeypatch.setattr(gold_cmd, "_resolve_dataset_ids", lambda dataset_id: [dataset_id or "gold.market.full.m1"])
    monkeypatch.setattr(gold_cmd, "_validate_version_args", lambda **kwargs: None)

    class _Report:
        rows_out = 1
        parquet_path = "/tmp/data.parquet"

        def to_dict(self) -> dict[str, object]:
            return {"symbol": "BTC", "dataset_id": "gold.market.full.m1", "rows_out": 1}

    def _build_gold_for_symbol(**kwargs: object) -> _Report:
        captured_kwargs.update(kwargs)
        return _Report()

    monkeypatch.setattr(gold_cmd, "build_gold_for_symbol", _build_gold_for_symbol)

    args = gold_args()
    gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))
    payload = capsys.readouterr().out
    assert "gold.market.full.m1" in payload
    assert captured_kwargs["keep_last_versions"] == 3


def test_run_gold_build_skips_symbol_on_value_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(gold_cmd, "_resolve_dataset_ids", lambda dataset_id: [dataset_id or "gold.market.full.m1"])
    monkeypatch.setattr(gold_cmd, "_resolve_gold_symbols", lambda **kwargs: ["BTC"])
    monkeypatch.setattr(gold_cmd, "_validate_version_args", lambda **kwargs: None)

    def _raise_for_build(**kwargs: object) -> object:
        raise ValueError("missing silver prerequisite")

    monkeypatch.setattr(gold_cmd, "build_gold_for_symbol", _raise_for_build)

    args = gold_args()
    with caplog.at_level(logging.INFO):
        gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))
    payload = capsys.readouterr().out
    assert payload.strip() == '{\n  "reports": []\n}'
    assert (
        "Gold dataset skipped symbol=BTC dataset_id=gold.market.full.m1 reason=missing silver prerequisite"
        in caplog.text
    )


def test_run_gold_build_runs_history_full_minute_before_derived_timeframes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[str] = []

    monkeypatch.setattr(
        gold_cmd,
        "_resolve_dataset_ids",
        lambda dataset_id: [dataset_id or "gold.history.full.m5"],
    )
    monkeypatch.setattr(gold_cmd, "_resolve_gold_symbols", lambda **kwargs: ["BTC"])
    monkeypatch.setattr(gold_cmd, "_validate_version_args", lambda **kwargs: None)

    class _Report:
        rows_out = 1
        parquet_path = "/tmp/data.parquet"

        def to_dict(self) -> dict[str, object]:
            return {"dataset_id": built[-1], "rows_out": 1}

    def _build_gold_for_symbol(**kwargs: object) -> _Report:
        built.append(str(kwargs["dataset_id"]))
        return _Report()

    monkeypatch.setattr(gold_cmd, "build_gold_for_symbol", _build_gold_for_symbol)

    args = gold_args(dataset_id="gold.history.full.m5")
    gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))

    assert built == ["gold.history.full.m1", "gold.history.full.m5"]


def test_run_gold_build_runs_live_full_minute_before_derived_timeframes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[str] = []

    monkeypatch.setattr(
        gold_cmd,
        "_resolve_dataset_ids",
        lambda dataset_id: [dataset_id or "gold.live.full.m5"],
    )
    monkeypatch.setattr(gold_cmd, "_resolve_gold_symbols", lambda **kwargs: ["BTC"])
    monkeypatch.setattr(gold_cmd, "_validate_version_args", lambda **kwargs: None)

    class _Report:
        rows_out = 1
        parquet_path = "/tmp/data.parquet"

        def to_dict(self) -> dict[str, object]:
            return {"dataset_id": built[-1], "rows_out": 1}

    def _build_gold_for_symbol(**kwargs: object) -> _Report:
        built.append(str(kwargs["dataset_id"]))
        return _Report()

    monkeypatch.setattr(gold_cmd, "build_gold_for_symbol", _build_gold_for_symbol)

    args = gold_args(dataset_id="gold.live.full.m5")
    gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))

    assert built == ["gold.live.full.m1", "gold.live.full.m5"]


def test_run_gold_build_runs_live_extended_minute_before_derived_timeframes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[str] = []

    monkeypatch.setattr(
        gold_cmd,
        "_resolve_dataset_ids",
        lambda dataset_id: [dataset_id or "gold.live.extended.m5"],
    )
    monkeypatch.setattr(gold_cmd, "_resolve_gold_symbols", lambda **kwargs: ["BTC"])
    monkeypatch.setattr(gold_cmd, "_validate_version_args", lambda **kwargs: None)

    class _Report:
        rows_out = 1
        parquet_path = "/tmp/data.parquet"

        def to_dict(self) -> dict[str, object]:
            return {"dataset_id": built[-1], "rows_out": 1}

    def _build_gold_for_symbol(**kwargs: object) -> _Report:
        built.append(str(kwargs["dataset_id"]))
        return _Report()

    monkeypatch.setattr(gold_cmd, "build_gold_for_symbol", _build_gold_for_symbol)

    args = gold_args(dataset_id="gold.live.extended.m5")
    gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))

    assert built == ["gold.live.extended.m1", "gold.live.extended.m5"]


def test_run_gold_build_rejects_invalid_retention_keep_versions() -> None:
    args = gold_args(retention_keep_versions=0, no_json_output=True)
    with pytest.raises(ValueError, match="retention-keep-versions"):
        gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))


def test_run_gold_build_rejects_non_fixed_retention_keep_versions() -> None:
    args = gold_args(retention_keep_versions=4, no_json_output=True)
    with pytest.raises(ValueError, match="fixed at 3 versions"):
        gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))


def test_run_gold_build_rejects_invalid_maxprocesses() -> None:
    args = gold_args(maxprocesses=0, no_json_output=True)

    with pytest.raises(ValueError, match="maxprocesses"):
        gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))
