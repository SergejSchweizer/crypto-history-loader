"""Integration tests for forward-looking Gold prediction targets."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from application.dataset_contracts import gold_dataset_contract
from application.services.gold_service import build_gold_for_symbol
from tests.test_gold_regime_features import _write_required_sources_for_timestamps

pl = pytest.importorskip("polars")


def _manifest(path: str | None) -> dict[str, object]:
    assert path is not None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_prediction_targets_are_separate_forward_looking_dataset(tmp_path: Path) -> None:
    """Forward-looking labels should live outside the regime feature dataset."""

    timestamps = [datetime(2026, 5, 1, tzinfo=UTC) + timedelta(minutes=index) for index in range(65)]
    prices = [100.0 + index for index in range(65)]
    silver = tmp_path / "silver"
    _write_required_sources_for_timestamps(silver, timestamps, perp_closes=prices)

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-targets"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.prediction_targets.m1",
    )
    targets = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert targets.columns == [
        "timestamp_m1",
        "exchange",
        "symbol",
        "target_forward_return_1h",
        "target_forward_drawdown_1h",
        "target_cost_adjusted_return_1h",
        "target_future_rv_1h",
        "target_future_iv_spread_change_1h",
        "label_regime_shift_1h",
        "target_forward_return_4h",
        "target_forward_drawdown_4h",
        "target_cost_adjusted_return_4h",
        "target_future_rv_4h",
        "target_future_iv_spread_change_4h",
        "label_regime_shift_4h",
        "target_forward_return_1d",
        "target_forward_drawdown_1d",
        "target_cost_adjusted_return_1d",
        "target_future_rv_1d",
        "target_future_iv_spread_change_1d",
        "label_regime_shift_1d",
    ]
    assert targets["target_forward_return_1h"].to_list()[0] == pytest.approx(math.log(160.0 / 100.0))
    assert targets["target_future_rv_1h"].to_list()[0] == pytest.approx(82.0)
    assert targets["target_future_iv_spread_change_1h"].to_list()[0] == pytest.approx(60.0)
    assert targets["target_cost_adjusted_return_1h"].to_list()[0] == pytest.approx(
        math.log(160.0 / 100.0) - 0.0002 - (0.001 * 60 / 480.0)
    )
    assert targets["label_regime_shift_1h"].to_list()[0] is False
    assert targets["target_forward_return_1h"].to_list()[-60:] == [None for _index in range(60)]
    assert targets["target_forward_return_4h"].null_count() == targets.height
    assert targets["target_forward_return_1d"].null_count() == targets.height
    definitions = manifest["prediction_target_definitions"]
    assert definitions["horizons"] == {"1h": 60, "4h": 240, "1d": 1440}
    assert definitions["transaction_cost_bps"] == 2.0
    assert manifest["feature_metadata"]["target_forward_return_1h"]["source_dataset"] == "gold_prediction_targets"

    regime_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-regime"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.market.regime_features.m1",
    )
    regime = pl.read_parquet(regime_report.parquet_path)
    assert not any(column.startswith(("target_", "label_")) for column in regime.columns)


def test_prediction_target_contract_declares_separate_sources() -> None:
    """Prediction targets should have their own Gold contract."""

    contract = gold_dataset_contract("gold.market.prediction_targets.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "perps_ohlcv",
        "funding_1m_feature",
        "realized_volatility_1m_feature",
        "iv_rv_1m_feature",
    ]
    assert contract.optional_requirements == ()
