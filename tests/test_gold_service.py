"""Tests for gold transformation service."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services.gold_service import (
    GoldBuildReport,
    _build_history_full_derived_for_symbol,
    _bump_semver,
    _contract_bump_level,
    _dataset_includes_l2,
    _dataset_requirements,
    _feature_hash,
    _feature_source_dataset,
    _git_commit_hash,
    _history_full_derived_interval,
    _history_full_source_dataset_id,
    _iso_utc,
    _json_payload_hash,
    _parse_semver,
    _read_latest_gold_dataset_artifact,
    build_gold_for_symbol,
    discover_gold_symbols,
    discover_gold_symbols_for_dataset,
    normalize_symbol,
    validate_gold_retention_keep_versions,
)

pl = pytest.importorskip("polars")


def test_parse_and_bump_semver() -> None:
    assert _parse_semver("v1.2.3") == (1, 2, 3)
    assert _bump_semver("v1.2.3", "major") == "v2.0.0"
    assert _bump_semver("v1.2.3", "minor") == "v1.3.0"
    assert _bump_semver("v1.2.3", "patch") == "v1.2.4"
    assert _bump_semver("v1.2.3", "none") == "v1.2.3"
    with pytest.raises(ValueError, match="Invalid semantic version"):
        _parse_semver("1.2.3")
    with pytest.raises(ValueError, match="Unsupported semver bump level"):
        _bump_semver("v1.2.3", "x")


def test_validate_gold_retention_keep_versions_is_fixed_to_three() -> None:
    assert validate_gold_retention_keep_versions(3) == 3
    with pytest.raises(ValueError, match="fixed at 3 versions"):
        validate_gold_retention_keep_versions(4)


def test_contract_bump_level_branches() -> None:
    current = {
        "columns": ["a", "b"],
        "join_policy": "full_outer_coalesce",
        "source_dataset_keys": ["spot_ohlcv_1m"],
    }
    prev_invalid = {
        "contract_signature": {"columns": "x", "join_policy": "full_outer_coalesce", "source_dataset_keys": []}
    }
    assert _contract_bump_level(
        prev_invalid, current, previous_source_data_hash="h1", current_source_data_hash="h1"
    ) == (
        "major",
        "invalid_contract_signature",
    )
    prev_join = {
        "contract_signature": {"columns": ["a", "b"], "join_policy": "inner", "source_dataset_keys": ["spot_ohlcv_1m"]}
    }
    assert _contract_bump_level(prev_join, current, previous_source_data_hash="h1", current_source_data_hash="h1") == (
        "major",
        "join_policy_changed",
    )
    prev_removed_key = {
        "contract_signature": {
            "columns": ["a", "b"],
            "join_policy": "full_outer_coalesce",
            "source_dataset_keys": ["spot_ohlcv_1m", "perps_ohlcv_1m"],
        }
    }
    assert _contract_bump_level(
        prev_removed_key, current, previous_source_data_hash="h1", current_source_data_hash="h1"
    ) == (
        "major",
        "source_dataset_removed",
    )
    prev_missing_col = {
        "contract_signature": {
            "columns": ["a", "b", "c"],
            "join_policy": "full_outer_coalesce",
            "source_dataset_keys": ["spot_ohlcv_1m"],
        }
    }
    assert _contract_bump_level(
        prev_missing_col, current, previous_source_data_hash="h1", current_source_data_hash="h1"
    ) == (
        "major",
        "column_removed_or_renamed",
    )
    prev_order = {
        "contract_signature": {
            "columns": ["b", "a"],
            "join_policy": "full_outer_coalesce",
            "source_dataset_keys": ["spot_ohlcv_1m"],
        }
    }
    assert _contract_bump_level(prev_order, current, previous_source_data_hash="h1", current_source_data_hash="h1") == (
        "major",
        "column_order_changed",
    )
    prev_same = {"contract_signature": current}
    assert _contract_bump_level(
        prev_same, current, previous_source_data_hash="old", current_source_data_hash="new"
    ) == (
        "patch",
        "source_data_changed",
    )
    assert _contract_bump_level(
        prev_same, current, previous_source_data_hash="same", current_source_data_hash="same"
    ) == (
        "none",
        "no_change",
    )


def test_dataset_specs_symbol_normalization_and_hash_helpers() -> None:
    assert _dataset_requirements("gold.market.full.m1")
    assert _dataset_includes_l2("gold.hybrid.full_l2.m1") is True
    assert _dataset_includes_l2("gold.market.full.m1") is False
    with pytest.raises(ValueError, match="Unsupported dataset_id"):
        _dataset_requirements("gold.market.unknown")
    with pytest.raises(ValueError, match="Unsupported dataset_id"):
        _dataset_includes_l2("gold.market.unknown")

    assert normalize_symbol("btc/usdc") == "BTC"
    assert normalize_symbol("eth-perpetual") == "ETH"
    assert _feature_source_dataset("spot_ohlcv_close_price") == "spot_ohlcv_1m"
    assert _feature_source_dataset("perp_close_price") == "perps_ohlcv_1m"
    assert _feature_source_dataset("open_interest_observation_lag_sec") == "open_interest_1m_feature"
    assert _feature_source_dataset("funding_rate_last_known") == "funding_1m_feature"
    assert _feature_source_dataset("perps_trades_open_price") == "perps_trades_1m_feature"
    assert _feature_source_dataset("options_trades_open_price") == "options_trades_1m_feature"


def test_gold_service_small_contract_helpers_and_artifact_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Gold helper errors should identify missing or mismatched lineage artifacts."""

    report = GoldBuildReport(
        exchange="deribit",
        symbol="BTC",
        rows_out=1,
        columns=["timestamp_m1"],
        min_timestamp=None,
        max_timestamp=None,
        parquet_path="x.parquet",
        manifest_path=None,
        plot_path=None,
        hash_string="h",
        dataset_id="gold.history.full.m1",
        dataset_version="v1.0.0",
        feature_set_hash="f",
        source_data_hash="s",
        git_commit_hash="g",
        version_bump_level="manual",
        version_bump_reason="manual_version",
        previous_version=None,
    )
    assert report.to_dict()["dataset_id"] == "gold.history.full.m1"
    assert _iso_utc(None) is None
    assert _history_full_derived_interval("gold.history.full.m5") == "5m"
    assert _history_full_source_dataset_id("gold.history.full.m5") == "gold.history.full.m1"
    with pytest.raises(ValueError, match="Missing gold dataset"):
        _read_latest_gold_dataset_artifact(
            gold_root=str(tmp_path), dataset_id="gold.history.full.m1", exchange="deribit", symbol="BTC"
        )
    artifact_dir = (
        tmp_path
        / "dataset_id=gold.history.full.m1"
        / "dataset_type=gold_symbol_dataset"
        / "feature_set_version=v1.0.0"
        / "exchange=deribit"
        / "symbol=BTC"
    )
    artifact_dir.mkdir(parents=True)
    parquet = artifact_dir / "BTC.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(parquet)
    with pytest.raises(ValueError, match="Missing gold manifest"):
        _read_latest_gold_dataset_artifact(
            gold_root=str(tmp_path), dataset_id="gold.history.full.m1", exchange="deribit", symbol="BTC"
        )
    parquet.with_suffix(".json").write_text('{"dataset_id":"other"}', encoding="utf-8")
    with pytest.raises(ValueError, match="lineage mismatch"):
        _read_latest_gold_dataset_artifact(
            gold_root=str(tmp_path), dataset_id="gold.history.full.m1", exchange="deribit", symbol="BTC"
        )
    monkeypatch.setattr("application.services.gold_service.subprocess.check_output", lambda *_args, **_kwargs: "")
    assert _git_commit_hash() == "nogit"
    monkeypatch.setattr(
        "application.services.gold_service.subprocess.check_output",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("git unavailable")),
    )
    assert _git_commit_hash() == "nogit"


def test_gold_service_rejects_invalid_derived_dataset_ids() -> None:
    """Derived builders should reject unsupported lineage identifiers before I/O."""

    with pytest.raises(ValueError, match="Unsupported derived"):
        _build_history_full_derived_for_symbol(
            gold_root="/tmp/unused",
            exchange="deribit",
            symbol="BTC",
            dataset_id="gold.unknown.m5",
            dataset_version="v1.0.0",
            auto_version=False,
            version_base="v1.0.0",
            keep_last_versions=3,
        )


def test_gold_service_delegation_boundaries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Application Gold wrappers should preserve their explicit frame-service boundaries."""

    import application.services.gold_service as service

    sentinel = object()
    delegates = {
        "read_latest_l2_gold_frame": (
            "_read_latest_l2_gold_frame",
            {"l2_root": "x", "exchange": "deribit", "symbol": "BTC"},
        ),
        "prepare_l2": ("_prepare_l2", {"pl": sentinel, "frame": sentinel, "symbol": "BTC"}),
        "l2_invalid_mask_expr": ("_l2_invalid_mask_expr", {"pl": sentinel, "columns": set()}),
        "validate_or_filter_l2_quality": (
            "_validate_or_filter_l2_quality",
            {"pl": sentinel, "frame": sentinel, "mode": "strict"},
        ),
        "prepare_spot_ohlcv_or_perp": (
            "_prepare_spot_ohlcv_or_perp",
            {"pl": sentinel, "frame": sentinel, "prefix": "spot", "symbol": "BTC"},
        ),
        "prepare_open_interest": ("_prepare_open_interest", {"pl": sentinel, "frame": sentinel, "symbol": "BTC"}),
        "prepare_funding": ("_prepare_funding", {"pl": sentinel, "frame": sentinel, "symbol": "BTC"}),
        "prepare_trades": ("_prepare_trades", {"pl": sentinel, "frame": sentinel, "symbol": "BTC"}),
        "prepare_options_trades": ("_prepare_options_trades", {"pl": sentinel, "frame": sentinel, "symbol": "BTC"}),
        "prepare_volatility_index_data": (
            "_prepare_volatility_index_data",
            {"pl": sentinel, "frame": sentinel, "symbol": "BTC"},
        ),
        "prepare_dataset_frame": (
            "_prepare_dataset_frame",
            {"pl": sentinel, "dataset_type": "spot_ohlcv", "frame": sentinel, "symbol": "BTC"},
        ),
        "optional_feature_schema": ("_optional_feature_schema", {"pl": sentinel, "dataset_type": "spot_ohlcv"}),
        "build_minute_grid": (
            "_build_minute_grid",
            {"pl": sentinel, "prepared": [], "exchange": "deribit", "symbol": "BTC"},
        ),
    }
    for target_name, (wrapper_name, kwargs) in delegates.items():
        monkeypatch.setattr(service.gold_frames, target_name, lambda *_args, **_kwargs: sentinel)
        assert getattr(service, wrapper_name)(**kwargs) is sentinel
    monkeypatch.setattr(service.gold_frames, "discover_symbols_for_dataset", lambda **_kwargs: {"BTC"})
    assert service.discover_gold_symbols_for_dataset("silver", "deribit", "gold.history.full.m5") == ["BTC"]
    assert service._strategy_feature_lookbacks("gold.market.full.m1") == {}
    assert service._prediction_target_definitions("gold.market.full.m1") == {}
    assert service._origin_repository("gold.live.full.m1") == "crypto-live-loader"
    assert service._origin_repository("gold.history.full.m1") == "crypto-history-loader"
    assert service._add_strategy_feature_families(sentinel, sentinel, "gold.market.full.m1") is sentinel
    assert service._add_prediction_targets(sentinel, sentinel, "gold.market.full.m1") is sentinel
    assert service._add_live_extended_feature_families(sentinel, sentinel, "gold.market.full.m1") is sentinel
    assert _feature_source_dataset("historical_prediction_perps_rv_1h") == "historical_prediction_1m_feature"
    assert _feature_source_dataset("volatility_index_data_value") == "volatility_index_data_observed"
    assert _feature_source_dataset("volatility_index_value") == "volatility_index_data_observed"
    assert _feature_source_dataset("rv_1h") == "realized_volatility_1m_feature"
    assert _feature_source_dataset("iv_minus_rv_1h") == "iv_rv_1m_feature"
    assert _feature_source_dataset("perps_l2_spread") == "perps_l2_1m_feature"
    assert _feature_source_dataset("options_l2_contract_count") == "options_l2_1m_feature"
    assert _feature_source_dataset("options_surface_atm_iv") == "options_surface_1m_feature"
    assert _feature_source_dataset("index_price") == "index_price_1m_feature"
    assert _feature_source_dataset("futures_summary_mark_price") == "futures_summary_1m_feature"
    assert _feature_source_dataset("strategy_momentum_log_return_1m") == "gold_strategy_features"
    assert _feature_source_dataset("strategy_reversion_vwap_distance_15m") == "gold_strategy_features"
    assert _feature_source_dataset("target_forward_return_1h") == "gold_prediction_targets"
    assert _feature_source_dataset("label_regime_shift_1h") == "gold_prediction_targets"
    assert _feature_source_dataset("historical_volatility_reference") == ("historical_volatility_observed")
    assert _feature_source_dataset("l2_coverage_ratio") == "gold_merged"
    assert _feature_source_dataset("custom_col") == "gold_merged"

    assert _feature_hash(["a", "b"]) == _feature_hash(["a", "b"])
    assert _feature_hash(["a", "b"]) != _feature_hash(["b", "a"])
    assert _json_payload_hash({"a": 1, "b": 2}) == _json_payload_hash({"b": 2, "a": 1})


def _write_silver_month(
    root: Path,
    *,
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    target = (
        root
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"{symbol}_{month.replace('-', '_')}.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([dict(row) for row in rows]).write_parquet(target)


def _require_manifest_path(report: object) -> Path:
    manifest_path = getattr(report, "manifest_path", None)
    assert isinstance(manifest_path, str)
    return Path(manifest_path)


def _write_l2_gold_parquet(root: Path, *, symbol: str, exchange: str, rows: list[dict[str, object]]) -> None:
    target = (
        root
        / "dataset_id=gold.l2.micro.m1"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "version=v1.0.0"
        / "build_id=testhash_abcdef12_12345678"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)


def _write_perps_trades_1m_feature_month(
    root: Path,
    *,
    exchange: str,
    symbol: str,
    month: str,
    timestamps: list[datetime],
) -> None:
    rows: list[dict[str, object]] = []
    for idx, ts in enumerate(timestamps):
        rows.append(
            {
                "timestamp_m1": ts,
                "exchange": exchange,
                "symbol": symbol,
                "instrument_type": "perp",
                "open_price": 100.0 + idx,
                "high_price": 101.0 + idx,
                "low_price": 99.0 + idx,
                "close_price": 100.5 + idx,
                "volume": 10.0 + idx,
                "quote_volume": 1000.0 + idx,
                "trade_count": 5 + idx,
                "buy_volume": 6.0 + idx,
                "sell_volume": 4.0,
                "buy_trade_count": 3 + idx,
                "sell_trade_count": 2,
                "buy_volume_share": 0.6,
            }
        )
    _write_silver_month(
        root,
        dataset_type="perps_trades_1m_feature",
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        month=month,
        rows=rows,
    )


def _write_options_trades_1m_feature_month(
    root: Path,
    *,
    exchange: str,
    symbol: str,
    month: str,
    timestamps: list[datetime],
) -> None:
    rows: list[dict[str, object]] = []
    for idx, ts in enumerate(timestamps):
        rows.append(
            {
                "timestamp_m1": ts,
                "exchange": exchange,
                "symbol": symbol,
                "instrument_type": "option",
                "open_price": 10.0 + idx,
                "high_price": 11.0 + idx,
                "low_price": 9.0 + idx,
                "close_price": 10.5 + idx,
                "volume": 4.0 + idx,
                "quote_volume": 40.0 + idx,
                "trade_count": 2 + idx,
                "buy_volume": 2.0 + idx,
                "sell_volume": 2.0,
                "buy_trade_count": 1 + idx,
                "sell_trade_count": 1,
                "buy_volume_share": 0.5,
            }
        )
    _write_silver_month(
        root,
        dataset_type="options_trades_1m_feature",
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        month=month,
        rows=rows,
    )


def _write_volatility_observed_month(
    root: Path,
    *,
    dataset_type: str,
    exchange: str,
    symbol: str,
    month: str,
    timestamps: list[datetime],
) -> None:
    rows: list[dict[str, object]] = []
    for idx, ts in enumerate(timestamps):
        rows.append(
            {
                "timestamp": ts,
                "exchange": exchange,
                "symbol": symbol,
                "instrument_type": "perp",
                "dataset_type": dataset_type.replace("_observed", ""),
                "volatility_value": 50.0 + idx,
                "volatility_open": 49.0 + idx,
                "volatility_high": 51.0 + idx,
                "volatility_low": 48.5 + idx,
                "volatility_close": 50.0 + idx,
                "volatility_source_timestamp": ts,
                "ingested_at": ts,
                "source_endpoint": "public_volatility",
            }
        )
    _write_silver_month(
        root,
        dataset_type=dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        month=month,
        rows=rows,
    )


def test_build_gold_for_symbol_writes_hashed_parquet_and_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.5,
                "high_price": 2.5,
                "low_price": 1.0,
                "close_price": 2.0,
                "volume": 11.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.5,
                "close_price": 10.5,
                "volume": 110.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.5,
                "high_price": 11.5,
                "low_price": 10.0,
                "close_price": 11.0,
                "volume": 111.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1000.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            },
            {
                "timestamp_m1": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1001.0,
                "open_interest_is_observed": False,
                "open_interest_is_ffill": True,
                "minutes_since_open_interest_observation": 1,
                "open_interest_observation_lag_sec": 60,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            },
            {
                "timestamp": t1,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 1,
                "is_funding_observation_minute": False,
                "funding_data_available": True,
            },
        ],
    )
    _write_perps_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )

    assert discover_gold_symbols(str(silver), exchange) == [symbol]

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        manifest=True,
    )

    parquet_path = Path(report.parquet_path)
    assert parquet_path.name.startswith("BTC_GOLD_")
    assert parquet_path.suffix == ".parquet"
    assert "dataset_id=gold.market.full.m1" in report.parquet_path
    assert "dataset_type=gold_symbol_dataset" in report.parquet_path
    assert "feature_set_version=v1.0.0" in report.parquet_path
    assert f"exchange={exchange}" in report.parquet_path
    assert f"symbol={symbol}" in report.parquet_path
    assert Path(report.parquet_path).exists()
    assert _require_manifest_path(report).exists()
    assert report.plot_path is None or Path(report.plot_path).exists()
    assert _require_manifest_path(report).name.startswith("BTC_GOLD_")
    assert _require_manifest_path(report).suffix == ".json"

    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["symbol"] == symbol
    assert payload["exchange"] == exchange
    assert payload["rows_out"] == 2
    assert "plot_generated" in payload
    assert "source_silver_datasets" in payload
    assert "spot_ohlcv_1m" in payload["source_silver_datasets"]
    assert "columns" in payload["source_silver_datasets"]["spot_ohlcv_1m"]
    assert "open_time" in payload["source_silver_datasets"]["spot_ohlcv_1m"]["columns"]
    assert "perps_ohlcv_1m" in payload["source_silver_datasets"]
    assert "open_interest_1m_feature" in payload["source_silver_datasets"]
    assert "funding_1m_feature" in payload["source_silver_datasets"]
    assert "perps_trades_1m_feature" in payload["source_silver_datasets"]
    assert "options_trades_1m_feature" in payload["source_silver_datasets"]
    assert "volatility_index_data_observed" in payload["source_silver_datasets"]
    assert payload["source_silver_datasets"]["spot_ohlcv_1m"]["source_symbols"] == ["BTC"]
    assert payload["source_silver_datasets"]["perps_ohlcv_1m"]["source_symbols"] == ["BTC"]
    assert "feature_metadata" in payload
    assert "spot_ohlcv_close_price" in payload["feature_metadata"]
    assert payload["feature_metadata"]["spot_ohlcv_close_price"]["source_exchange"] == exchange
    assert "time_range" in payload["feature_metadata"]["spot_ohlcv_close_price"]
    assert payload["feature_metadata"]["spot_ohlcv_close_price"]["time_range"]["min_timestamp"] is not None
    assert payload["dataset_id"] == "gold.market.full.m1"
    assert payload["dataset_version"] == "v1.0.0"
    assert "feature_set_hash" in payload
    assert "source_data_hash" in payload
    assert "git_commit_hash" in payload
    assert "build_id" in payload
    assert payload["input_artifact_fingerprints"]
    assert payload["incremental_m1_plan"]["changed_months"] == ["2026-05"]

    parquet_mtime_ns = parquet_path.stat().st_mtime_ns
    monkeypatch.setattr(
        "application.services.gold_service._read_dataset_frame",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unchanged build must not read Silver")),
    )
    unchanged = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        manifest=True,
    )
    assert unchanged.parquet_path == report.parquet_path
    assert unchanged.version_bump_reason == "unchanged_input"
    assert parquet_path.stat().st_mtime_ns == parquet_mtime_ns


def test_build_gold_reads_legacy_perp_silver_dataset(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 1.0,
                "high_price": 1.1,
                "low_price": 0.9,
                "close_price": 1.0,
                "volume": 1.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perp",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 10.0,
                "high_price": 10.1,
                "low_price": 9.9,
                "close_price": 10.0,
                "volume": 10.0,
            }
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol="BTC",
        dataset_id="gold.market.core.m1",
    )

    written = pl.read_parquet(report.parquet_path)
    assert written["perp_close_price"].to_list() == [10.0]


def test_build_gold_prefers_canonical_perps_ohlcv_over_legacy_perp(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 1.0,
                "high_price": 1.1,
                "low_price": 0.9,
                "close_price": 1.0,
                "volume": 1.0,
            }
        ],
    )
    for dataset_type, close_price in [("perp", 10.0), ("perps_ohlcv", 20.0)]:
        _write_silver_month(
            silver,
            dataset_type=dataset_type,
            exchange=exchange,
            symbol="BTC-PERPETUAL",
            timeframe="1m",
            month="2026-05",
            rows=[
                {
                    "open_time": t0,
                    "exchange": exchange,
                    "symbol": "BTC",
                    "open_price": close_price,
                    "high_price": close_price + 0.1,
                    "low_price": close_price - 0.1,
                    "close_price": close_price,
                    "volume": close_price,
                }
            ],
        )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol="BTC",
        dataset_id="gold.market.core.m1",
    )

    written = pl.read_parquet(report.parquet_path)
    assert written["perp_close_price"].to_list() == [20.0]


def test_build_gold_dedupes_overlapping_silver_partitions_by_recency(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    spot_ohlcv_timeframe_dir = (
        silver / "dataset_type=spot_ohlcv" / f"exchange={exchange}" / "symbol=BTC_USDC" / "timeframe=1m"
    )
    spot_ohlcv_timeframe_dir.mkdir(parents=True, exist_ok=True)
    old_spot_ohlcv_path = spot_ohlcv_timeframe_dir / "BTC_2026_05_old.parquet"
    new_spot_ohlcv_path = spot_ohlcv_timeframe_dir / "BTC_2026_05_new.parquet"
    pl.DataFrame(
        [
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "volume": 10.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 9.0,
                "high_price": 9.0,
                "low_price": 9.0,
                "close_price": 9.0,
                "volume": 11.0,
            },
        ]
    ).write_parquet(old_spot_ohlcv_path)
    pl.DataFrame(
        [
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 2.0,
                "high_price": 2.0,
                "low_price": 2.0,
                "close_price": 2.0,
                "volume": 20.0,
            }
        ]
    ).write_parquet(new_spot_ohlcv_path)
    now = datetime.now().timestamp()
    os.utime(old_spot_ohlcv_path, (now - 120.0, now - 120.0))
    os.utime(new_spot_ohlcv_path, (now, now))

    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 10.0,
                "low_price": 10.0,
                "close_price": 10.0,
                "volume": 100.0,
            }
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.market.core.m1",
    )
    # Both partition files are read: for the overlapping t0 minute the freshest file wins,
    # while t1 (only present in the older file) is still preserved in the union.
    assert report.rows_out == 2

    written = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    assert written.get_column("spot_ohlcv_open_price").to_list() == [2.0, 9.0]

    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["source_silver_datasets"]["spot_ohlcv_1m"]["rows"] == 2


def test_build_gold_for_symbol_normalizes_input_symbol(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)

    for dataset_type, rows in [
        (
            "spot_ohlcv",
            [
                {
                    "open_time": t0,
                    "exchange": exchange,
                    "symbol": "BTC_USDC",
                    "open_price": 1.0,
                    "high_price": 1.1,
                    "low_price": 0.9,
                    "close_price": 1.0,
                    "volume": 1.0,
                }
            ],
        ),
        (
            "perps_ohlcv",
            [
                {
                    "open_time": t0,
                    "exchange": exchange,
                    "symbol": "BTC-PERPETUAL",
                    "open_price": 1.0,
                    "high_price": 1.1,
                    "low_price": 0.9,
                    "close_price": 1.0,
                    "volume": 1.0,
                }
            ],
        ),
        (
            "open_interest_1m_feature",
            [
                {
                    "timestamp_m1": t0,
                    "exchange": exchange,
                    "symbol": "BTC-PERPETUAL",
                    "open_interest": 1.0,
                    "open_interest_is_observed": True,
                    "open_interest_is_ffill": False,
                    "minutes_since_open_interest_observation": 0,
                    "open_interest_observation_lag_sec": 0,
                }
            ],
        ),
        (
            "funding_1m_feature",
            [
                {
                    "timestamp": t0,
                    "exchange": exchange,
                    "symbol": "BTC-PERPETUAL",
                    "funding_rate_last_known": 0.0,
                    "minutes_since_funding": 0,
                    "is_funding_observation_minute": True,
                    "funding_data_available": True,
                }
            ],
        ),
        (
            "perps_trades_1m_feature",
            [
                {
                    "timestamp_m1": t0,
                    "exchange": exchange,
                    "symbol": "BTC-PERPETUAL",
                    "instrument_type": "perp",
                    "open_price": 1.0,
                    "high_price": 1.1,
                    "low_price": 0.9,
                    "close_price": 1.0,
                    "volume": 1.0,
                    "quote_volume": 1.0,
                    "trade_count": 1,
                    "buy_volume": 1.0,
                    "sell_volume": 0.0,
                    "buy_trade_count": 1,
                    "sell_trade_count": 0,
                    "buy_volume_share": 1.0,
                }
            ],
        ),
        (
            "options_trades_1m_feature",
            [
                {
                    "timestamp_m1": t0,
                    "exchange": exchange,
                    "symbol": "BTC",
                    "instrument_type": "option",
                    "open_price": 1.0,
                    "high_price": 1.1,
                    "low_price": 0.9,
                    "close_price": 1.0,
                    "volume": 1.0,
                    "quote_volume": 1.0,
                    "trade_count": 1,
                    "buy_volume": 1.0,
                    "sell_volume": 0.0,
                    "buy_trade_count": 1,
                    "sell_trade_count": 0,
                    "buy_volume_share": 1.0,
                }
            ],
        ),
        (
            "volatility_index_data_observed",
            [
                {
                    "timestamp": t0,
                    "exchange": exchange,
                    "symbol": "BTC",
                    "instrument_type": "perp",
                    "dataset_type": "volatility_index_data",
                    "volatility_value": 50.0,
                    "volatility_open": 49.0,
                    "volatility_high": 51.0,
                    "volatility_low": 48.5,
                    "volatility_close": 50.0,
                    "volatility_source_timestamp": t0,
                    "ingested_at": t0,
                    "source_endpoint": "public_volatility",
                }
            ],
        ),
        (
            "volatility_index_data_observed",
            [
                {
                    "timestamp": t0,
                    "exchange": exchange,
                    "symbol": "BTC",
                    "instrument_type": "perp",
                    "dataset_type": "volatility_index_data",
                    "volatility_value": 70.0,
                    "volatility_open": 69.0,
                    "volatility_high": 71.0,
                    "volatility_low": 68.5,
                    "volatility_close": 70.0,
                    "volatility_source_timestamp": t0,
                    "ingested_at": t0,
                    "source_endpoint": "public_volatility",
                }
            ],
        ),
    ]:
        _write_silver_month(
            silver,
            dataset_type=dataset_type,
            exchange=exchange,
            symbol="BTC-PERPETUAL",
            timeframe="1m",
            month="2026-05",
            rows=rows,
        )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol="btc_perpetual",
    )
    assert "dataset_id=gold.market.full.m1" in report.parquet_path


def test_build_gold_iv_rv_uses_historical_sources_without_forward_looking_reference(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    exchange = "deribit"
    symbol = "BTC"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)
    timestamps = [t0, t1]

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": timestamp,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 100.0 + index,
                "high_price": 101.0 + index,
                "low_price": 99.0 + index,
                "close_price": 100.5 + index,
                "volume": 10.0 + index,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": timestamp,
                "exchange": exchange,
                "symbol": "BTC-PERPETUAL",
                "open_price": 101.0 + index,
                "high_price": 102.0 + index,
                "low_price": 100.0 + index,
                "close_price": 101.5 + index,
                "volume": 20.0 + index,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": timestamp,
                "exchange": exchange,
                "symbol": "BTC-PERPETUAL",
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": index,
                "is_funding_observation_minute": index == 0,
                "funding_data_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": exchange,
                "symbol": "BTC-PERPETUAL",
                "open_interest": 1000.0 + index,
                "open_interest_is_observed": index == 0,
                "open_interest_is_ffill": index == 1,
                "minutes_since_open_interest_observation": index,
                "open_interest_observation_lag_sec": index * 60,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="realized_volatility_1m_feature",
        exchange=exchange,
        symbol="BTC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": exchange,
                "symbol": "BTC",
                "rv_5m": 0.01 + index,
                "rv_15m": 0.02 + index,
                "rv_1h": 0.03 + index,
                "rv_4h": 0.04 + index,
                "rv_1d": 0.05 + index,
                "rv_5m_annualized_pct": 22.0 + index,
                "rv_15m_annualized_pct": 21.0 + index,
                "rv_1h_annualized_pct": 20.0 + index,
                "rv_4h_annualized_pct": 19.0 + index,
                "rv_1d_annualized_pct": 18.0 + index,
                "rv_30d": 0.06 + index,
                "rv_30d_annualized_pct": 45.0 + index,
                "parkinson_rv_1h": 0.06 + index,
                "jump_proxy": 0.001 * index,
                "spot_available": True,
                "perps_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="iv_rv_1m_feature",
        exchange=exchange,
        symbol="BTC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": exchange,
                "symbol": "BTC",
                "iv_minus_rv_1h": 5.0 + index,
                "iv_minus_rv_1d": 3.0 + index,
                "iv_rv_ratio_1h": 1.2 + index,
                "iv_rv_ratio_1d": 1.1 + index,
                "iv_rv_spread_30d_pct": 15.0 + index,
                "iv_rv_ratio_30d": 1.3 + index,
                "iv_rv_zscore_1d": 0.5 + index,
                "iv_rv_percentile_30d": 0.7 + index,
                "minutes_since_iv_observation": index,
                "minutes_since_rv_observation": index,
                "iv_available": True,
                "rv_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="historical_volatility_observed",
        exchange=exchange,
        symbol="BTC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t1,
                "exchange": exchange,
                "symbol": "BTC",
                "historical_volatility": 42.0,
                "historical_volatility_source_timestamp": t1,
                "ingested_at": t1,
                "source_endpoint": "public_get_historical_volatility",
            }
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.market.iv_rv.m1",
    )

    frame = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))

    assert payload["required_source_datasets"] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "realized_volatility_1m_feature",
        "iv_rv_1m_feature",
    ]
    assert payload["optional_source_datasets"] == ["historical_volatility_observed"]
    assert payload["optional_source_availability"]["historical_volatility_observed"]["available"] is True
    assert payload["source_silver_datasets"]["historical_volatility_observed"]["available"] is True
    assert "feature_set_hash" in payload
    assert payload["source_silver_datasets"]["iv_rv_1m_feature"]["rows"] == 2
    assert payload["source_silver_datasets"]["realized_volatility_1m_feature"]["rows"] == 2
    assert frame["timestamp_m1"].to_list() == timestamps
    assert frame["historical_volatility_reference"].to_list() == [None, 42.0]
    assert frame["rv_1h"].to_list() == [0.03, 1.03]
    assert frame["iv_minus_rv_1h"].to_list() == [5.0, 6.0]
    assert frame["spot_ohlcv_close_price"].to_list() == [100.5, 101.5]
    assert frame["perp_close_price"].to_list() == [101.5, 102.5]
    assert frame["funding_rate_last_known"].to_list() == [0.001, 0.001]
    assert frame["open_interest_open_interest"].to_list() == [1000.0, 1001.0]


def test_build_gold_for_symbol_trades_only_dataset(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_perps_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol=symbol,
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.market.perps_trades.m1",
        manifest=True,
    )

    assert "dataset_id=gold.market.perps_trades.m1" in report.parquet_path
    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "gold.market.perps_trades.m1"
    assert "perps_trades_1m_feature" in payload["source_silver_datasets"]


def test_build_gold_for_symbol_options_trades_only_dataset(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol=symbol,
        month="2026-05",
        timestamps=[t0, t1],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.market.options_trades.m1",
        manifest=True,
    )

    assert "dataset_id=gold.market.options_trades.m1" in report.parquet_path
    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "gold.market.options_trades.m1"
    assert "options_trades_1m_feature" in payload["source_silver_datasets"]
    assert report.manifest_path is not None
    assert report.plot_path is not None
    assert _require_manifest_path(report).exists()
    assert Path(report.plot_path).exists()


def test_discover_gold_symbols_requires_trades_dataset(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "volume": 1.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "volume": 1.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_interest": 1.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "funding_rate_last_known": 0.0,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            }
        ],
    )
    assert discover_gold_symbols(str(silver), exchange) == []


def test_discover_gold_symbols_for_extended_history_full_reuses_source_contract(
    tmp_path: Path,
) -> None:
    silver = tmp_path / "silver"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "volume": 1.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "volume": 1.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "open_interest": 1.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "funding_rate_last_known": 0.0,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="historical_prediction_1m_feature",
        exchange=exchange,
        symbol="BTC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "historical_prediction_spot_log_return_1m": 0.0,
                "historical_prediction_perps_log_return_1m": 0.0,
                "historical_prediction_spot_rv_15m": 0.0,
                "historical_prediction_spot_rv_1h": 0.0,
                "historical_prediction_spot_rv_1d": 0.0,
                "historical_prediction_perps_rv_15m": 0.0,
                "historical_prediction_perps_rv_1h": 0.0,
                "historical_prediction_perps_rv_1d": 0.0,
                "historical_prediction_spot_perp_basis": 0.0,
                "historical_prediction_basis_change_1m": 0.0,
                "historical_prediction_basis_zscore_1h": 0.0,
                "historical_prediction_open_interest_delta_1m": 0.0,
                "historical_prediction_open_interest_pct_change_1m": 0.0,
                "historical_prediction_open_interest_zscore_1h": 0.0,
                "historical_prediction_funding_rate_change_1m": 0.0,
                "historical_prediction_funding_rate_zscore_1d": 0.0,
                "historical_prediction_funding_basis_divergence": 0.0,
                "historical_prediction_perps_trade_imbalance": 0.0,
                "historical_prediction_perps_trade_count_zscore_1h": 0.0,
                "historical_prediction_perps_quote_volume_zscore_1h": 0.0,
                "historical_prediction_perps_price_impact_1m": 0.0,
                "historical_prediction_options_trade_imbalance": 0.0,
                "historical_prediction_options_trade_count_zscore_1h": 0.0,
                "historical_prediction_options_quote_volume_zscore_1h": 0.0,
                "historical_prediction_leverage_build_up_signal": 0.0,
                "historical_prediction_short_stress_signal": 0.0,
                "historical_prediction_flow_volatility_pressure": 0.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_trades_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "instrument_type": "perp",
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "volume": 1.0,
                "quote_volume": 1.0,
                "trade_count": 1,
                "buy_volume": 0.5,
                "sell_volume": 0.5,
                "buy_trade_count": 1,
                "sell_trade_count": 0,
                "buy_volume_share": 0.5,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="options_trades_1m_feature",
        exchange=exchange,
        symbol="BTC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": "BTC",
                "instrument_type": "option",
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "volume": 1.0,
                "quote_volume": 1.0,
                "trade_count": 1,
                "buy_volume": 0.5,
                "sell_volume": 0.5,
                "buy_trade_count": 1,
                "sell_trade_count": 0,
                "buy_volume_share": 0.5,
            }
        ],
    )

    assert discover_gold_symbols_for_dataset(str(silver), exchange, "gold.history.extended_full.m1") == ["BTC"]
    assert discover_gold_symbols_for_dataset(str(silver), exchange, "gold.history.extended.m1") == ["BTC"]
    assert discover_gold_symbols_for_dataset(str(silver), exchange, "gold.history.extended.m5") == ["BTC"]
    assert discover_gold_symbols_for_dataset(str(silver), exchange, "gold.history.extended.m30") == ["BTC"]
    assert discover_gold_symbols_for_dataset(str(silver), exchange, "gold.history.extended.h1") == ["BTC"]
    assert discover_gold_symbols_for_dataset(str(silver), exchange, "gold.history.full.m5") == ["BTC"]


def test_build_gold_hybrid_full_l2_contains_l2_features(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.5,
                "high_price": 2.5,
                "low_price": 1.0,
                "close_price": 2.0,
                "volume": 11.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.5,
                "close_price": 10.5,
                "volume": 110.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.5,
                "high_price": 11.5,
                "low_price": 10.0,
                "close_price": 11.0,
                "volume": 111.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1000.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            },
            {
                "timestamp_m1": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1001.0,
                "open_interest_is_observed": False,
                "open_interest_is_ffill": True,
                "minutes_since_open_interest_observation": 1,
                "open_interest_observation_lag_sec": 60,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            },
            {
                "timestamp": t1,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 1,
                "is_funding_observation_minute": False,
                "funding_data_available": True,
            },
        ],
    )
    _write_perps_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol=symbol,
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_l2_gold_parquet(
        gold,
        symbol=symbol,
        exchange=exchange,
        rows=[
            {"ts_minute": t0, "exchange": exchange, "symbol": symbol, "snapshot_count": 5, "coverage_ratio": 0.9},
            {"ts_minute": t1, "exchange": exchange, "symbol": symbol, "snapshot_count": 6, "coverage_ratio": 1.0},
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.hybrid.full_l2.m1",
        manifest=True,
    )
    assert Path(report.parquet_path).exists()
    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["dataset_id"] == "gold.hybrid.full_l2.m1"
    assert "gold_l2_m1" in payload["source_silver_datasets"]
    written = pl.read_parquet(report.parquet_path)
    assert "l2_snapshot_count" in written.columns
    assert "l2_coverage_ratio" in written.columns


def test_build_gold_hybrid_full_l2_uses_requested_exchange_l2(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.5,
                "high_price": 2.5,
                "low_price": 1.0,
                "close_price": 2.0,
                "volume": 11.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.5,
                "close_price": 10.5,
                "volume": 110.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.5,
                "high_price": 11.5,
                "low_price": 10.0,
                "close_price": 11.0,
                "volume": 111.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1000.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            },
            {
                "timestamp_m1": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1001.0,
                "open_interest_is_observed": False,
                "open_interest_is_ffill": True,
                "minutes_since_open_interest_observation": 1,
                "open_interest_observation_lag_sec": 60,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            },
            {
                "timestamp": t1,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 1,
                "is_funding_observation_minute": False,
                "funding_data_available": True,
            },
        ],
    )
    _write_perps_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol=symbol,
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    # Valid artifact for requested exchange
    _write_l2_gold_parquet(
        gold,
        symbol=symbol,
        exchange=exchange,
        rows=[
            {"ts_minute": t0, "exchange": exchange, "symbol": symbol, "snapshot_count": 5, "coverage_ratio": 0.8},
            {"ts_minute": t1, "exchange": exchange, "symbol": symbol, "snapshot_count": 6, "coverage_ratio": 1.0},
        ],
    )
    # Invalid artifact for a different exchange; should be ignored.
    _write_l2_gold_parquet(
        gold,
        symbol=symbol,
        exchange="binance",
        rows=[
            {"ts_minute": t0, "exchange": "binance", "symbol": symbol, "snapshot_count": 5, "coverage_ratio": 1.5},
            {"ts_minute": t1, "exchange": "binance", "symbol": symbol, "snapshot_count": 6, "coverage_ratio": 1.5},
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.hybrid.full_l2.m1",
        manifest=True,
    )
    written = pl.read_parquet(report.parquet_path)
    assert float(written["l2_coverage_ratio"].max()) == 1.0
    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["observed_row_coverage_ratio"] == 1.0
    assert payload["missing_minutes_in_span"] == 0
    assert payload["expected_minutes_in_span"] == 2


def test_build_gold_hybrid_full_l2_rejects_invalid_l2_coverage_ratio(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.5,
                "high_price": 2.5,
                "low_price": 1.0,
                "close_price": 2.0,
                "volume": 11.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.5,
                "close_price": 10.5,
                "volume": 110.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.5,
                "high_price": 11.5,
                "low_price": 10.0,
                "close_price": 11.0,
                "volume": 111.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1000.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            },
            {
                "timestamp_m1": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1001.0,
                "open_interest_is_observed": False,
                "open_interest_is_ffill": True,
                "minutes_since_open_interest_observation": 1,
                "open_interest_observation_lag_sec": 60,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            },
            {
                "timestamp": t1,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 1,
                "is_funding_observation_minute": False,
                "funding_data_available": True,
            },
        ],
    )
    _write_perps_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol=symbol,
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_l2_gold_parquet(
        gold,
        symbol=symbol,
        exchange=exchange,
        rows=[
            {"ts_minute": t0, "exchange": exchange, "symbol": symbol, "snapshot_count": 5, "coverage_ratio": 1.2},
            {"ts_minute": t1, "exchange": exchange, "symbol": symbol, "snapshot_count": 6, "coverage_ratio": 0.9},
        ],
    )

    with pytest.raises(ValueError, match="L2 validation failed"):
        build_gold_for_symbol(
            silver_root=str(silver),
            gold_root=str(gold),
            exchange=exchange,
            symbol=symbol,
            dataset_id="gold.hybrid.full_l2.m1",
        )


def test_build_gold_hybrid_full_l2_lenient_drops_invalid_rows(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.5,
                "high_price": 2.5,
                "low_price": 1.0,
                "close_price": 2.0,
                "volume": 11.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.5,
                "close_price": 10.5,
                "volume": 110.0,
            },
            {
                "open_time": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.5,
                "high_price": 11.5,
                "low_price": 10.0,
                "close_price": 11.0,
                "volume": 111.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1000.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            },
            {
                "timestamp_m1": t1,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1001.0,
                "open_interest_is_observed": False,
                "open_interest_is_ffill": True,
                "minutes_since_open_interest_observation": 1,
                "open_interest_observation_lag_sec": 60,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            },
            {
                "timestamp": t1,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 1,
                "is_funding_observation_minute": False,
                "funding_data_available": True,
            },
        ],
    )
    _write_perps_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol=symbol,
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t1],
    )
    _write_l2_gold_parquet(
        gold,
        symbol=symbol,
        exchange=exchange,
        rows=[
            {"ts_minute": t0, "exchange": exchange, "symbol": symbol, "snapshot_count": 5, "coverage_ratio": 1.2},
            {"ts_minute": t1, "exchange": exchange, "symbol": symbol, "snapshot_count": 6, "coverage_ratio": 0.9},
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.hybrid.full_l2.m1",
        manifest=True,
        l2_validation_mode="lenient",
    )
    written = pl.read_parquet(report.parquet_path)
    assert written.height == 1
    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["l2_validation_mode"] == "lenient"
    assert payload["l2_invalid_rows_found"] == 1
    assert payload["l2_invalid_rows_dropped"] == 1


def test_build_gold_full_keeps_minute_grid_and_reports_missing_values(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 5, 1, 0, 2, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 2.0,
                "low_price": 0.5,
                "close_price": 1.5,
                "volume": 10.0,
            },
            {
                "open_time": t2,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.6,
                "high_price": 2.6,
                "low_price": 1.2,
                "close_price": 2.1,
                "volume": 12.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.5,
                "close_price": 10.5,
                "volume": 110.0,
            },
            {
                "open_time": t2,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.6,
                "high_price": 11.6,
                "low_price": 10.1,
                "close_price": 11.1,
                "volume": 112.0,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="open_interest_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp_m1": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1000.0,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            },
            {
                "timestamp_m1": t2,
                "exchange": exchange,
                "symbol": symbol,
                "open_interest": 1002.0,
                "open_interest_is_observed": False,
                "open_interest_is_ffill": True,
                "minutes_since_open_interest_observation": 2,
                "open_interest_observation_lag_sec": 120,
            },
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "timestamp": t0,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 0,
                "is_funding_observation_minute": True,
                "funding_data_available": True,
            },
            {
                "timestamp": t2,
                "exchange": exchange,
                "symbol": symbol,
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": 2,
                "is_funding_observation_minute": False,
                "funding_data_available": True,
            },
        ],
    )
    _write_perps_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t2],
    )
    _write_options_trades_1m_feature_month(
        silver,
        exchange=exchange,
        symbol=symbol,
        month="2026-05",
        timestamps=[t0, t2],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t2],
    )
    _write_volatility_observed_month(
        silver,
        dataset_type="volatility_index_data_observed",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        month="2026-05",
        timestamps=[t0, t2],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.market.full.m1",
        manifest=True,
    )
    written = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    assert written.height == 3
    assert written["spot_ohlcv_close_price"].null_count() == 1
    payload = json.loads(_require_manifest_path(report).read_text(encoding="utf-8"))
    assert payload["missing_minutes_in_span"] == 0
    assert payload["missing_value_count_total"] >= 1
    assert payload["missing_value_count_by_column"]["spot_ohlcv_close_price"] == 1
    assert payload["feature_metadata"]["spot_ohlcv_close_price"]["missing_values"] == 1


def test_build_gold_uses_latest_similar_silver_dataset_variant(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)

    stale_spot_ohlcv = (
        silver
        / "dataset_type=spot_ohlcv"
        / f"exchange={exchange}"
        / "symbol=BTC-USDC"
        / "timeframe=1m"
        / "BTC-USDC_2026_05.parquet"
    )
    fresh_spot_ohlcv = (
        silver
        / "dataset_type=spot_ohlcv"
        / f"exchange={exchange}"
        / "symbol=BTC"
        / "timeframe=1m"
        / "BTC_2026_05.parquet"
    )
    stale_spot_ohlcv.parent.mkdir(parents=True, exist_ok=True)
    fresh_spot_ohlcv.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 1.1,
                "low_price": 0.9,
                "close_price": 1.0,
                "volume": 10.0,
            }
        ]
    ).write_parquet(stale_spot_ohlcv)
    pl.DataFrame(
        [
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 2.0,
                "high_price": 2.1,
                "low_price": 1.9,
                "close_price": 2.0,
                "volume": 20.0,
            }
        ]
    ).write_parquet(fresh_spot_ohlcv)
    stale_ts = datetime(2026, 5, 1, 0, 0, tzinfo=UTC).timestamp()
    fresh_ts = datetime(2026, 5, 2, 0, 0, tzinfo=UTC).timestamp()
    os.utime(stale_spot_ohlcv, (stale_ts, stale_ts))
    os.utime(fresh_spot_ohlcv, (fresh_ts, fresh_ts))

    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.0,
                "close_price": 10.0,
                "volume": 100.0,
            }
        ],
    )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.market.core.m1",
        manifest=True,
    )

    written = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    assert written.height == 1
    assert written.get_column("spot_ohlcv_close_price").to_list() == [2.0]


def test_build_gold_prunes_to_latest_three_versions(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 1.1,
                "low_price": 0.9,
                "close_price": 1.0,
                "volume": 10.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.0,
                "close_price": 10.0,
                "volume": 100.0,
            }
        ],
    )

    for dataset_version in ("v1.0.0", "v1.0.1", "v1.0.2", "v1.0.3"):
        build_gold_for_symbol(
            silver_root=str(silver),
            gold_root=str(gold),
            exchange=exchange,
            symbol=symbol,
            dataset_id="gold.market.core.m1",
            dataset_version=dataset_version,
            auto_version=False,
            keep_last_versions=3,
        )

    version_dirs = sorted(
        (gold / "dataset_id=gold.market.core.m1" / "dataset_type=gold_symbol_dataset").glob("feature_set_version=*")
    )
    kept_versions = sorted(path.name.split("=", 1)[1] for path in version_dirs)
    assert kept_versions == ["v1.0.1", "v1.0.2", "v1.0.3"]


def test_build_gold_prunes_to_latest_three_artifacts_with_same_version(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot_ohlcv",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 1.0,
                "high_price": 1.1,
                "low_price": 0.9,
                "close_price": 1.0,
                "volume": 10.0,
            }
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perps_ohlcv",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {
                "open_time": t0,
                "exchange": exchange,
                "symbol": symbol,
                "open_price": 10.0,
                "high_price": 11.0,
                "low_price": 9.0,
                "close_price": 10.0,
                "volume": 100.0,
            }
        ],
    )

    artifact_dir = (
        gold
        / "dataset_id=gold.market.core.m1"
        / "dataset_type=gold_symbol_dataset"
        / "feature_set_version=v1.0.0"
        / "exchange=deribit"
        / "symbol=BTC"
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    for i in range(5):
        stem = artifact_dir / f"BTC_GOLD_seed_{i}"
        for suffix in (".parquet", ".json", ".png"):
            path = stem.with_suffix(suffix)
            path.write_text("x", encoding="utf-8")

    build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        dataset_id="gold.market.core.m1",
        dataset_version="v1.0.0",
        auto_version=False,
        keep_last_versions=3,
    )

    parquet_files = sorted(artifact_dir.glob("*.parquet"))
    json_files = sorted(artifact_dir.glob("*.json"))
    png_files = sorted(artifact_dir.glob("*.png"))
    assert len(parquet_files) == 3
    assert len(json_files) == 3
    assert len(png_files) == 3
