"""Tests for silver command orchestration behavior."""

from __future__ import annotations

import argparse
import logging

import pytest

from api.commands import silver as silver_cmd
from application.dataset_contracts import supported_silver_build_ids
from application.services.silver_service import SilverBuildReport


def silver_args(*, market: list[str], symbols: list[str] | None = None, maxprocesses: int = 1) -> argparse.Namespace:
    """Build the minimal argparse namespace used by silver-build command tests."""

    return argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=market,
        dataset=market,
        symbols=symbols,
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=maxprocesses,
        no_json_output=True,
    )


def test_silver_build_registry_covers_all_parser_datasets() -> None:
    assert set(silver_cmd.SILVER_BUILD_SPECS) == set(silver_cmd.SILVER_BUILD_DATASETS)
    assert silver_cmd.SILVER_BUILD_DATASETS == supported_silver_build_ids()
    assert set(silver_cmd.DEFAULT_SILVER_BUILD_DATASETS).issubset(silver_cmd.SILVER_BUILD_SPECS)
    for dataset, spec in silver_cmd.SILVER_BUILD_SPECS.items():
        assert spec.dataset == dataset


def test_run_silver_build_uses_native_funding_timeframe_for_symbol_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, str]] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange, instrument_type
        captured.append((market, timeframe))
        if market == "funding":
            return ["BTC-PERPETUAL"]
        return []

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(
        silver_cmd,
        "build_funding_observed_for_symbol",
        lambda **kwargs: _report("funding_observed"),
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_funding_1m_feature_for_symbol",
        lambda **kwargs: _report("funding_1m_feature"),
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_open_interest_observed_for_symbol",
        lambda **kwargs: _report("open_interest_observed"),
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_open_interest_1m_feature_for_symbol",
        lambda **kwargs: _report("open_interest_1m_feature"),
    )
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["funding"])
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert captured == [("funding", "8h")]


def test_run_silver_build_uses_tick_timeframe_for_trades_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange, instrument_type
        captured.append((market, timeframe))
        if market == "perps_trades":
            return ["BTC-PERPETUAL"]
        return []

    def fake_build_trades(**kwargs: object) -> SilverBuildReport:
        built.append((str(kwargs["symbol"]), str(kwargs.get("timeframe", kwargs.get("observed_timeframe", "")))))
        return _report("perps_trades_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_observed_for_symbol", fake_build_trades)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_1m_feature_for_symbol", fake_build_trades)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["perps_trades"])
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert captured == [("perps_trades", "tick")]
    assert built == [("BTC-PERPETUAL", "tick"), ("BTC-PERPETUAL", "tick")]


def test_run_silver_build_uses_tick_timeframe_for_options_trades_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, str, str | None]] = []
    built: list[str] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange
        captured.append((market, timeframe, instrument_type))
        if market == "options_trades":
            return ["BTC"]
        return []

    def fake_build_perps_trades_observed(**kwargs: object) -> SilverBuildReport:
        built.append(str(kwargs.get("output_dataset_type", "missing")))
        return _report("options_trades_observed")

    def fake_build_trades_feature(**kwargs: object) -> SilverBuildReport:
        built.append(str(kwargs.get("output_dataset_type", "missing")))
        return _report("options_trades_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_observed_for_symbol", fake_build_perps_trades_observed)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_1m_feature_for_symbol", fake_build_trades_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["options_trades"])
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert captured == [("options_trades", "tick", "option")]
    assert built == ["options_trades_observed", "options_trades_1m_feature"]


def test_run_silver_build_volatility_index_data_builds_observed_and_feature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[tuple[str, str]] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange, timeframe, instrument_type
        if market == "volatility_index_data":
            return ["SOL"]
        return []

    def fake_build_observed(**kwargs: object) -> SilverBuildReport:
        built.append(("observed", str(kwargs["symbol"])))
        return _report("volatility_index_data_observed")

    def fake_build_feature(**kwargs: object) -> SilverBuildReport:
        built.append(("feature", str(kwargs["symbol"])))
        return _report("volatility_index_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "build_volatility_observed_for_symbol", fake_build_observed)
    monkeypatch.setattr(silver_cmd, "build_volatility_index_1m_feature_for_symbol", fake_build_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["volatility_index_data"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert built == [("observed", "SOL"), ("feature", "SOL")]


def test_run_silver_build_realized_volatility_builds_iv_rv_feature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[tuple[str, str]] = []

    def fake_discover_realized_symbols(*, silver_root: str, exchange: str, timeframe: str) -> list[str]:
        del silver_root, exchange, timeframe
        return ["BTC"]

    def fake_build_rv(**kwargs: object) -> SilverBuildReport:
        built.append(("rv", str(kwargs["symbol"])))
        return _report("realized_volatility_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_realized_volatility_symbols", fake_discover_realized_symbols)
    monkeypatch.setattr(silver_cmd, "build_realized_volatility_1m_feature_for_symbol", fake_build_rv)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["realized_volatility"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert built == [("rv", "BTC")]


def test_run_silver_build_runs_historical_prediction_after_base_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[str] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange, timeframe, instrument_type
        return ["BTC-PERPETUAL"] if market == "perps_trades" else []

    def fake_discover_historical_symbols(*, silver_root: str, exchange: str, timeframe: str) -> list[str]:
        del silver_root, exchange, timeframe
        return ["BTC"]

    def fake_build_trades_observed(**kwargs: object) -> SilverBuildReport:
        built.append("perps_trades_observed")
        return _report("perps_trades_observed")

    def fake_build_trades_feature(**kwargs: object) -> SilverBuildReport:
        built.append("perps_trades_1m_feature")
        return _report("perps_trades_1m_feature")

    def fake_build_historical_prediction(**kwargs: object) -> SilverBuildReport:
        built.append("historical_prediction_1m_feature")
        return _report("historical_prediction_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "discover_historical_prediction_symbols", fake_discover_historical_symbols)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_observed_for_symbol", fake_build_trades_observed)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_1m_feature_for_symbol", fake_build_trades_feature)
    monkeypatch.setattr(
        silver_cmd, "build_historical_prediction_1m_feature_for_symbol", fake_build_historical_prediction
    )
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["perps_trades", "historical_prediction"])
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert built == [
        "perps_trades_observed",
        "perps_trades_1m_feature",
        "historical_prediction_1m_feature",
    ]


def test_run_silver_build_defers_iv_rv_until_after_base_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: list[str] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange, timeframe, instrument_type
        return ["BTC"] if market == "spot_ohlcv" else []

    def fake_discover_iv_rv_symbols(*, silver_root: str, exchange: str, timeframe: str) -> list[str]:
        del silver_root, exchange, timeframe
        return ["BTC"]

    def fake_build_spot(**kwargs: object) -> SilverBuildReport:
        built.append(f"spot:{kwargs['symbol']}")
        return _report("spot_ohlcv")

    def fake_build_iv_rv(**kwargs: object) -> SilverBuildReport:
        built.append(f"iv_rv:{kwargs['symbol']}")
        return _report("iv_rv_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "discover_iv_rv_symbols", fake_discover_iv_rv_symbols)
    monkeypatch.setattr(silver_cmd, "build_silver_for_symbol", fake_build_spot)
    monkeypatch.setattr(silver_cmd, "build_iv_rv_1m_feature_for_symbol", fake_build_iv_rv)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["spot_ohlcv", "iv_rv"])
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert built == ["spot:BTC", "iv_rv:BTC"]


def test_run_silver_build_index_price_builds_observed_and_feature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovered: list[tuple[str, str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_index_price_symbols(*, bronze_root: str, exchange: str, dataset_type: str) -> list[str]:
        discovered.append((bronze_root, exchange, dataset_type))
        return ["BTC"]

    def fake_build_observed(**kwargs: object) -> SilverBuildReport:
        built.append(("observed", str(kwargs["symbol"])))
        return _report("index_price_snapshot_1m_observed")

    def fake_build_feature(**kwargs: object) -> SilverBuildReport:
        built.append(("feature", str(kwargs["symbol"])))
        return _report("index_price_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_index_price_symbols", fake_discover_index_price_symbols)
    monkeypatch.setattr(silver_cmd, "build_index_price_observed_for_symbol", fake_build_observed)
    monkeypatch.setattr(silver_cmd, "build_index_price_1m_feature_for_symbol", fake_build_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["index_price_snapshot_1m"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/bronze", "deribit", "index_price_snapshot_1m")]
    assert built == [("observed", "BTC"), ("feature", "BTC")]


def test_run_silver_build_futures_summary_builds_observed_and_feature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovered: list[tuple[str, str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_futures_summary_symbols(*, bronze_root: str, exchange: str, dataset_type: str) -> list[str]:
        discovered.append((bronze_root, exchange, dataset_type))
        return ["ETH"]

    def fake_build_observed(**kwargs: object) -> SilverBuildReport:
        built.append(("observed", str(kwargs["symbol"])))
        return _report("futures_summary_snapshot_1m_observed")

    def fake_build_feature(**kwargs: object) -> SilverBuildReport:
        built.append(("feature", str(kwargs["symbol"])))
        return _report("futures_summary_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_futures_summary_symbols", fake_discover_futures_summary_symbols)
    monkeypatch.setattr(silver_cmd, "build_futures_summary_observed_for_symbol", fake_build_observed)
    monkeypatch.setattr(silver_cmd, "build_futures_summary_1m_feature_for_symbol", fake_build_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["futures_summary_snapshot_1m"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/bronze", "deribit", "futures_summary_snapshot_1m")]
    assert built == [("observed", "ETH"), ("feature", "ETH")]


def test_run_silver_build_routes_options_ticker_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[tuple[str, str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_options_ticker_symbols(*, bronze_root: str, exchange: str, dataset_type: str) -> list[str]:
        discovered.append((bronze_root, exchange, dataset_type))
        return ["BTC"]

    def fake_build_options_ticker(**kwargs: object) -> SilverBuildReport:
        built.append((str(kwargs["symbol"]), str(kwargs["timeframe"])))
        return _report("options_ticker_snapshot_1m_observed")

    monkeypatch.setattr(silver_cmd, "discover_options_ticker_symbols", fake_discover_options_ticker_symbols)
    monkeypatch.setattr(silver_cmd, "build_options_ticker_observed_for_symbol", fake_build_options_ticker)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["options_ticker_snapshot_1m"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/bronze", "deribit", "options_ticker_snapshot_1m")]
    assert built == [("BTC", "1m")]


def test_run_silver_build_routes_options_instrument_ticker_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[tuple[str, str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_options_instrument_ticker_symbols(
        *, bronze_root: str, exchange: str, dataset_type: str
    ) -> list[str]:
        discovered.append((bronze_root, exchange, dataset_type))
        return ["BTC"]

    def fake_build_options_instrument_ticker(**kwargs: object) -> SilverBuildReport:
        built.append((str(kwargs["symbol"]), str(kwargs["timeframe"])))
        return _report("options_instrument_ticker_snapshot_1m_observed")

    monkeypatch.setattr(
        silver_cmd,
        "discover_options_instrument_ticker_symbols",
        fake_discover_options_instrument_ticker_symbols,
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_options_instrument_ticker_observed_for_symbol",
        fake_build_options_instrument_ticker,
    )
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = silver_args(market=["options_instrument_ticker_snapshot_1m"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/bronze", "deribit", "options_instrument_ticker_snapshot_1m")]
    assert built == [("BTC", "1m")]


def test_run_silver_build_routes_options_surface_feature(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[tuple[str, str, str]] = []
    built: list[str] = []

    def fake_discover_options_surface_symbols(*, silver_root: str, exchange: str, timeframe: str) -> list[str]:
        discovered.append((silver_root, exchange, timeframe))
        return ["ETH"]

    def fake_build_options_surface(**kwargs: object) -> SilverBuildReport:
        built.append(str(kwargs["symbol"]))
        return _report("options_surface_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_options_surface_symbols", fake_discover_options_surface_symbols)
    monkeypatch.setattr(silver_cmd, "build_options_surface_1m_feature_for_symbol", fake_build_options_surface)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = silver_args(market=["options_surface_1m_feature"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/silver", "deribit", "1m")]
    assert built == ["ETH"]


def test_run_silver_build_routes_perps_l2_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[tuple[str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_l2_symbols(**kwargs: object) -> list[str]:
        discovered.append((str(kwargs["dataset_type"]), str(kwargs["instrument_type"])))
        return ["BTC-PERPETUAL"]

    def fake_build_observed(**kwargs: object) -> SilverBuildReport:
        built.append(("observed", str(kwargs["symbol"])))
        return _report("perps_l2_snapshot_1m_observed")

    def fake_build_feature(**kwargs: object) -> SilverBuildReport:
        built.append(("feature", str(kwargs["symbol"])))
        return _report("perps_l2_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_l2_symbols", fake_discover_l2_symbols)
    monkeypatch.setattr(silver_cmd, "build_perps_l2_observed_for_symbol", fake_build_observed)
    monkeypatch.setattr(silver_cmd, "build_perps_l2_1m_feature_for_symbol", fake_build_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = silver_args(market=["perps_l2_snapshot_1m"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("perps_l2_snapshot_1m", "perp")]
    assert built == [("observed", "BTC-PERPETUAL"), ("feature", "BTC-PERPETUAL")]


def test_run_silver_build_routes_options_l2_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[tuple[str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_l2_symbols(**kwargs: object) -> list[str]:
        discovered.append((str(kwargs["dataset_type"]), str(kwargs["instrument_type"])))
        return ["BTC"]

    def fake_build_observed(**kwargs: object) -> SilverBuildReport:
        built.append(("observed", str(kwargs["symbol"])))
        return _report("options_l2_snapshot_1m_observed")

    def fake_build_feature(**kwargs: object) -> SilverBuildReport:
        built.append(("feature", str(kwargs["symbol"])))
        return _report("options_l2_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_l2_symbols", fake_discover_l2_symbols)
    monkeypatch.setattr(silver_cmd, "build_options_l2_observed_for_symbol", fake_build_observed)
    monkeypatch.setattr(silver_cmd, "build_options_l2_1m_feature_for_symbol", fake_build_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = silver_args(market=["options_l2_snapshot_1m"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("options_l2_snapshot_1m", "option")]
    assert built == [("observed", "BTC"), ("feature", "BTC")]


def test_run_silver_build_routes_recent_trade_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[tuple[str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_recent_trade_symbols(**kwargs: object) -> list[str]:
        discovered.append((str(kwargs["bronze_root"]), str(kwargs["exchange"])))
        return ["BTC"]

    def fake_build_recent(**kwargs: object) -> SilverBuildReport:
        built.append((str(kwargs["symbol"]), str(kwargs["timeframe"])))
        return _report("recent_trade_snapshot_1m_observed")

    monkeypatch.setattr(silver_cmd, "discover_recent_trade_symbols", fake_discover_recent_trade_symbols)
    monkeypatch.setattr(silver_cmd, "build_recent_trade_snapshot_observed_for_symbol", fake_build_recent)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = silver_args(market=["recent_trade_snapshot_1m"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/bronze", "deribit")]
    assert built == [("BTC", "tick")]


def test_run_silver_build_routes_both_metadata_families(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[str] = []
    built: list[str] = []

    def fake_discover_metadata(**kwargs: object) -> list[str]:
        discovered.append(str(kwargs["dataset_type"]))
        return ["BTC"]

    def fake_build_options(**kwargs: object) -> SilverBuildReport:
        built.append("options")
        return _report("instrument_metadata_snapshot_daily_observed")

    def fake_build_futures(**kwargs: object) -> SilverBuildReport:
        built.append("futures")
        return _report("futures_instrument_metadata_snapshot_daily_observed")

    monkeypatch.setattr(silver_cmd, "discover_instrument_metadata_symbols", fake_discover_metadata)
    monkeypatch.setattr(silver_cmd, "build_instrument_metadata_observed_for_symbol", fake_build_options)
    monkeypatch.setattr(silver_cmd, "build_futures_instrument_metadata_observed_for_symbol", fake_build_futures)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = silver_args(
        market=[
            "instrument_metadata_snapshot_daily",
            "futures_instrument_metadata_snapshot_daily",
        ]
    )

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [
        "instrument_metadata_snapshot_daily",
        "futures_instrument_metadata_snapshot_daily",
    ]
    assert built == ["options", "futures"]


def test_run_silver_build_routes_historical_volatility(monkeypatch: pytest.MonkeyPatch) -> None:
    discovered: list[tuple[str, str, str]] = []
    built: list[str] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange
        discovered.append((market, str(instrument_type), timeframe))
        return ["BTC"]

    def fake_build(**kwargs: object) -> SilverBuildReport:
        built.append(str(kwargs["symbol"]))
        return _report("historical_volatility_observed")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "build_historical_volatility_observed_for_symbol", fake_build)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = silver_args(market=["historical_volatility"])

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("historical_volatility", "perp", "1m")]
    assert built == ["BTC"]


def test_run_silver_build_rejects_invalid_maxprocesses() -> None:
    args = silver_args(market=["spot_ohlcv"], symbols=["BTC"], maxprocesses=0)

    with pytest.raises(ValueError, match="maxprocesses"):
        silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))


def test_collect_job_results_logs_wait_points_and_completion(caplog: pytest.LogCaptureFixture) -> None:
    reports: list[dict[str, object]] = []

    class FakeFuture:
        def __init__(self, payload: list[dict[str, object]]) -> None:
            self._payload = payload

        def result(self) -> list[dict[str, object]]:
            return self._payload

    futures = [
        ("realized_volatility", "BTC", FakeFuture([{"dataset": "rv"}])),
        ("iv_rv", "ETH", FakeFuture([{"dataset": "iv_rv"}])),
    ]

    with caplog.at_level(logging.INFO, logger="test"):
        silver_cmd._collect_job_results(
            futures=futures,
            logger=logging.getLogger("test"),
            reports=reports,
            stage_name="base",
        )

    assert reports == [{"dataset": "rv"}, {"dataset": "iv_rv"}]
    assert "Silver waiting for base job market=realized_volatility symbol=BTC" in caplog.text
    assert "Silver completed base job market=realized_volatility symbol=BTC" in caplog.text
    assert "Silver waiting for base job market=iv_rv symbol=ETH" in caplog.text
    assert "Silver completed base job market=iv_rv symbol=ETH" in caplog.text


def test_collect_job_results_logs_context_on_failure(caplog: pytest.LogCaptureFixture) -> None:
    reports: list[dict[str, object]] = []

    class FakeFuture:
        def result(self) -> list[dict[str, object]]:
            raise RuntimeError("boom")

    futures = [("realized_volatility", "BTC", FakeFuture())]

    with caplog.at_level(logging.INFO, logger="test"):
        with pytest.raises(RuntimeError, match="boom"):
            silver_cmd._collect_job_results(
                futures=futures,
                logger=logging.getLogger("test"),
                reports=reports,
                stage_name="base",
            )

    assert "Silver waiting for base job market=realized_volatility symbol=BTC" in caplog.text
    assert "Silver base job failed market=realized_volatility symbol=BTC" in caplog.text


def _report(dataset: str) -> SilverBuildReport:
    return SilverBuildReport(
        dataset=dataset,
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        period_start=None,
        period_end=None,
        months_processed=[],
        rows_in=0,
        rows_out=0,
        duplicates_removed=0,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=None,
        max_timestamp=None,
        symbols=["BTC-PERPETUAL"],
        columns=["timestamp", "value"],
    )
