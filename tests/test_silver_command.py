"""Tests for silver command orchestration behavior."""

from __future__ import annotations

import argparse
import logging

import pytest

from api.commands import silver as silver_cmd


def test_run_silver_build_uses_native_funding_timeframe_for_symbol_discovery(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
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

    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["funding"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        no_json_output=True,
    )
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert captured == [("funding", "8h")]


def test_run_silver_build_uses_tick_timeframe_for_trades_discovery(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
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

    def fake_build_trades(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append((str(kwargs["symbol"]), str(kwargs.get("timeframe", kwargs.get("observed_timeframe", "")))))
        return _report("perps_trades_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_observed_for_symbol", fake_build_trades)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_1m_feature_for_symbol", fake_build_trades)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["perps_trades"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        no_json_output=True,
    )
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert captured == [("perps_trades", "tick")]
    assert built == [("BTC-PERPETUAL", "tick"), ("BTC-PERPETUAL", "tick")]


def test_run_silver_build_uses_tick_timeframe_for_options_trades_discovery(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
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

    def fake_build_perps_trades_observed(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append(str(kwargs.get("output_dataset_type", "missing")))
        return _report("options_trades_observed")

    def fake_build_trades_feature(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append(str(kwargs.get("output_dataset_type", "missing")))
        return _report("options_trades_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_observed_for_symbol", fake_build_perps_trades_observed)
    monkeypatch.setattr(silver_cmd, "build_perps_trades_1m_feature_for_symbol", fake_build_trades_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))

    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["options_trades"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        no_json_output=True,
    )
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert captured == [("options_trades", "tick", "option")]
    assert built == ["options_trades_observed", "options_trades_1m_feature"]


def test_run_silver_build_routes_options_instrument_ticker_dataset(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    discovered: list[tuple[str, str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_options_instrument_ticker_symbols(
        *, bronze_root: str, exchange: str, dataset_type: str
    ) -> list[str]:
        discovered.append((bronze_root, exchange, dataset_type))
        return ["BTC"]

    def fake_build_options_instrument_ticker(**kwargs: object) -> silver_cmd.SilverBuildReport:
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

    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["options_instrument_ticker_snapshot_1m"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=1,
        no_json_output=True,
    )

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/bronze", "deribit", "options_instrument_ticker_snapshot_1m")]
    assert built == [("BTC", "1m")]


def test_run_silver_build_routes_options_surface_feature(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    discovered: list[tuple[str, str, str]] = []
    built: list[str] = []

    def fake_discover_options_surface_symbols(*, silver_root: str, exchange: str, timeframe: str) -> list[str]:
        discovered.append((silver_root, exchange, timeframe))
        return ["ETH"]

    def fake_build_options_surface(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append(str(kwargs["symbol"]))
        return _report("options_surface_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_options_surface_symbols", fake_discover_options_surface_symbols)
    monkeypatch.setattr(silver_cmd, "build_options_surface_1m_feature_for_symbol", fake_build_options_surface)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["options_surface_1m_feature"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=1,
        no_json_output=True,
    )

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/silver", "deribit", "1m")]
    assert built == ["ETH"]


def test_run_silver_build_routes_perps_l2_dataset(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    discovered: list[tuple[str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_l2_symbols(**kwargs: object) -> list[str]:
        discovered.append((str(kwargs["dataset_type"]), str(kwargs["instrument_type"])))
        return ["BTC-PERPETUAL"]

    def fake_build_observed(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append(("observed", str(kwargs["symbol"])))
        return _report("perps_l2_snapshot_1m_observed")

    def fake_build_feature(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append(("feature", str(kwargs["symbol"])))
        return _report("perps_l2_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_l2_symbols", fake_discover_l2_symbols)
    monkeypatch.setattr(silver_cmd, "build_perps_l2_observed_for_symbol", fake_build_observed)
    monkeypatch.setattr(silver_cmd, "build_perps_l2_1m_feature_for_symbol", fake_build_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["perps_l2_snapshot_1m"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=1,
        no_json_output=True,
    )

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("perps_l2_snapshot_1m", "perp")]
    assert built == [("observed", "BTC-PERPETUAL"), ("feature", "BTC-PERPETUAL")]


def test_run_silver_build_routes_options_l2_dataset(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    discovered: list[tuple[str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_l2_symbols(**kwargs: object) -> list[str]:
        discovered.append((str(kwargs["dataset_type"]), str(kwargs["instrument_type"])))
        return ["BTC"]

    def fake_build_observed(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append(("observed", str(kwargs["symbol"])))
        return _report("options_l2_snapshot_1m_observed")

    def fake_build_feature(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append(("feature", str(kwargs["symbol"])))
        return _report("options_l2_1m_feature")

    monkeypatch.setattr(silver_cmd, "discover_l2_symbols", fake_discover_l2_symbols)
    monkeypatch.setattr(silver_cmd, "build_options_l2_observed_for_symbol", fake_build_observed)
    monkeypatch.setattr(silver_cmd, "build_options_l2_1m_feature_for_symbol", fake_build_feature)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["options_l2_snapshot_1m"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=1,
        no_json_output=True,
    )

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("options_l2_snapshot_1m", "option")]
    assert built == [("observed", "BTC"), ("feature", "BTC")]


def test_run_silver_build_routes_recent_trade_snapshot(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    discovered: list[tuple[str, str]] = []
    built: list[tuple[str, str]] = []

    def fake_discover_recent_trade_symbols(**kwargs: object) -> list[str]:
        discovered.append((str(kwargs["bronze_root"]), str(kwargs["exchange"])))
        return ["BTC"]

    def fake_build_recent(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append((str(kwargs["symbol"]), str(kwargs["timeframe"])))
        return _report("recent_trade_snapshot_1m_observed")

    monkeypatch.setattr(silver_cmd, "discover_recent_trade_symbols", fake_discover_recent_trade_symbols)
    monkeypatch.setattr(silver_cmd, "build_recent_trade_snapshot_observed_for_symbol", fake_build_recent)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["recent_trade_snapshot_1m"],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=1,
        no_json_output=True,
    )

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [("lake/bronze", "deribit")]
    assert built == [("BTC", "tick")]


def test_run_silver_build_routes_both_metadata_families(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    discovered: list[str] = []
    built: list[str] = []

    def fake_discover_metadata(**kwargs: object) -> list[str]:
        discovered.append(str(kwargs["dataset_type"]))
        return ["BTC"]

    def fake_build_options(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append("options")
        return _report("instrument_metadata_snapshot_daily_observed")

    def fake_build_futures(**kwargs: object) -> silver_cmd.SilverBuildReport:
        built.append("futures")
        return _report("futures_instrument_metadata_snapshot_daily_observed")

    monkeypatch.setattr(silver_cmd, "discover_instrument_metadata_symbols", fake_discover_metadata)
    monkeypatch.setattr(silver_cmd, "build_instrument_metadata_observed_for_symbol", fake_build_options)
    monkeypatch.setattr(silver_cmd, "build_futures_instrument_metadata_observed_for_symbol", fake_build_futures)
    monkeypatch.setattr(silver_cmd, "write_monthly_sidecars", lambda **kwargs: ([], []))
    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=[
            "instrument_metadata_snapshot_daily",
            "futures_instrument_metadata_snapshot_daily",
        ],
        symbols=None,
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=1,
        no_json_output=True,
    )

    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert discovered == [
        "instrument_metadata_snapshot_daily",
        "futures_instrument_metadata_snapshot_daily",
    ]
    assert built == ["options", "futures"]


def test_run_silver_build_rejects_invalid_maxprocesses() -> None:
    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["spot_ohlcv"],
        symbols=["BTC"],
        timeframe="1m",
        manifest=False,
        plot=False,
        maxprocesses=0,
        no_json_output=True,
    )

    with pytest.raises(ValueError, match="maxprocesses"):
        silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))


def _report(dataset: str) -> silver_cmd.SilverBuildReport:
    return silver_cmd.SilverBuildReport(
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
