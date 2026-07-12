"""Silver build command for spot_ohlcv/perp OHLCV transformation."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Literal, cast

from application.dataset_contracts import supported_silver_build_ids
from application.services.silver_service import (
    SilverBuildReport,
    build_funding_1m_feature_for_symbol,
    build_funding_observed_for_symbol,
    build_futures_instrument_metadata_observed_for_symbol,
    build_futures_summary_1m_feature_for_symbol,
    build_futures_summary_observed_for_symbol,
    build_historical_volatility_observed_for_symbol,
    build_index_price_1m_feature_for_symbol,
    build_index_price_observed_for_symbol,
    build_instrument_metadata_observed_for_symbol,
    build_iv_rv_1m_feature_for_symbol,
    build_open_interest_1m_feature_for_symbol,
    build_open_interest_observed_for_symbol,
    build_options_instrument_ticker_observed_for_symbol,
    build_options_l2_1m_feature_for_symbol,
    build_options_l2_observed_for_symbol,
    build_options_surface_1m_feature_for_symbol,
    build_options_ticker_observed_for_symbol,
    build_perps_l2_1m_feature_for_symbol,
    build_perps_l2_observed_for_symbol,
    build_perps_trades_1m_feature_for_symbol,
    build_perps_trades_observed_for_symbol,
    build_realized_volatility_1m_feature_for_symbol,
    build_recent_trade_snapshot_observed_for_symbol,
    build_silver_for_symbol,
    build_volatility_index_1m_feature_for_symbol,
    build_volatility_observed_for_symbol,
    build_volatility_snapshot_observed_for_symbol,
    discover_futures_summary_symbols,
    discover_index_price_symbols,
    discover_instrument_metadata_symbols,
    discover_iv_rv_symbols,
    discover_l2_symbols,
    discover_options_instrument_ticker_symbols,
    discover_options_surface_symbols,
    discover_options_ticker_symbols,
    discover_realized_volatility_symbols,
    discover_recent_trade_symbols,
    discover_symbols,
    discover_volatility_snapshot_symbols,
)
from application.services.silver_sidecars import write_monthly_sidecars
from ingestion.funding import DERIBIT_FUNDING_NATIVE_INTERVAL

SilverDiscoveryKind = Literal[
    "bronze",
    "volatility_snapshot",
    "realized_volatility",
    "iv_rv",
    "index_price",
    "futures_summary",
    "options_ticker",
    "options_instrument_ticker",
    "options_surface",
    "perps_l2",
    "options_l2",
    "recent_trade",
    "instrument_metadata",
]


@dataclass(frozen=True)
class SilverBuildSpec:
    """Declarative routing contract for one ``silver-build`` dataset choice."""

    dataset: str
    discovery: SilverDiscoveryKind
    handler_key: str | None = None
    bronze_dataset: str | None = None
    bronze_instrument: str | None = None
    bronze_timeframe: str | None = None


SILVER_BUILD_SPECS: dict[str, SilverBuildSpec] = {
    "spot_ohlcv": SilverBuildSpec(dataset="spot_ohlcv", discovery="bronze"),
    "perps_ohlcv": SilverBuildSpec(
        dataset="perps_ohlcv", discovery="bronze", bronze_dataset="perps_ohlcv", bronze_instrument="perp"
    ),
    "open_interest": SilverBuildSpec(
        dataset="open_interest", discovery="bronze", bronze_dataset="open_interest", bronze_instrument="perp"
    ),
    "funding": SilverBuildSpec(
        dataset="funding",
        discovery="bronze",
        bronze_dataset="funding",
        bronze_instrument="perp",
        bronze_timeframe=DERIBIT_FUNDING_NATIVE_INTERVAL,
    ),
    "perps_trades": SilverBuildSpec(
        dataset="perps_trades",
        discovery="bronze",
        bronze_dataset="perps_trades",
        bronze_instrument="perp",
        bronze_timeframe="tick",
    ),
    "options_trades": SilverBuildSpec(
        dataset="options_trades",
        discovery="bronze",
        bronze_dataset="options_trades",
        bronze_instrument="option",
        bronze_timeframe="tick",
    ),
    "volatility_index_data": SilverBuildSpec(
        dataset="volatility_index_data",
        discovery="bronze",
        bronze_dataset="volatility_index_data",
        bronze_instrument="perp",
    ),
    "volatility_index_snapshot_1m": SilverBuildSpec(
        dataset="volatility_index_snapshot_1m", discovery="volatility_snapshot"
    ),
    "realized_volatility": SilverBuildSpec(dataset="realized_volatility", discovery="realized_volatility"),
    "iv_rv": SilverBuildSpec(dataset="iv_rv", discovery="iv_rv"),
    "index_price_snapshot_1m": SilverBuildSpec(dataset="index_price_snapshot_1m", discovery="index_price"),
    "futures_summary_snapshot_1m": SilverBuildSpec(dataset="futures_summary_snapshot_1m", discovery="futures_summary"),
    "options_ticker_snapshot_1m": SilverBuildSpec(dataset="options_ticker_snapshot_1m", discovery="options_ticker"),
    "options_instrument_ticker_snapshot_1m": SilverBuildSpec(
        dataset="options_instrument_ticker_snapshot_1m", discovery="options_instrument_ticker"
    ),
    "options_surface_1m_feature": SilverBuildSpec(dataset="options_surface_1m_feature", discovery="options_surface"),
    "perps_l2_snapshot_1m": SilverBuildSpec(dataset="perps_l2_snapshot_1m", discovery="perps_l2"),
    "options_l2_snapshot_1m": SilverBuildSpec(dataset="options_l2_snapshot_1m", discovery="options_l2"),
    "recent_trade_snapshot_1m": SilverBuildSpec(dataset="recent_trade_snapshot_1m", discovery="recent_trade"),
    "instrument_metadata_snapshot_daily": SilverBuildSpec(
        dataset="instrument_metadata_snapshot_daily", discovery="instrument_metadata"
    ),
    "futures_instrument_metadata_snapshot_daily": SilverBuildSpec(
        dataset="futures_instrument_metadata_snapshot_daily", discovery="instrument_metadata"
    ),
    "historical_volatility": SilverBuildSpec(
        dataset="historical_volatility",
        discovery="bronze",
        bronze_dataset="historical_volatility",
        bronze_instrument="perp",
    ),
}

SILVER_BUILD_DATASETS: tuple[str, ...] = supported_silver_build_ids()
DEFAULT_SILVER_BUILD_DATASETS: tuple[str, ...] = (
    "spot_ohlcv",
    "perps_ohlcv",
    "open_interest",
    "funding",
    "perps_trades",
    "options_trades",
    "volatility_index_data",
)


def add_silver_build_parser(subparsers: Any) -> None:
    """Register ``silver-build`` parser."""

    parser = subparsers.add_parser("silver-build", help="Build silver monthly parquet outputs from bronze data")
    parser.add_argument("--bronze-root", default="lake/bronze", help="Bronze lake root")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument(
        "--dataset",
        nargs="+",
        choices=list(SILVER_BUILD_DATASETS),
        default=list(DEFAULT_SILVER_BUILD_DATASETS),
    )
    parser.add_argument("--symbols", nargs="+", help="Optional symbol list; auto-discovered when omitted")
    parser.add_argument("--timeframe", default="1m", help="Timeframe to process (default: 1m)")
    parser.add_argument("--manifest", action="store_true", help="Generate monthly silver manifest sidecars")
    parser.add_argument("--plot", action="store_true", help="Generate monthly silver plot PNG sidecars")
    parser.add_argument("--maxprocesses", type=int, default=4, help="Maximum parallel silver build workers")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_silver_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run silver build for configured markets/symbols."""

    bronze_root = cast(str, args.bronze_root)
    silver_root = cast(str, args.silver_root)
    exchange = cast(str, args.exchange)
    timeframe = cast(str, args.timeframe)
    maxprocesses = int(getattr(args, "maxprocesses", 4))
    if maxprocesses < 1:
        raise ValueError(f"Invalid --maxprocesses '{maxprocesses}'. Value must be an integer >= 1")
    reports: list[dict[str, object]] = []

    def _report_payload(report_market: str, symbol_value: str, report: SilverBuildReport) -> dict[str, object]:
        manifest_path: str | None = None
        manifest_paths: list[str] = []
        plot_path: str | None = None
        plot_paths: list[str] = []
        want_manifest = bool(getattr(args, "manifest", False))
        want_plot = bool(getattr(args, "plot", False))
        if want_manifest or want_plot:
            manifest_paths, plot_paths = write_monthly_sidecars(
                silver_root=silver_root,
                market=report_market,
                exchange=exchange,
                symbol=symbol_value,
                report=report,
                write_manifest=want_manifest,
                plot=want_plot,
            )
            manifest_path = manifest_paths[0] if manifest_paths else None
            plot_path = plot_paths[0] if plot_paths else None
        report_dict = report.to_dict()
        report_dict["manifest_path"] = manifest_path
        report_dict["manifest_paths"] = manifest_paths
        report_dict["plot_path"] = plot_path
        report_dict["plot_paths"] = plot_paths
        return report_dict

    def _run_funding(symbol: str) -> list[dict[str, object]]:
        observed = build_funding_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=DERIBIT_FUNDING_NATIVE_INTERVAL,
        )
        observed_payload = _report_payload("funding_observed", symbol, observed)

        feature = build_funding_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe=DERIBIT_FUNDING_NATIVE_INTERVAL,
        )
        feature_payload = _report_payload("funding_1m_feature", symbol, feature)
        logger.info(
            "Silver funding reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_open_interest(symbol: str) -> list[dict[str, object]]:
        observed = build_open_interest_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        observed_payload = _report_payload("open_interest_observed", symbol, observed)

        feature = build_open_interest_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe=timeframe,
        )
        feature_payload = _report_payload("open_interest_1m_feature", symbol, feature)
        logger.info(
            "Silver Open Interest reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_trades(symbol: str) -> list[dict[str, object]]:
        observed = build_perps_trades_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            instrument_type="perp",
            timeframe="tick",
        )
        observed_payload = _report_payload("perps_trades_observed", symbol, observed)
        feature = build_perps_trades_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe="tick",
        )
        feature_payload = _report_payload("perps_trades_1m_feature", symbol, feature)
        logger.info(
            "Silver trades reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_options_trades(symbol: str) -> list[dict[str, object]]:
        observed = build_perps_trades_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            instrument_type="option",
            timeframe="tick",
            bronze_dataset_type="options_trades",
            output_dataset_type="options_trades_observed",
        )
        observed_payload = _report_payload("options_trades_observed", symbol, observed)
        feature = build_perps_trades_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe="tick",
            observed_dataset_type="options_trades_observed",
            output_dataset_type="options_trades_1m_feature",
        )
        feature_payload = _report_payload("options_trades_1m_feature", symbol, feature)
        logger.info(
            "Silver options_trades reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_volatility_index_data(symbol: str) -> list[dict[str, object]]:
        observed = build_volatility_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            bronze_dataset_type="volatility_index_data",
            output_dataset_type="volatility_index_data_observed",
        )
        observed_payload = _report_payload("volatility_index_data_observed", symbol, observed)
        feature = build_volatility_index_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature_payload = _report_payload("volatility_index_1m_feature", symbol, feature)
        logger.info(
            "Silver volatility_index_data reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_volatility_index_snapshot(symbol: str) -> list[dict[str, object]]:
        observed = build_volatility_snapshot_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        observed_payload = _report_payload("volatility_index_snapshot_1m_observed", symbol, observed)

        feature = build_volatility_index_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature_payload = _report_payload("volatility_index_1m_feature", symbol, feature)
        logger.info(
            "Silver volatility_index_snapshot_1m reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_realized_volatility(symbol: str) -> list[dict[str, object]]:
        feature = build_realized_volatility_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature_payload = _report_payload("realized_volatility_1m_feature", symbol, feature)
        iv_rv = build_iv_rv_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        iv_rv_payload = _report_payload("iv_rv_1m_feature", symbol, iv_rv)
        logger.info(
            "Silver realized_volatility reports written symbol=%s rv_rows=%s iv_rv_rows=%s",
            symbol,
            feature.rows_out,
            iv_rv.rows_out,
        )
        return [feature_payload, iv_rv_payload]

    def _run_iv_rv(symbol: str) -> list[dict[str, object]]:
        feature = build_iv_rv_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature_payload = _report_payload("iv_rv_1m_feature", symbol, feature)
        logger.info("Silver iv_rv report written symbol=%s feature_rows=%s", symbol, feature.rows_out)
        return [feature_payload]

    def _run_index_price(symbol: str) -> list[dict[str, object]]:
        observed = build_index_price_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        observed_payload = _report_payload("index_price_snapshot_1m_observed", symbol, observed)
        feature = build_index_price_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature_payload = _report_payload("index_price_1m_feature", symbol, feature)
        logger.info(
            "Silver index_price_snapshot_1m reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_futures_summary(symbol: str) -> list[dict[str, object]]:
        observed = build_futures_summary_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        observed_payload = _report_payload("futures_summary_snapshot_1m_observed", symbol, observed)
        feature = build_futures_summary_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature_payload = _report_payload("futures_summary_1m_feature", symbol, feature)
        logger.info(
            "Silver futures_summary_snapshot_1m reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_options_ticker(symbol: str) -> list[dict[str, object]]:
        observed = build_options_ticker_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        observed_payload = _report_payload("options_ticker_snapshot_1m_observed", symbol, observed)
        logger.info(
            "Silver options_ticker_snapshot_1m report written symbol=%s observed_rows=%s",
            symbol,
            observed.rows_out,
        )
        return [observed_payload]

    def _run_options_instrument_ticker(symbol: str) -> list[dict[str, object]]:
        observed = build_options_instrument_ticker_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        observed_payload = _report_payload("options_instrument_ticker_snapshot_1m_observed", symbol, observed)
        logger.info(
            "Silver options_instrument_ticker_snapshot_1m report written symbol=%s observed_rows=%s",
            symbol,
            observed.rows_out,
        )
        return [observed_payload]

    def _run_options_surface(symbol: str) -> list[dict[str, object]]:
        feature = build_options_surface_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature_payload = _report_payload("options_surface_1m_feature", symbol, feature)
        logger.info(
            "Silver options_surface_1m_feature report written symbol=%s feature_rows=%s",
            symbol,
            feature.rows_out,
        )
        return [feature_payload]

    def _run_perps_l2(symbol: str) -> list[dict[str, object]]:
        observed = build_perps_l2_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature = build_perps_l2_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        logger.info(
            "Silver perps_l2_snapshot_1m reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [
            _report_payload("perps_l2_snapshot_1m_observed", symbol, observed),
            _report_payload("perps_l2_1m_feature", symbol, feature),
        ]

    def _run_options_l2(symbol: str) -> list[dict[str, object]]:
        observed = build_options_l2_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        feature = build_options_l2_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        logger.info(
            "Silver options_l2_snapshot_1m reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [
            _report_payload("options_l2_snapshot_1m_observed", symbol, observed),
            _report_payload("options_l2_1m_feature", symbol, feature),
        ]

    def _run_recent_trade_snapshot(symbol: str) -> list[dict[str, object]]:
        observed = build_recent_trade_snapshot_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe="tick",
        )
        logger.info(
            "Silver recent_trade_snapshot_1m report written symbol=%s observed_rows=%s",
            symbol,
            observed.rows_out,
        )
        return [_report_payload("recent_trade_snapshot_1m_observed", symbol, observed)]

    def _run_instrument_metadata(symbol: str) -> list[dict[str, object]]:
        observed = build_instrument_metadata_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
        )
        logger.info(
            "Silver instrument_metadata_snapshot_daily report written symbol=%s observed_rows=%s",
            symbol,
            observed.rows_out,
        )
        return [_report_payload("instrument_metadata_snapshot_daily_observed", symbol, observed)]

    def _run_futures_instrument_metadata(symbol: str) -> list[dict[str, object]]:
        observed = build_futures_instrument_metadata_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
        )
        logger.info(
            "Silver futures_instrument_metadata_snapshot_daily report written symbol=%s observed_rows=%s",
            symbol,
            observed.rows_out,
        )
        return [_report_payload("futures_instrument_metadata_snapshot_daily_observed", symbol, observed)]

    def _run_historical_volatility(symbol: str) -> list[dict[str, object]]:
        observed = build_historical_volatility_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        logger.info(
            "Silver historical_volatility report written symbol=%s observed_rows=%s",
            symbol,
            observed.rows_out,
        )
        return [_report_payload("historical_volatility_observed", symbol, observed)]

    def _run_ohlcv(market: str, symbol: str) -> list[dict[str, object]]:
        report = build_silver_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            market=market,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        payload = _report_payload(market, symbol, report)
        logger.info(
            "Silver dataset built market=%s symbol=%s rows_in=%s rows_out=%s",
            market,
            symbol,
            report.rows_in,
            report.rows_out,
        )
        return [payload]

    market_handlers: dict[str, Callable[[str], list[dict[str, object]]]] = {
        "funding": _run_funding,
        "open_interest": _run_open_interest,
        "perps_trades": _run_trades,
        "options_trades": _run_options_trades,
        "volatility_index_data": _run_volatility_index_data,
        "volatility_index_snapshot_1m": _run_volatility_index_snapshot,
        "realized_volatility": _run_realized_volatility,
        "iv_rv": _run_iv_rv,
        "index_price_snapshot_1m": _run_index_price,
        "futures_summary_snapshot_1m": _run_futures_summary,
        "options_ticker_snapshot_1m": _run_options_ticker,
        "options_instrument_ticker_snapshot_1m": _run_options_instrument_ticker,
        "options_surface_1m_feature": _run_options_surface,
        "perps_l2_snapshot_1m": _run_perps_l2,
        "options_l2_snapshot_1m": _run_options_l2,
        "recent_trade_snapshot_1m": _run_recent_trade_snapshot,
        "instrument_metadata_snapshot_daily": _run_instrument_metadata,
        "futures_instrument_metadata_snapshot_daily": _run_futures_instrument_metadata,
        "historical_volatility": _run_historical_volatility,
    }

    def _discover_effective_symbols(market: str, spec: SilverBuildSpec) -> list[str]:
        symbols = cast(list[str] | None, args.symbols)
        if symbols is not None:
            return symbols
        if spec.discovery == "volatility_snapshot":
            return discover_volatility_snapshot_symbols(
                bronze_root=bronze_root,
                dataset_type="volatility_index_snapshot_1m",
                exchange=exchange,
            )
        if spec.discovery == "realized_volatility":
            return discover_realized_volatility_symbols(
                silver_root=silver_root,
                exchange=exchange,
                timeframe=timeframe,
            )
        if spec.discovery == "iv_rv":
            return discover_iv_rv_symbols(
                silver_root=silver_root,
                exchange=exchange,
                timeframe=timeframe,
            )
        if spec.discovery == "index_price":
            return discover_index_price_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
                dataset_type="index_price_snapshot_1m",
            )
        if spec.discovery == "futures_summary":
            return discover_futures_summary_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
                dataset_type="futures_summary_snapshot_1m",
            )
        if spec.discovery == "options_ticker":
            return discover_options_ticker_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
                dataset_type="options_ticker_snapshot_1m",
            )
        if spec.discovery == "options_instrument_ticker":
            return discover_options_instrument_ticker_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
                dataset_type="options_instrument_ticker_snapshot_1m",
            )
        if spec.discovery == "options_surface":
            return discover_options_surface_symbols(
                silver_root=silver_root,
                exchange=exchange,
                timeframe=timeframe,
            )
        if spec.discovery == "perps_l2":
            return discover_l2_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
                dataset_type="perps_l2_snapshot_1m",
                instrument_type="perp",
            )
        if spec.discovery == "options_l2":
            return discover_l2_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
                dataset_type="options_l2_snapshot_1m",
                instrument_type="option",
            )
        if spec.discovery == "recent_trade":
            return discover_recent_trade_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
            )
        if spec.discovery == "instrument_metadata":
            return discover_instrument_metadata_symbols(
                bronze_root=bronze_root,
                exchange=exchange,
                dataset_type=market,
            )

        bronze_dataset = spec.bronze_dataset or market
        bronze_instrument = spec.bronze_instrument or market
        configured_timeframe = spec.bronze_timeframe or timeframe
        discovery_timeframe = timeframe if configured_timeframe == "1m" else configured_timeframe
        return discover_symbols(
            bronze_root=bronze_root,
            market=bronze_dataset,
            exchange=exchange,
            timeframe=discovery_timeframe,
            instrument_type=bronze_instrument,
        )

    selected = getattr(args, "dataset", getattr(args, "market", None))
    if selected is None:
        raise ValueError("Missing dataset selection. Provide --dataset.")
    jobs: list[tuple[str, str, Callable[[], list[dict[str, object]]]]] = []

    def _make_handler_job(
        handler: Callable[[str], list[dict[str, object]]],
        symbol: str,
    ) -> Callable[[], list[dict[str, object]]]:
        def _job() -> list[dict[str, object]]:
            return handler(symbol)

        return _job

    def _make_ohlcv_job(market: str, symbol: str) -> Callable[[], list[dict[str, object]]]:
        def _job() -> list[dict[str, object]]:
            return _run_ohlcv(market, symbol)

        return _job

    for market in cast(list[str], selected):
        spec = SILVER_BUILD_SPECS[market]
        effective_symbols = _discover_effective_symbols(market, spec)
        logger.info("Silver build schedule market=%s symbols=%s timeframe=%s", market, effective_symbols, timeframe)
        handler = market_handlers.get(market)
        for symbol in effective_symbols:
            if handler is not None:
                jobs.append((market, symbol, _make_handler_job(handler, symbol)))
            else:
                jobs.append((market, symbol, _make_ohlcv_job(market, symbol)))

    logger.info("Silver build parallelization maxprocesses=%s jobs=%s", maxprocesses, len(jobs))
    with ThreadPoolExecutor(max_workers=maxprocesses) as executor:
        futures = [executor.submit(job) for _, _, job in jobs]
        for future in futures:
            reports.extend(future.result())

    if not bool(args.no_json_output):
        print(json.dumps({"reports": reports}, indent=2))
    logger.info("Command complete: silver-build reports=%s", len(reports))
