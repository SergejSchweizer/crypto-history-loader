"""Silver build command for spot/perp OHLCV transformation."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

from application.services.silver_service import (
    SilverBuildReport,
    build_funding_1m_feature_for_symbol,
    build_funding_observed_for_symbol,
    build_oi_1m_feature_for_symbol,
    build_oi_observed_for_symbol,
    build_perp_trades_1m_feature_for_symbol,
    build_perp_trades_observed_for_symbol,
    build_silver_for_symbol,
    build_volatility_observed_for_symbol,
    discover_symbols,
    write_monthly_sidecars,
)
from ingestion.funding import DERIBIT_FUNDING_NATIVE_INTERVAL

_MARKET_DISCOVERY_CONFIG: dict[str, tuple[str, str, str]] = {
    "funding": ("funding", "perp", DERIBIT_FUNDING_NATIVE_INTERVAL),
    "oi": ("oi", "perp", "1m"),
    "perp_trades": ("perp_trades", "perp", "tick"),
    "option_trades": ("option_trades", "option", "tick"),
    "volatility_index_data": ("volatility_index_data", "perp", "1m"),
}


def add_silver_build_parser(subparsers: Any) -> None:
    """Register ``silver-build`` parser."""

    parser = subparsers.add_parser("silver-build", help="Build silver monthly parquet outputs from bronze data")
    parser.add_argument("--bronze-root", default="lake/bronze", help="Bronze lake root")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument(
        "--dataset",
        nargs="+",
        choices=[
            "spot",
            "perp",
            "oi",
            "funding",
            "perp_trades",
            "option_trades",
            "volatility_index_data",
        ],
        default=[
            "spot",
            "perp",
            "oi",
            "funding",
            "perp_trades",
            "option_trades",
            "volatility_index_data",
        ],
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

    def _run_oi(symbol: str) -> list[dict[str, object]]:
        observed = build_oi_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        observed_payload = _report_payload("oi_observed", symbol, observed)

        feature = build_oi_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe=timeframe,
        )
        feature_payload = _report_payload("oi_1m_feature", symbol, feature)
        logger.info(
            "Silver OI reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_trades(symbol: str) -> list[dict[str, object]]:
        observed = build_perp_trades_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            instrument_type="perp",
            timeframe="tick",
        )
        observed_payload = _report_payload("perp_trades_observed", symbol, observed)
        feature = build_perp_trades_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe="tick",
        )
        feature_payload = _report_payload("perp_trades_1m_feature", symbol, feature)
        logger.info(
            "Silver trades reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )
        return [observed_payload, feature_payload]

    def _run_option_trades(symbol: str) -> list[dict[str, object]]:
        observed = build_perp_trades_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            instrument_type="option",
            timeframe="tick",
            bronze_dataset_type="option_trades",
            output_dataset_type="option_trades_observed",
        )
        observed_payload = _report_payload("option_trades_observed", symbol, observed)
        feature = build_perp_trades_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe="tick",
            observed_dataset_type="option_trades_observed",
            output_dataset_type="option_trades_1m_feature",
        )
        feature_payload = _report_payload("option_trades_1m_feature", symbol, feature)
        logger.info(
            "Silver option_trades reports written symbol=%s observed_rows=%s feature_rows=%s",
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
        logger.info(
            "Silver volatility_index_data report written symbol=%s observed_rows=%s",
            symbol,
            observed.rows_out,
        )
        return [observed_payload]

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
        "oi": _run_oi,
        "perp_trades": _run_trades,
        "option_trades": _run_option_trades,
        "volatility_index_data": _run_volatility_index_data,
    }

    def _discovery_params_for_market(market: str, default_timeframe: str) -> tuple[str, str, str]:
        configured = _MARKET_DISCOVERY_CONFIG.get(market)
        if configured is None:
            return market, market, default_timeframe
        bronze_dataset, bronze_instrument, configured_timeframe = configured
        discovery_timeframe = default_timeframe if configured_timeframe == "1m" else configured_timeframe
        return bronze_dataset, bronze_instrument, discovery_timeframe

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
        symbols = cast(list[str] | None, args.symbols)
        bronze_dataset, bronze_instrument, discovery_timeframe = _discovery_params_for_market(market, timeframe)
        effective_symbols = symbols or discover_symbols(
            bronze_root=bronze_root,
            market=bronze_dataset,
            exchange=exchange,
            timeframe=discovery_timeframe,
            instrument_type=bronze_instrument,
        )
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
