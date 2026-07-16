"""Tests for typed Bronze build request/result contracts."""

from __future__ import annotations

from api import cli
from api.cli import build_parser
from application.bronze_contracts import (
    BronzeBuildRequest,
    BronzeDatasetSelection,
    BronzeRuntimeContext,
    bronze_build_request_from_args,
    bronze_build_result_from_counters,
)


def test_bronze_build_request_default_matches_parser_defaults() -> None:
    args = build_parser().parse_args(["bronze-build"])

    request = bronze_build_request_from_args(args)

    assert request == BronzeBuildRequest(
        dataset_selection=BronzeDatasetSelection(
            exchange="deribit",
            exchanges=(),
            data_types=("spot_ohlcv",),
            symbols=("BTC", "ETH", "SOL"),
        ),
        runtime_context=BronzeRuntimeContext(
            tail_delta_only=False,
            start_date=None,
            symbol_start_dates=(),
            exchange_symbol_start_dates=(),
        ),
        lake_root="lake/bronze",
        save_parquet_lake=False,
        no_json_output=False,
        debug=False,
        invoked_via_cli=False,
    )


def test_bronze_build_request_explicit_dataset_selection() -> None:
    args = build_parser().parse_args(
        [
            "bronze-build",
            "--exchange",
            "deribit",
            "--exchanges",
            "deribit",
            "--dataset",
            "perps_trades",
            "options_trades",
            "--symbols",
            "BTC",
            "ETH",
        ]
    )

    request = bronze_build_request_from_args(args)

    assert request.dataset_selection == BronzeDatasetSelection(
        exchange="deribit",
        exchanges=("deribit",),
        data_types=("perps_trades", "options_trades"),
        symbols=("BTC", "ETH"),
    )


def test_bronze_build_request_explicit_time_bounds() -> None:
    args = build_parser().parse_args(
        [
            "bronze-build",
            "--tail-delta-only",
            "--start-date",
            "2023-04-24",
            "--symbol-start-dates",
            "BTC=2023-04-24",
            "ETH=2023-05-01",
            "--exchange-symbol-start-dates",
            "deribit:BTC=2023-04-24",
        ]
    )

    request = bronze_build_request_from_args(args)

    assert request.runtime_context == BronzeRuntimeContext(
        tail_delta_only=True,
        start_date="2023-04-24",
        symbol_start_dates=("BTC=2023-04-24", "ETH=2023-05-01"),
        exchange_symbol_start_dates=("deribit:BTC=2023-04-24",),
    )


def test_bronze_build_request_full_gap_fill_flag_overrides_tail_delta_only() -> None:
    args = build_parser().parse_args(["bronze-build", "--tail-delta-only", "--full-gap-fill"])

    request = bronze_build_request_from_args(args)

    assert request.runtime_context.tail_delta_only is False


def test_bronze_build_request_debug_flag() -> None:
    args = build_parser().parse_args(["--debug", "bronze-build"])

    request = bronze_build_request_from_args(args)

    assert request.debug is True


def test_bronze_build_request_save_parquet_lake_and_no_json_output_flags() -> None:
    args = build_parser().parse_args(
        ["bronze-build", "--save-parquet-lake", "--lake-root", "lake/custom-bronze", "--no-json-output"]
    )

    request = bronze_build_request_from_args(args)

    assert request.save_parquet_lake is True
    assert request.lake_root == "lake/custom-bronze"
    assert request.no_json_output is True


def test_bronze_build_request_invoked_via_cli_flag() -> None:
    args = build_parser().parse_args(["bronze-build"])
    args._invoked_via_cli = True

    request = bronze_build_request_from_args(args)

    assert request.invoked_via_cli is True


def test_bronze_build_request_is_deterministic_across_repeated_conversion() -> None:
    args = build_parser().parse_args(["bronze-build", "--dataset", "funding", "--symbols", "BTC"])

    first = bronze_build_request_from_args(args)
    second = bronze_build_request_from_args(args)

    assert first == second
    assert hash(first) == hash(second)


def test_bronze_build_request_respects_config_env_override_precedence() -> None:
    parser = build_parser()
    args = parser.parse_args(["bronze-build", "--symbols", "BTC"])
    command_parser = cli._subparser_for_command(parser, "bronze-build")
    assert command_parser is not None
    explicit_dests = cli._collect_explicit_cli_dests(command_parser, ["bronze-build", "--symbols", "BTC"])
    config: dict[str, object] = {
        "bronze-build": {
            "symbols": ["ETH", "SOL"],
            "lake_root": "lake/from-config",
            "start_date": "2024-01-01",
        }
    }

    cli._apply_yaml_defaults(args=args, command="bronze-build", config=config, explicit_dests=explicit_dests)
    request = bronze_build_request_from_args(args)

    # Explicit CLI flag wins over the config default.
    assert request.dataset_selection.symbols == ("BTC",)
    # Config default applies where no CLI flag was given.
    assert request.lake_root == "lake/from-config"
    assert request.runtime_context.start_date == "2024-01-01"


def test_bronze_build_result_from_counters_snapshots_inputs() -> None:
    success_counts = {"spot_ohlcv": 3}
    error_counts = {"spot_ohlcv": 1}
    sidecars = ["lake/bronze/a.json"]

    result = bronze_build_result_from_counters(
        data_types=["spot_ohlcv"],
        symbols=["BTC"],
        success_counts_by_dataset=success_counts,
        error_counts_by_dataset=error_counts,
        sidecars_written=sidecars,
    )

    assert result.data_types == ("spot_ohlcv",)
    assert result.symbols == ("BTC",)
    assert result.success_counts_by_dataset == {"spot_ohlcv": 3}
    assert result.error_counts_by_dataset == {"spot_ohlcv": 1}
    assert result.sidecars_written == ("lake/bronze/a.json",)

    # Mutating the caller's inputs after the fact must not affect the snapshot.
    success_counts["spot_ohlcv"] = 999
    sidecars.append("lake/bronze/b.json")
    assert result.success_counts_by_dataset == {"spot_ohlcv": 3}
    assert result.sidecars_written == ("lake/bronze/a.json",)
