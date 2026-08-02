"""Integration tests for the canonical historical full Gold dataset."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.dataset_contracts import SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS, gold_dataset_contract
from application.services.gold_service import build_gold_for_symbol, build_gold_timeframe_fanout_for_symbol
from tests.test_gold_regime_features import _write_silver

pl = pytest.importorskip("polars")


def _manifest(path: str | None) -> dict[str, object]:
    assert path is not None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_trade_features(silver: Path, timestamps: list[datetime]) -> None:
    for dataset_type in ("perps_trades_1m_feature", "options_trades_1m_feature"):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol="BTC-PERPETUAL" if dataset_type == "perps_trades_1m_feature" else "BTC",
            timeframe="1m",
            rows=[
                {
                    "timestamp_m1": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "instrument_type": "perpetual" if dataset_type == "perps_trades_1m_feature" else "option",
                    "open_price": 100.0 + index,
                    "high_price": 101.0 + index,
                    "low_price": 99.0 + index,
                    "close_price": 100.5 + index,
                    "volume": 10.0 + index,
                    "quote_volume": 1000.0 + index,
                    "trade_count": 20 + index,
                    "buy_volume": 6.0 + index,
                    "sell_volume": 4.0,
                    "buy_trade_count": 12 + index,
                    "sell_trade_count": 8,
                    "buy_volume_share": 0.6,
                }
                for index, timestamp in enumerate(timestamps)
            ],
        )


def _write_history_sources(
    silver: Path,
    timestamps: list[datetime],
    *,
    include_historical_prediction: bool,
) -> None:
    """Write the Silver sources used by the history-full Gold contracts."""

    spot_timestamps = timestamps[1:]
    for dataset_type, symbol, source_timestamps, scale in (
        ("spot_ohlcv", "BTC_USDC", spot_timestamps, 1.0),
        ("perps_ohlcv", "BTC-PERPETUAL", timestamps, 10.0),
    ):
        _write_silver(
            silver,
            dataset_type=dataset_type,
            symbol=symbol,
            timeframe="1m",
            rows=[
                {
                    "open_time": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    "open_price": scale + index,
                    "high_price": scale + index + 1.0,
                    "low_price": scale + index - 0.5,
                    "close_price": scale + index + 0.5,
                    "volume": 100.0 + index,
                }
                for index, timestamp in enumerate(source_timestamps)
            ],
        )
    _write_silver(
        silver,
        dataset_type="funding_1m_feature",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        rows=[
            {
                "timestamp": timestamp,
                "exchange": "deribit",
                "symbol": "BTC-PERPETUAL",
                "funding_rate_last_known": 0.001,
                "minutes_since_funding": index,
                "is_funding_observation_minute": index == 0,
                "funding_data_available": True,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_silver(
        silver,
        dataset_type="open_interest_1m_feature",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        rows=[
            {
                "timestamp_m1": timestamp,
                "exchange": "deribit",
                "symbol": "BTC-PERPETUAL",
                "open_interest": 1000.0 + index,
                "open_interest_is_observed": True,
                "open_interest_is_ffill": False,
                "minutes_since_open_interest_observation": 0,
                "open_interest_observation_lag_sec": 0,
            }
            for index, timestamp in enumerate(timestamps)
        ],
    )
    _write_trade_features(silver, timestamps)
    if include_historical_prediction:
        _write_silver(
            silver,
            dataset_type="historical_prediction_1m_feature",
            symbol="BTC",
            timeframe="1m",
            rows=[
                {
                    "timestamp_m1": timestamp,
                    "exchange": "deribit",
                    "symbol": "BTC",
                    **{
                        column: float(index + offset)
                        for offset, column in enumerate(SILVER_HISTORICAL_PREDICTION_FEATURE_COLUMNS[3:], start=1)
                    },
                }
                for index, timestamp in enumerate(timestamps)
            ],
        )


def test_history_full_gold_joins_historical_sources_without_targets(tmp_path: Path) -> None:
    """The historical full dataset should contain only raw-Bronze-backed historical families."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(2)]
    silver = tmp_path / "silver"
    _write_history_sources(silver, timestamps, include_historical_prediction=False)

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-history-full"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.full.m1",
    )
    history_full = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert history_full.height == 2
    assert history_full["timestamp_m1"].to_list() == timestamps
    assert "spot_ohlcv_close_price" in history_full.columns
    assert "perp_close_price" in history_full.columns
    assert "funding_rate_last_known" in history_full.columns
    assert "open_interest_open_interest" in history_full.columns
    assert "perps_trades_close_price" in history_full.columns
    assert "options_trades_close_price" in history_full.columns
    assert history_full.columns == [
        "timestamp_m1",
        "exchange",
        "symbol",
        "spot_ohlcv_open_price",
        "spot_ohlcv_high_price",
        "spot_ohlcv_low_price",
        "spot_ohlcv_close_price",
        "spot_ohlcv_volume",
        "spot_ohlcv_quote_volume",
        "spot_ohlcv_trade_count",
        "perp_open_price",
        "perp_high_price",
        "perp_low_price",
        "perp_close_price",
        "perp_volume",
        "perp_quote_volume",
        "perp_trade_count",
        "funding_rate_last_known",
        "funding_observed_at",
        "minutes_since_funding",
        "is_funding_observation_minute",
        "funding_data_available",
        "open_interest_open_interest",
        "open_interest_is_observed",
        "open_interest_is_ffill",
        "minutes_since_open_interest_observation",
        "open_interest_observation_lag_sec",
        "open_interest_source_timestamp",
        "perps_trades_open_price",
        "perps_trades_high_price",
        "perps_trades_low_price",
        "perps_trades_close_price",
        "perps_trades_volume",
        "perps_trades_quote_volume",
        "perps_trades_trade_count",
        "perps_trades_buy_volume",
        "perps_trades_sell_volume",
        "perps_trades_buy_trade_count",
        "perps_trades_sell_trade_count",
        "perps_trades_buy_volume_share",
        "options_trades_open_price",
        "options_trades_high_price",
        "options_trades_low_price",
        "options_trades_close_price",
        "options_trades_volume",
        "options_trades_quote_volume",
        "options_trades_trade_count",
        "options_trades_buy_volume",
        "options_trades_sell_volume",
        "options_trades_buy_trade_count",
        "options_trades_sell_trade_count",
        "options_trades_buy_volume_share",
    ]
    assert "rv_1h" not in history_full.columns
    assert "iv_minus_rv_1h" not in history_full.columns
    assert "strategy_momentum_log_return_1m" not in history_full.columns
    assert "strategy_reversion_half_life_5m" not in history_full.columns
    assert "historical_volatility_reference" not in history_full.columns
    assert "perps_trades_buy_volume_share" in history_full.columns
    assert "options_trades_sell_trade_count" in history_full.columns
    assert "minutes_since_open_interest_observation" in history_full.columns
    assert "funding_data_available" in history_full.columns
    assert "historical_prediction_perps_rv_1h" not in history_full.columns
    assert history_full["spot_ohlcv_close_price"].to_list()[0] is None
    assert not any(column.startswith(("target_", "label_")) for column in history_full.columns)
    assert manifest["dataset_id"] == "gold.history.full.m1"
    assert manifest["required_source_datasets"] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "perps_trades_1m_feature",
        "options_trades_1m_feature",
    ]
    assert manifest["optional_source_datasets"] == []
    assert manifest["strategy_feature_lookbacks"] == {}
    assert manifest["prediction_target_definitions"] == {}
    assert manifest["feature_metadata"]["perps_trades_close_price"]["source_dataset"] == "perps_trades_1m_feature"


def test_history_full_gold_contract_declares_canonical_historical_sources() -> None:
    """The typed historical full contract should declare historical sources explicitly."""

    contract = gold_dataset_contract("gold.history.full.m1")
    assert [requirement.dataset_type for requirement in contract.requirements] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "perps_trades_1m_feature",
        "options_trades_1m_feature",
    ]
    assert contract.optional_requirements == ()


def test_extended_history_full_gold_includes_historical_prediction_features(tmp_path: Path) -> None:
    """The extended history-full dataset should retain historical prediction features."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(6)]
    silver = tmp_path / "silver"
    _write_history_sources(silver, timestamps, include_historical_prediction=True)

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-history-full"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.extended_full.m1",
    )
    extended_history_full = pl.read_parquet(report.parquet_path).sort("timestamp_m1")

    assert report.dataset_id == "gold.history.extended_full.m1"
    assert "historical_prediction_perps_rv_1h" in extended_history_full.columns
    assert "historical_prediction_short_stress_signal" in extended_history_full.columns


def test_extended_history_gold_alias_includes_historical_prediction_features(tmp_path: Path) -> None:
    """The new extended history dataset alias should match the extended feature contract."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(2)]
    silver = tmp_path / "silver"
    _write_history_sources(silver, timestamps, include_historical_prediction=True)

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(tmp_path / "gold-history-extended"),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.extended.m1",
    )
    extended_history = pl.read_parquet(report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(report.manifest_path)

    assert report.dataset_id == "gold.history.extended.m1"
    assert "historical_prediction_perps_rv_1h" in extended_history.columns
    assert "historical_prediction_short_stress_signal" in extended_history.columns
    assert manifest["required_source_datasets"] == [
        "spot_ohlcv",
        "perps_ohlcv",
        "funding_1m_feature",
        "open_interest_1m_feature",
        "perps_trades_1m_feature",
        "options_trades_1m_feature",
        "historical_prediction_1m_feature",
    ]


@pytest.mark.parametrize(
    ("dataset_id", "expected_rows"),
    [
        ("gold.history.extended.m5", 12),
        ("gold.history.extended.m30", 2),
        ("gold.history.extended.h1", 1),
    ],
)
def test_extended_history_gold_derives_coarser_timeframes_from_extended_minute_artifact(
    tmp_path: Path,
    dataset_id: str,
    expected_rows: int,
) -> None:
    """Extended history-derived datasets should resample the extended minute artifact."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(60)]
    silver = tmp_path / "silver"
    gold = tmp_path / "gold-history-extended"
    _write_history_sources(silver, timestamps, include_historical_prediction=True)

    minute_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.extended.m1",
    )
    assert minute_report.rows_out == 60

    derived_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange="deribit",
        symbol="BTC",
        dataset_id=dataset_id,
    )
    derived = pl.read_parquet(derived_report.parquet_path).sort("timestamp_m1")
    manifest = _manifest(derived_report.manifest_path)

    assert derived_report.dataset_id == dataset_id
    assert derived_report.rows_out == expected_rows
    assert derived["timestamp_m1"].to_list()[0] == timestamps[0]
    assert "historical_prediction_perps_rv_1h" in derived.columns
    assert "historical_prediction_short_stress_signal" in derived.columns
    assert manifest["source_dataset_id"] == "gold.history.extended.m1"
    assert manifest["required_source_datasets"] == ["gold.history.extended.m1"]


def test_history_full_gold_derives_coarser_timeframes_from_minute_artifact(tmp_path: Path) -> None:
    """Coarser history-full datasets should be resampled from the canonical minute artifact."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(6)]
    silver = tmp_path / "silver"
    gold = tmp_path / "gold-history-full"
    _write_history_sources(silver, timestamps, include_historical_prediction=False)

    minute_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.full.m1",
    )
    assert minute_report.rows_out == 6

    resampled_report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.full.m5",
    )
    resampled = pl.read_parquet(resampled_report.parquet_path).sort("timestamp_m1")

    assert resampled_report.dataset_id == "gold.history.full.m5"
    assert resampled_report.rows_out == 2
    assert resampled["timestamp_m1"].to_list() == [
        datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 5, 1, 0, 5, tzinfo=UTC),
    ]
    assert resampled["spot_ohlcv_open_price"].to_list() == [1.0, 5.0]
    assert resampled["spot_ohlcv_close_price"].to_list() == [4.5, 5.5]
    assert resampled["perps_trades_volume"].to_list() == [60.0, 15.0]
    assert resampled["perps_trades_buy_volume_share"].to_list() == [
        pytest.approx(40.0 / 60.0),
        pytest.approx(11.0 / 15.0),
    ]
    assert resampled["options_trades_trade_count"].to_list() == [110, 25]


def test_history_full_gold_fanout_reads_once_and_publishes_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sibling fan-out must reuse one M1 read and publish every requested timeframe."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(60)]
    silver = tmp_path / "silver"
    gold = tmp_path / "gold-history-fanout"
    _write_history_sources(silver, timestamps, include_historical_prediction=False)
    build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.full.m1",
    )
    import application.services.gold_service as gold_service

    reads = 0
    original_read = gold_service._read_latest_gold_dataset_artifact

    def _counted_read(**kwargs: object) -> tuple[object, Path, dict[str, object]]:
        nonlocal reads
        reads += 1
        return original_read(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(gold_service, "_read_latest_gold_dataset_artifact", _counted_read)
    reports = build_gold_timeframe_fanout_for_symbol(
        gold_root=str(gold),
        exchange="deribit",
        symbol="BTC",
        dataset_ids=["gold.history.full.h1", "gold.history.full.m5", "gold.history.full.m30"],
    )

    assert reads == 1
    assert [report.dataset_id for report in reports] == [
        "gold.history.full.h1",
        "gold.history.full.m30",
        "gold.history.full.m5",
    ]
    assert [pl.read_parquet(report.parquet_path).height for report in reports] == [1, 2, 12]


def test_history_full_gold_fanout_restores_siblings_after_publication_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed sibling transaction retains readable prior artifacts and a retry completes it."""

    timestamps = [datetime(2026, 5, 1, 0, minute, tzinfo=UTC) for minute in range(60)]
    silver = tmp_path / "silver"
    gold = tmp_path / "gold-history-fanout-retry"
    import application.services.gold_service as gold_service

    monkeypatch.setattr(gold_service, "_write_feature_distribution_plot", lambda *_args, **_kwargs: "plot.png")
    _write_history_sources(silver, timestamps, include_historical_prediction=False)
    build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange="deribit",
        symbol="BTC",
        dataset_id="gold.history.full.m1",
    )
    dataset_ids = ["gold.history.full.m5", "gold.history.full.m30"]
    first_reports = build_gold_timeframe_fanout_for_symbol(
        gold_root=str(gold), exchange="deribit", symbol="BTC", dataset_ids=dataset_ids
    )
    old_artifacts = {
        Path(path): Path(path).read_bytes()
        for report in first_reports
        for path in (report.parquet_path, report.manifest_path)
        if path is not None
    }
    import application.services.gold_publication as gold_publication

    original_validate = gold_publication._validate_parquet
    validations = 0

    def _fail_second_validation(path: Path) -> None:
        nonlocal validations
        validations += 1
        if validations == 2:
            raise OSError("injected sibling validation failure")
        original_validate(path)

    with monkeypatch.context() as context:
        context.setattr(gold_publication, "_validate_parquet", _fail_second_validation)
        with pytest.raises(OSError, match="injected sibling"):
            build_gold_timeframe_fanout_for_symbol(
                gold_root=str(gold), exchange="deribit", symbol="BTC", dataset_ids=dataset_ids
            )
    assert {path: path.read_bytes() for path in old_artifacts} == old_artifacts

    retry_reports = build_gold_timeframe_fanout_for_symbol(
        gold_root=str(gold), exchange="deribit", symbol="BTC", dataset_ids=dataset_ids
    )
    assert [pl.read_parquet(report.parquet_path).height for report in retry_reports] == [2, 12]
