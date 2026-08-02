"""Tests for plot helper utilities."""

from __future__ import annotations

import builtins
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.feature_profile import (
    _normalized_series,
    _ordered_numeric_columns,
    _sample_frame_for_plot,
    _time_axis_style,
    write_feature_distribution_plot,
)
from ingestion.plotting import (
    build_plot_filename,
    price_value,
    save_candle_plots,
    save_funding_plot,
    save_open_interest_plot,
)
from ingestion.spot_ohlcv import SpotCandle


def _sample_candle() -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=datetime(2026, 1, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=110.0,
        low_price=95.0,
        close_price=105.0,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=20,
    )


def test_price_value_selector() -> None:
    candle = _sample_candle()
    assert price_value(candle, "spot_ohlcv") == 105.0
    assert price_value(candle, "close") == 105.0
    assert price_value(candle, "open") == 100.0
    assert price_value(candle, "high") == 110.0
    assert price_value(candle, "low") == 95.0


def test_build_plot_filename_sanitizes_inputs() -> None:
    file_name = build_plot_filename(
        exchange="deribit",
        symbol="BTC/PERPETUAL",
        interval="1m",
        price_field="close",
    )
    assert file_name.endswith(".png")
    assert "20260101" not in file_name
    assert "BTC_PERPETUAL" in file_name


def test_save_candle_plots_skips_empty_and_writes_png(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")
    candle = _sample_candle()
    out = save_candle_plots(
        {
            "deribit": {
                "BTCUSDT": [candle],
                "EMPTY": [],
            }
        },
        output_dir=str(tmp_path),
        price_field="close",
    )
    assert len(out) == 1
    assert Path(out[0]).exists()
    assert out[0].endswith(".png")


def test_save_open_interest_plot_empty_input_returns_path(tmp_path: Path) -> None:
    out_path = str(tmp_path / "open_interest.png")
    assert save_open_interest_plot("deribit", "BTC", "1m", [], [], out_path) == out_path


def test_save_funding_plot_empty_input_returns_path(tmp_path: Path) -> None:
    out_path = str(tmp_path / "funding.png")
    assert save_funding_plot("deribit", "BTC", "8h", [], [], out_path) == out_path


def test_save_open_interest_and_funding_plots_write_png(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC), datetime(2026, 1, 1, 0, 1, tzinfo=UTC)]
    open_interest_path = save_open_interest_plot(
        "deribit",
        "BTC",
        "1m",
        times,
        [100.0, 101.0],
        str(tmp_path / "open_interest" / "plot.png"),
    )
    funding_path = save_funding_plot(
        "deribit",
        "BTC",
        "8h",
        times,
        [0.001, -0.002],
        str(tmp_path / "funding" / "plot.png"),
    )
    assert Path(open_interest_path).exists()
    assert Path(funding_path).exists()


def test_plot_functions_raise_runtime_error_when_matplotlib_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    real_import = builtins.__import__

    def _fake_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] = (),
        level: int = 0,
    ) -> object:
        if name.startswith("matplotlib") or name == "polars":
            raise ImportError("optional dependency missing")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    with pytest.raises(RuntimeError, match="matplotlib is required"):
        save_candle_plots({"deribit": {"BTCUSDT": [_sample_candle()]}}, str(tmp_path), "close")
    with pytest.raises(RuntimeError, match="matplotlib is required"):
        save_open_interest_plot(
            "deribit",
            "BTC",
            "1m",
            [datetime(2026, 1, 1, 0, 0, tzinfo=UTC)],
            [1.0],
            str(tmp_path / "open_interest.png"),
        )
    with pytest.raises(RuntimeError, match="matplotlib is required"):
        save_funding_plot(
            "deribit",
            "BTC",
            "8h",
            [datetime(2026, 1, 1, 0, 0, tzinfo=UTC)],
            [0.1],
            str(tmp_path / "funding.png"),
        )


def test_feature_profile_helpers_handle_sampling_numeric_order_and_normalization() -> None:
    """Feature-profile helpers keep plotting deterministic across sparse and flat data."""

    frame = __import__("polars").DataFrame(
        {
            "timestamp_m1": [datetime(2026, 1, 1, tzinfo=UTC)] * 3,
            "spot_ohlcv_close_price": [1.0, 2.0, 3.0],
            "funding_rate": [None, 0.1, 0.2],
            "l2_spread": [1.0, 1.0, 1.0],
            "other": ["x", "y", "z"],
        }
    )
    assert _ordered_numeric_columns(frame) == ["spot_ohlcv_close_price", "funding_rate", "l2_spread"]
    assert _sample_frame_for_plot(frame) is frame
    values, normalized, missing = _normalized_series([1, None, 3])
    assert values == [1.0, 3.0]
    assert normalized[0] == 0.0 and math.isnan(normalized[1]) and normalized[2] == 1.0
    assert missing == 1
    assert _normalized_series([2, 2]) == ([2.0, 2.0], [0.0, 0.0], 0)
    assert _normalized_series([None]) == ([], [], 1)


def test_feature_profile_sampling_large_frames_and_axis_style() -> None:
    """Large profile inputs are sampled while time-axis styles remain type-aware."""

    pl = __import__("polars")
    timestamps = [datetime(2026, 1, 1, tzinfo=UTC) + __import__("datetime").timedelta(minutes=i) for i in range(3_100)]
    frame = pl.DataFrame({"timestamp_m1": timestamps, "value": list(range(3_100))})
    sampled = _sample_frame_for_plot(frame)
    assert sampled.height == 3_000
    import matplotlib.dates as mdates
    import matplotlib.ticker as mticker

    major, minor, _formatter = _time_axis_style(mdates, mticker, [1, 2])
    assert minor is None
    major, minor, _formatter = _time_axis_style(mdates, mticker, timestamps[:2])
    assert minor is None
    assert major is not None


def test_feature_profile_writes_numeric_profile_and_skips_non_numeric(tmp_path: Path) -> None:
    """Feature profile generation should emit a plot only when numeric features exist."""

    pl = __import__("polars")
    frame = pl.DataFrame(
        {
            "timestamp_m1": [
                datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
                datetime(2026, 1, 1, 0, 2, tzinfo=UTC),
            ],
            "exchange": ["deribit"] * 3,
            "symbol": ["BTC"] * 3,
            "spot_ohlcv_close_price": [100.0, None, 102.0],
        }
    )
    output = tmp_path / "profile.png"
    assert write_feature_distribution_plot(frame, output, normalize_y=False) == str(output.resolve())
    assert output.exists()
    text_only = pl.DataFrame({"label": ["a", "b"]})
    assert write_feature_distribution_plot(text_only, tmp_path / "empty.png") is None


def test_feature_profile_handles_dependency_and_axis_edge_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Optional plotting dependencies and all supported time spans have explicit fallbacks."""

    import ingestion.feature_profile as profile

    real_import = builtins.__import__

    def _missing_matplotlib(name: str, *args: object, **kwargs: object) -> object:
        if name.startswith("matplotlib"):
            raise ImportError("missing matplotlib")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _missing_matplotlib)
    assert profile.write_feature_distribution_plot({}, Path("missing.png")) is None
    assert profile._iso_utc(None) is None
    with pytest.raises(RuntimeError, match="polars is required"):
        profile._require_polars()

    monkeypatch.undo()
    import matplotlib.dates as mdates
    import matplotlib.ticker as mticker

    for hours in (1, 12, 72, 24 * 30):
        timestamps = [datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)]
        timestamps[1] = timestamps[0] + __import__("datetime").timedelta(hours=hours)
        major, minor, formatter = profile._time_axis_style(mdates, mticker, timestamps)
        assert major is not None
        assert minor is not None
        assert formatter is not None
    major, minor, formatter = profile._time_axis_style(mdates, mticker, [])
    assert major is not None and minor is None and formatter is not None
