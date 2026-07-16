"""Tests for the five-module ISIN selection and statistics contracts."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from application.services.isin_selection_service import (
    run_bivariate_statistics,
    run_metadata_filter,
    run_univariate_filter,
    run_univariate_statistics,
)


def _write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_metadata_filter_persists_hash_addressable_conjunctive_selection(tmp_path: Path) -> None:
    """metadata_filter should filter all_isins and persist a stable selection artifact."""

    source = tmp_path / "all_isins.csv"
    _write_csv(
        source,
        [
            {"isin": "US1", "exchange": "US", "type": "ETF", "currency": "USD"},
            {"isin": "DE1", "exchange": "XETRA", "type": "ETF", "currency": "EUR"},
            {"isin": "US2", "exchange": "US", "type": "COMMON", "currency": "USD"},
        ],
        ["isin", "exchange", "type", "currency"],
    )

    result = run_metadata_filter(
        all_isins_csv=source,
        output_root=tmp_path / "metadata_filter",
        predicates=["exchange=US", "type=ETF"],
        selection_name=None,
    )

    assert result.rows == 1
    assert result.selection_id.endswith(result.selection_hash)
    assert _read_csv(result.isins_path) == [{"isin": "US1", "exchange": "US", "type": "ETF", "currency": "USD"}]
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["module"] == "metadata_filter"
    assert manifest["predicates"] == ["exchange=US", "type=ETF"]


def test_univariate_statistics_and_filter_are_selection_compatible(tmp_path: Path) -> None:
    """univariate_statistics output should feed univariate_filter selections."""

    prices = tmp_path / "prices.csv"
    _write_csv(
        prices,
        [
            {"isin": "A", "date": "2026-01-01", "adjusted_close": "100"},
            {"isin": "A", "date": "2026-01-02", "adjusted_close": "110"},
            {"isin": "A", "date": "2026-01-03", "adjusted_close": "121"},
            {"isin": "B", "date": "2026-01-01", "adjusted_close": "100"},
            {"isin": "B", "date": "2026-01-02", "adjusted_close": "90"},
            {"isin": "B", "date": "2026-01-03", "adjusted_close": "81"},
        ],
        ["isin", "date", "adjusted_close"],
    )
    stats = run_univariate_statistics(
        prices_csv=prices,
        output_csv=tmp_path / "stats" / "univariate.csv",
        manifest_json=tmp_path / "stats" / "univariate.json",
        price_column="adjusted_close",
    )

    assert stats.rows == 2
    filtered = run_univariate_filter(
        statistics_csv=stats.output_csv,
        output_root=tmp_path / "univariate_filter",
        predicates=["mean_log_return>0"],
        selection_name="positive_returns",
    )

    assert filtered.selection_name == "positive_returns"
    assert _read_csv(filtered.isins_path)[0]["isin"] == "A"


def test_bivariate_statistics_uses_unique_pairs_and_date_intersection(tmp_path: Path) -> None:
    """bivariate_statistics should compute each pair once on common return dates only."""

    selection = tmp_path / "selection.csv"
    _write_csv(selection, [{"isin": "A"}, {"isin": "B"}, {"isin": "C"}], ["isin"])
    prices = tmp_path / "prices.csv"
    _write_csv(
        prices,
        [
            {"isin": "A", "date": "2026-01-01", "adjusted_close": "100"},
            {"isin": "A", "date": "2026-01-02", "adjusted_close": "110"},
            {"isin": "A", "date": "2026-01-03", "adjusted_close": "121"},
            {"isin": "B", "date": "2026-01-01", "adjusted_close": "200"},
            {"isin": "B", "date": "2026-01-02", "adjusted_close": "220"},
            {"isin": "B", "date": "2026-01-03", "adjusted_close": "242"},
            {"isin": "C", "date": "2026-01-01", "adjusted_close": "50"},
            {"isin": "C", "date": "2026-01-04", "adjusted_close": "55"},
        ],
        ["isin", "date", "adjusted_close"],
    )

    result = run_bivariate_statistics(
        selection_csv=selection,
        prices_csv=prices,
        output_csv=tmp_path / "bivariate.csv",
        manifest_json=tmp_path / "bivariate.json",
        price_column="adjusted_close",
        min_overlap=2,
    )

    rows = _read_csv(result.output_csv)
    assert rows == [
        {
            "isin_left": "A",
            "isin_right": "B",
            "common_return_observations": "2",
            "covariance": "0",
            "pearson_correlation": "",
        }
    ]
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["pair_policy"] == "unique unordered ISIN pairs only"
    assert manifest["alignment"] == "date intersection of daily log returns"
