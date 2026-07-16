"""Contracts for ISIN reference, selection, and statistics modules."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import combinations
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


@dataclass(frozen=True)
class SelectionResult:
    """Result metadata for a persisted ISIN selection."""

    selection_id: str
    selection_name: str
    selection_hash: str
    output_dir: Path
    rows: int
    manifest_path: Path
    isins_path: Path


@dataclass(frozen=True)
class StatisticsResult:
    """Result metadata for a persisted statistics dataset."""

    dataset: str
    rows: int
    output_csv: Path
    manifest_path: Path


@dataclass(frozen=True)
class Predicate:
    """Conjunctive row predicate parsed from CLI syntax."""

    column: str
    operator: str
    value: str

    def matches(self, row: dict[str, str]) -> bool:
        """Return whether a CSV row satisfies this predicate."""

        actual = row.get(self.column, "")
        parsed_actual = _float_or_none(actual)
        if self.operator == "=":
            return actual == self.value
        if self.operator == "!=":
            return actual != self.value
        if self.operator == "~":
            return self.value.lower() in actual.lower()
        if self.operator == ">=":
            return parsed_actual is not None and parsed_actual >= float(self.value)
        if self.operator == "<=":
            return parsed_actual is not None and parsed_actual <= float(self.value)
        if self.operator == ">":
            return parsed_actual is not None and parsed_actual > float(self.value)
        if self.operator == "<":
            return parsed_actual is not None and parsed_actual < float(self.value)
        raise ValueError(f"Unsupported predicate operator: {self.operator}")

    def canonical(self) -> str:
        """Return the stable predicate representation used for hashes and names."""

        return f"{self.column}{self.operator}{self.value}"


def parse_predicates(raw_predicates: Sequence[str]) -> tuple[Predicate, ...]:
    """Parse CLI predicate strings into stable conjunctive predicates."""

    operators = ("!=", ">=", "<=", "~", ">", "<", "=")
    parsed: list[Predicate] = []
    for raw in raw_predicates:
        expression = raw.strip()
        if not expression:
            continue
        for operator in operators:
            if operator in expression:
                column, value = expression.split(operator, 1)
                column = column.strip()
                value = value.strip()
                if not column or not value:
                    raise ValueError(f"Invalid predicate: {raw}")
                parsed.append(Predicate(column=column, operator=operator, value=value))
                break
        else:
            raise ValueError(f"Invalid predicate '{raw}'. Expected syntax like field=value or metric>=1.0")
    return tuple(parsed)


def run_metadata_filter(
    *,
    all_isins_csv: Path,
    output_root: Path,
    predicates: Sequence[str],
    selection_name: str | None,
) -> SelectionResult:
    """Filter the canonical all-ISIN source by metadata and persist a referencable selection."""

    parsed = parse_predicates(predicates)
    rows = [row for row in _read_csv_dicts(all_isins_csv) if all(predicate.matches(row) for predicate in parsed)]
    return _write_selection(
        module="metadata_filter",
        source_path=all_isins_csv,
        output_root=output_root,
        rows=rows,
        predicates=parsed,
        selection_name=selection_name,
    )


def run_univariate_filter(
    *,
    statistics_csv: Path,
    output_root: Path,
    predicates: Sequence[str],
    selection_name: str | None,
) -> SelectionResult:
    """Filter univariate statistics by metric predicates and persist a referencable selection."""

    parsed = parse_predicates(predicates)
    rows = [row for row in _read_csv_dicts(statistics_csv) if all(predicate.matches(row) for predicate in parsed)]
    return _write_selection(
        module="univariate_filter",
        source_path=statistics_csv,
        output_root=output_root,
        rows=rows,
        predicates=parsed,
        selection_name=selection_name,
    )


def run_univariate_statistics(
    *,
    prices_csv: Path,
    output_csv: Path,
    manifest_json: Path,
    price_column: str,
) -> StatisticsResult:
    """Compute per-ISIN log-return statistics from daily prices."""

    prices_by_isin: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in _read_csv_dicts(prices_csv):
        isin = row.get("isin", "").strip().upper()
        date = row.get("date", "").strip()
        price = _float_or_none(row.get(price_column, ""))
        if isin and date and price is not None and price > 0:
            prices_by_isin[isin].append((date, price))

    output_rows: list[dict[str, str]] = []
    for isin in sorted(prices_by_isin):
        series = sorted(prices_by_isin[isin], key=lambda item: item[0])
        returns = _log_returns([price for _, price in series])
        if not returns:
            continue
        output_rows.append(
            {
                "isin": isin,
                "observations": str(len(series)),
                "return_observations": str(len(returns)),
                "mean_log_return": _format_float(_mean(returns)),
                "volatility": _format_float(_sample_std(returns)),
                "cvar_95": _format_float(_cvar(returns, alpha=0.95)),
                "sharpe": _format_float(_sharpe(returns)),
                "sortino": _format_float(_sortino(returns)),
                "skewness": _format_float(_skewness(returns)),
                "max_drawdown": _format_float(_max_drawdown([price for _, price in series])),
                "log_price_slope": _format_float(_slope([math.log(price) for _, price in series])),
            }
        )

    _write_csv_dicts(output_csv, output_rows, _UNIVARIATE_FIELDS)
    _write_manifest(
        manifest_json,
        {
            "dataset": "univariate_statistics",
            "source_csv": str(prices_csv),
            "output_csv": str(output_csv),
            "price_column": price_column,
            "row_count": len(output_rows),
            "fields": _UNIVARIATE_FIELDS,
        },
    )
    return StatisticsResult(
        dataset="univariate_statistics",
        rows=len(output_rows),
        output_csv=output_csv,
        manifest_path=manifest_json,
    )


def run_bivariate_statistics(
    *,
    selection_csv: Path,
    prices_csv: Path,
    output_csv: Path,
    manifest_json: Path,
    price_column: str,
    min_overlap: int,
) -> StatisticsResult:
    """Compute pair statistics on the selected ISIN set using date intersections only."""

    selected_isins = _read_selection_isins(selection_csv)
    returns_by_isin = _daily_returns_by_isin(
        prices_csv=prices_csv, price_column=price_column, allowed_isins=selected_isins
    )
    rows: list[dict[str, str]] = []
    for left, right in combinations(sorted(returns_by_isin), 2):
        common_dates = sorted(set(returns_by_isin[left]).intersection(returns_by_isin[right]))
        if len(common_dates) < min_overlap:
            continue
        left_values = [returns_by_isin[left][date] for date in common_dates]
        right_values = [returns_by_isin[right][date] for date in common_dates]
        covariance = _sample_covariance(left_values, right_values)
        rows.append(
            {
                "isin_left": left,
                "isin_right": right,
                "common_return_observations": str(len(common_dates)),
                "covariance": _format_float(covariance),
                "pearson_correlation": _format_float(_pearson(left_values, right_values, covariance=covariance)),
            }
        )

    _write_csv_dicts(output_csv, rows, _BIVARIATE_FIELDS)
    _write_manifest(
        manifest_json,
        {
            "dataset": "bivariate_statistics",
            "selection_csv": str(selection_csv),
            "source_csv": str(prices_csv),
            "output_csv": str(output_csv),
            "price_column": price_column,
            "min_overlap": min_overlap,
            "row_count": len(rows),
            "pair_policy": "unique unordered ISIN pairs only",
            "alignment": "date intersection of daily log returns",
            "fields": _BIVARIATE_FIELDS,
        },
    )
    return StatisticsResult(
        dataset="bivariate_statistics",
        rows=len(rows),
        output_csv=output_csv,
        manifest_path=manifest_json,
    )


_UNIVARIATE_FIELDS = [
    "isin",
    "observations",
    "return_observations",
    "mean_log_return",
    "volatility",
    "cvar_95",
    "sharpe",
    "sortino",
    "skewness",
    "max_drawdown",
    "log_price_slope",
]
_BIVARIATE_FIELDS = [
    "isin_left",
    "isin_right",
    "common_return_observations",
    "covariance",
    "pearson_correlation",
]


def _write_selection(
    *,
    module: str,
    source_path: Path,
    output_root: Path,
    rows: list[dict[str, str]],
    predicates: Sequence[Predicate],
    selection_name: str | None,
) -> SelectionResult:
    canonical_predicates = [predicate.canonical() for predicate in predicates]
    resolved_name = selection_name or _selection_name(canonical_predicates)
    selection_hash = _selection_hash(module=module, source_path=source_path, predicates=canonical_predicates)
    selection_id = f"{resolved_name}_{selection_hash}"
    output_dir = output_root / selection_id
    isins_path = output_dir / "isins.csv"
    manifest_path = output_dir / "manifest.json"
    fieldnames = _ordered_fields(rows)
    _write_csv_dicts(isins_path, rows, fieldnames)
    _write_manifest(
        manifest_path,
        {
            "dataset": "isin_selection",
            "module": module,
            "selection_id": selection_id,
            "selection_name": resolved_name,
            "selection_hash": selection_hash,
            "source_csv": str(source_path),
            "predicates": canonical_predicates,
            "row_count": len(rows),
            "isins_csv": str(isins_path),
            "fields": fieldnames,
        },
    )
    return SelectionResult(
        selection_id=selection_id,
        selection_name=resolved_name,
        selection_hash=selection_hash,
        output_dir=output_dir,
        rows=len(rows),
        manifest_path=manifest_path,
        isins_path=isins_path,
    )


def _read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv_dicts(path: Path, rows: Sequence[dict[str, str]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, newline="") as temp_handle:
        writer = csv.DictWriter(temp_handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
        temp_name = temp_handle.name
    os.replace(temp_name, path)


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    payload = {"created_at_utc": _utc_now(), **payload}
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as temp_handle:
        temp_handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        temp_name = temp_handle.name
    os.replace(temp_name, path)


def _ordered_fields(rows: Sequence[dict[str, str]]) -> list[str]:
    fields: list[str] = []
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    return fields or ["isin"]


def _selection_name(predicates: Sequence[str]) -> str:
    if not predicates:
        return "all"
    slug = "_".join(_slugify(predicate) for predicate in predicates)
    return slug[:80] or "selection"


def _selection_hash(*, module: str, source_path: Path, predicates: Sequence[str]) -> str:
    payload = json.dumps(
        {"module": module, "source_path": str(source_path), "predicates": list(predicates)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _slugify(value: str) -> str:
    return "".join(char.lower() if char.isalnum() else "_" for char in value).strip("_")


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _float_or_none(value: object) -> float | None:
    try:
        parsed = float(str(value))
    except TypeError, ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _format_float(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return ""
    return f"{value:.12g}"


def _log_returns(prices: Sequence[float]) -> list[float]:
    returns: list[float] = []
    for previous, current in zip(prices, prices[1:], strict=False):
        if previous > 0 and current > 0:
            returns.append(math.log(current / previous))
    return returns


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _sample_std(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / (len(values) - 1))


def _sample_covariance(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) < 2 or len(left) != len(right):
        return 0.0
    left_mean = _mean(left)
    right_mean = _mean(right)
    return sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left, right, strict=True)
    ) / (len(left) - 1)


def _pearson(left: Sequence[float], right: Sequence[float], *, covariance: float) -> float | None:
    left_std = _sample_std(left)
    right_std = _sample_std(right)
    if left_std == 0 or right_std == 0:
        return None
    return covariance / (left_std * right_std)


def _cvar(values: Sequence[float], *, alpha: float) -> float:
    sorted_values = sorted(values)
    tail_count = max(1, math.ceil(len(sorted_values) * (1 - alpha)))
    return _mean(sorted_values[:tail_count])


def _sharpe(values: Sequence[float]) -> float | None:
    std = _sample_std(values)
    if std == 0:
        return None
    return _mean(values) / std


def _sortino(values: Sequence[float]) -> float | None:
    downside = [value for value in values if value < 0]
    if not downside:
        return None
    downside_std = math.sqrt(sum(value**2 for value in downside) / len(downside))
    if downside_std == 0:
        return None
    return _mean(values) / downside_std


def _skewness(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    std = _sample_std(values)
    if std == 0:
        return 0.0
    return sum(((value - mean) / std) ** 3 for value in values) / len(values)


def _max_drawdown(prices: Sequence[float]) -> float:
    peak = prices[0]
    worst = 0.0
    for price in prices:
        peak = max(peak, price)
        if peak > 0:
            worst = min(worst, price / peak - 1)
    return worst


def _slope(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    x_mean = (len(values) - 1) / 2
    y_mean = _mean(values)
    denominator = sum((index - x_mean) ** 2 for index in range(len(values)))
    if denominator == 0:
        return 0.0
    return sum((index - x_mean) * (value - y_mean) for index, value in enumerate(values)) / denominator


def _read_selection_isins(path: Path) -> set[str]:
    return {row.get("isin", "").strip().upper() for row in _read_csv_dicts(path) if row.get("isin", "").strip()}


def _daily_returns_by_isin(
    *,
    prices_csv: Path,
    price_column: str,
    allowed_isins: set[str],
) -> dict[str, dict[str, float]]:
    prices_by_isin: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for row in _read_csv_dicts(prices_csv):
        isin = row.get("isin", "").strip().upper()
        price = _float_or_none(row.get(price_column, ""))
        date = row.get("date", "").strip()
        if isin in allowed_isins and date and price is not None and price > 0:
            prices_by_isin[isin].append((date, price))

    returns_by_isin: dict[str, dict[str, float]] = {}
    for isin, series in prices_by_isin.items():
        sorted_series = sorted(series, key=lambda item: item[0])
        returns_by_date: dict[str, float] = {}
        for (previous_date, previous_price), (current_date, current_price) in zip(
            sorted_series, sorted_series[1:], strict=False
        ):
            if previous_date and previous_price > 0 and current_price > 0:
                returns_by_date[current_date] = math.log(current_price / previous_price)
        if returns_by_date:
            returns_by_isin[isin] = returns_by_date
    return returns_by_isin
