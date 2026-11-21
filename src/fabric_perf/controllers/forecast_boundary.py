"""Descriptive boundary statistics for STORED workload forecasts (task A0h, 2026-11-21).

How often does a stored 60 s arrival-rate forecast fall outside the HFBTP performance model's reported workload range
(10 to 200 tx/s)? This module only classifies and counts values it is given. It computes no forecast, loads no model and reads no
file; the caller supplies the stored values and says which dataset and contract they belong to.

Strict boundary semantics (the same comparison ``GuardedPerformancePredictor`` applies to a request):

* ``forecast < 10``            -> ``below_grid``
* ``10 <= forecast <= 200``    -> ``inside_grid`` (10 and 200 are inside)
* ``forecast > 200``           -> ``above_grid``
* missing, blank, non-numeric, NaN or infinite -> ``invalid`` (never counted as inside, never repaired)

A below-grid count is a PROXY for how often the predictive controller would abstain with ``outside_training_grid``. It is not a
measured controller abstention rate: that needs the exact Stage 3 decision path run over the same frames.
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, Sequence

GRID_LOW_TPS = 10.0
GRID_HIGH_TPS = 200.0
BELOW, INSIDE, ABOVE, INVALID = "below_grid", "inside_grid", "above_grid", "invalid"


def classify_forecast(value: Any, *, low: float = GRID_LOW_TPS, high: float = GRID_HIGH_TPS) -> str:
    """Classify ONE stored forecast value. Booleans, text that is not a finite number, None, NaN and infinities are ``invalid``."""
    if value is None or isinstance(value, bool):
        return INVALID
    if isinstance(value, str):
        if not value.strip():
            return INVALID
        try:
            value = float(value)
        except ValueError:
            return INVALID
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return INVALID
    if value < low:
        return BELOW
    if value > high:
        return ABOVE
    return INSIDE


def _runs(flags: Sequence[bool]) -> tuple[int, int]:
    """(number of maximal runs of True, length of the longest run)."""
    runs = longest = current = 0
    for flag in flags:
        if flag:
            current += 1
            if current == 1:
                runs += 1
            longest = max(longest, current)
        else:
            current = 0
    return runs, longest


def summarize_forecasts(values: Iterable[Any], *, origins_s: Sequence[Any] | None = None, day_start_offset_s: int = 0,
                        low: float = GRID_LOW_TPS, high: float = GRID_HIGH_TPS) -> dict[str, Any]:
    """Counts, percentages and the distribution near the lower boundary for one eligible segment.

    ``values`` are the stored forecasts in stored order. ``origins_s`` (seconds from the UTC day start, one per value) enables the
    per-UTC-hour breakdown of below-grid forecasts. Percentages are of ``valid`` forecasts (finite, non-negative numbers); the
    invalid count is reported separately and is never in a denominator or a numerator.
    """
    items = list(values)
    if origins_s is not None and len(origins_s) != len(items):
        raise ValueError("origins_s and values differ in length")
    classes = [classify_forecast(v, low=low, high=high) for v in items]
    below, inside, above, invalid = (classes.count(c) for c in (BELOW, INSIDE, ABOVE, INVALID))
    valid = below + inside + above
    numbers = sorted(float(v) for v, c in zip(items, classes) if c != INVALID)
    pct = (lambda n: None if valid == 0 else 100.0 * n / valid)

    def quantile(q: float) -> float | None:
        if not numbers:
            return None
        position = q * (len(numbers) - 1)
        lower = math.floor(position)
        upper = min(lower + 1, len(numbers) - 1)
        return numbers[lower] + (numbers[upper] - numbers[lower]) * (position - lower)

    runs, longest = _runs([c == BELOW for c in classes])
    result: dict[str, Any] = {
        "eligible_forecasts": len(items), "valid": valid, "invalid_or_missing": invalid,
        "below_grid": below, "below_grid_pct_of_valid": pct(below),
        "inside_grid": inside, "inside_grid_pct_of_valid": pct(inside),
        "above_grid": above, "above_grid_pct_of_valid": pct(above),
        "boundary": {"low_tps": low, "high_tps": high, "rule": "below: <low; inside: low<=x<=high; above: >high"},
        "distribution_tps": {"min": numbers[0] if numbers else None, "p05": quantile(0.05), "p25": quantile(0.25),
                             "median": quantile(0.5), "p75": quantile(0.75), "p95": quantile(0.95),
                             "max": numbers[-1] if numbers else None},
        "near_lower_boundary": {"below_by_at_most_1_tps": sum(1 for x in numbers if low - 1.0 <= x < low),
                                "inside_by_at_most_1_tps": sum(1 for x in numbers if low <= x <= low + 1.0),
                                "exactly_at_low": sum(1 for x in numbers if x == low),
                                "exactly_at_high": sum(1 for x in numbers if x == high)},
        "below_grid_runs": runs, "longest_below_grid_run_forecasts": longest,
    }
    if origins_s is not None:
        hours: dict[int, list[int]] = {}
        for origin, klass in zip(origins_s, classes):
            if klass == INVALID:
                continue
            hour = int((float(origin) + day_start_offset_s) // 3600)
            bucket = hours.setdefault(hour, [0, 0])
            bucket[0] += 1
            bucket[1] += klass == BELOW
        result["by_utc_hour"] = [{"utc_hour": h, "valid_forecasts": n, "below_grid": b} for h, (n, b) in sorted(hours.items())]
        result["utc_hours_with_below_grid"] = [h for h, (_, b) in sorted(hours.items()) if b]
    return result


def eligibility_problems(header: Sequence[str], rows: Sequence[Mapping[str, Any]], *, forecast_column: str, horizon_s: int,
                         min_origin_s: int) -> list[str]:
    """Why a stored table is NOT eligible for the Stage 3 forecast contract (empty list: eligible).

    Contract: the column is the frozen EWMA's forecast; every row's target window is exactly ``horizon_s`` seconds long; every origin
    leaves at least ``min_origin_s`` seconds of history; origins strictly increase.
    """
    problems = []
    for column in ("origin_s", "target_end_s", forecast_column):
        if column not in header:
            problems.append(f"column {column!r} is absent")
    if problems:
        return problems
    previous = None
    for number, row in enumerate(rows, 1):
        try:
            origin, end = float(row["origin_s"]), float(row["target_end_s"])
        except (TypeError, ValueError):
            problems.append(f"row {number}: origin_s/target_end_s are not numbers")
            break
        if end - origin != horizon_s:
            problems.append(f"row {number}: target window is {end - origin} s, not {horizon_s} s")
            break
        if origin < min_origin_s:
            problems.append(f"row {number}: origin {origin} s leaves less than {min_origin_s} s of history")
            break
        if previous is not None and origin <= previous:
            problems.append(f"row {number}: origins do not strictly increase")
            break
        previous = origin
    return problems
