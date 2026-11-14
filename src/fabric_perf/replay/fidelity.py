"""Offline schedule-integrity checks and the four-rate table.

Four rates are kept in separate columns and never merged:
  observed_y    what the source trace did, per second
  forecast_yhat what the forecaster said for that second (NaN where no forecast applies)
  offered_s     what the schedule intends to submit, per second (intended, not realized)
  delivered_d   what Fabric delivered. NOT MEASURED OFFLINE: always NaN here.

These checks prove the schedule file is faithful to its source. They cannot show realized timing on Caliper,
which needs a live run (not authorized).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .common import CheckReport
from .windows import Window, burst_stats, second_counts

DELIVERED_STATUS = "NOT_MEASURED_OFFLINE"


def observed_schedule_integrity(window: Window, schedule_ms: np.ndarray) -> CheckReport:
    """Approach A must equal the source window exactly."""
    report = CheckReport(f"approach A integrity {window.day} start={window.start_s}s")
    schedule = np.asarray(schedule_ms, dtype=np.int64)
    source = window.offsets_ms
    report.add("count_conserved", len(source), len(schedule))
    same_length = len(source) == len(schedule)
    if not same_length:
        max_difference = -1  # not comparable; the count check already failed
    else:
        max_difference = int(np.abs(schedule - source).max()) if len(source) else 0
    report.add("max_abs_offset_difference_ms", 0, max_difference)
    report.add("zero_gaps_preserved", int((np.diff(source) == 0).sum()), int((np.diff(schedule) == 0).sum()))
    report.add("negative_gaps", 0, int((np.diff(schedule) < 0).sum()))
    inside = bool(len(schedule) == 0 or (schedule.min() >= 0 and schedule.max() < window.length_s * 1000))
    report.add("offsets_within_window", True, inside)
    # second_counts refuses out-of-window offsets, so only compare when they are inside.
    per_second_differences = (
        int((second_counts(schedule, window.length_s) != second_counts(source, window.length_s)).sum()) if inside else -1
    )
    report.add("per_second_counts_equal_source", 0, per_second_differences)
    return report


def generated_schedule_integrity(schedule_ms: np.ndarray, rates_per_s: np.ndarray, interval_s: float) -> CheckReport:
    """Approach B schedules: ordering, bounds, zero-rate intervals empty, volume within a Poisson band of the intensity."""
    report = CheckReport("approach B integrity")
    schedule = np.asarray(schedule_ms, dtype=np.int64)
    rates = np.asarray(rates_per_s, dtype=float)
    horizon_s = int(round(rates.size * interval_s))
    expected = float(rates.sum() * interval_s)
    report.add("negative_gaps", 0, int((np.diff(schedule) < 0).sum()))
    report.add("offsets_within_horizon", True, bool(len(schedule) == 0 or (schedule.min() >= 0 and schedule.max() < horizon_s * 1000)))
    zero_bins = np.flatnonzero(rates == 0)
    in_zero = 0
    if zero_bins.size and len(schedule):
        bin_of = np.minimum((schedule / (interval_s * 1000.0)).astype(np.int64), rates.size - 1)
        in_zero = int(np.isin(bin_of, zero_bins).sum())
    report.add("arrivals_in_zero_rate_intervals", 0, in_zero)
    report.add("volume_within_5_sigma_of_intensity", True,
               bool(abs(len(schedule) - expected) <= 5 * np.sqrt(max(expected, 1.0)) + 1))
    return report


def rate_table(window: Window, schedule_ms: np.ndarray, forecast_rates: np.ndarray | None = None,
               forecast_interval_s: int | None = None) -> pd.DataFrame:
    """Per-second observed / forecast / offered columns; delivered is NaN by design."""
    length_s = window.length_s
    forecast = np.full(length_s, np.nan)
    if forecast_rates is not None:
        if forecast_interval_s is None or len(forecast_rates) * forecast_interval_s != length_s:
            raise ValueError("forecast_rates must tile the window exactly")
        forecast = np.repeat(np.asarray(forecast_rates, dtype=float), forecast_interval_s)
    return pd.DataFrame({
        "second": np.arange(length_s),
        "observed_y": second_counts(window.offsets_ms, length_s),
        "forecast_yhat": forecast,
        "offered_s": second_counts(schedule_ms, length_s),
        "delivered_d": np.full(length_s, np.nan),
    })


def rate_summary(table: pd.DataFrame, interval_s: int | None = None) -> dict:
    """Pairwise quantities that answer different questions (kept separate)."""
    y, s = table["observed_y"].to_numpy(float), table["offered_s"].to_numpy(float)
    out = {
        "delivered_status": DELIVERED_STATUS,
        "observed_total": float(y.sum()),
        "offered_total": float(s.sum()),
        "offered_over_observed_volume": float(s.sum() / y.sum()) if y.sum() else float("nan"),
        "per_second_mae_offered_vs_observed": float(np.abs(s - y).mean()),
    }
    if table["forecast_yhat"].notna().any() and interval_s:
        yhat = table["forecast_yhat"].to_numpy(float).reshape(-1, interval_s)[:, 0]
        mean_y = y.reshape(-1, interval_s).mean(axis=1)
        out["forecast_mae_vs_interval_mean"] = float(np.abs(yhat - mean_y).mean())
        out["forecast_bias_vs_interval_mean"] = float((yhat - mean_y).mean())
    return out


def schedule_descriptors(schedule_ms: np.ndarray, length_s: int) -> dict:
    """Burst descriptors of any schedule (comparable across arms). Gaps <= 5 ms are where Caliper 0.6 replay-rate is
    reported (11-12 audit, carried, not re-read) to skip its sleep."""
    return burst_stats(np.asarray(schedule_ms, dtype=np.int64), length_s)
