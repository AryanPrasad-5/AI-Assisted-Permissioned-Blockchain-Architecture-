"""Schedule builders.

Approach A  observed_schedule: the source millisecond offsets, unchanged.
Approach B  poisson_schedule: Poisson arrivals from a piecewise-constant intensity, built by time-changing a
            shared unit-rate Poisson stream (common random numbers: arms that share a seed differ only through
            their intensity). Intensity is never floored; a zero-rate interval receives no arrivals.

A schedule is an int64 array of non-decreasing millisecond offsets from the start of the measured window.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np

from .windows import Window


def observed_schedule(window: Window) -> np.ndarray:
    """Approach A: exact source offsets (ties kept, nothing smoothed or clipped)."""
    return np.array(window.offsets_ms, dtype=np.int64, copy=True)


def unit_stream(seed: int, min_total: float) -> np.ndarray:
    """Arrival times of a unit-rate Poisson process, extending past `min_total`.

    The stream depends only on the seed (a longer request extends, never changes, a shorter one), which is what
    makes it a common-random-number stream across arms.
    """
    n = int(min_total + 10 * np.sqrt(max(min_total, 1.0)) + 1000)
    while True:
        times = np.random.default_rng(seed).standard_exponential(n).cumsum()
        if times[-1] > min_total:
            return times
        n *= 2


def poisson_schedule(rates_per_s: np.ndarray, interval_s: float, seed: int) -> np.ndarray:
    """Approach B arrivals from per-interval rates (tx/s) over consecutive intervals of `interval_s` seconds."""
    rates = np.asarray(rates_per_s, dtype=float)
    if rates.ndim != 1 or rates.size == 0 or not np.isfinite(rates).all() or (rates < 0).any():
        raise ValueError("rates must be a non-empty 1-D array of finite non-negative values")
    if interval_s <= 0:
        raise ValueError("interval_s must be positive")
    cumulative = np.concatenate([[0.0], np.cumsum(rates * interval_s)])  # Lambda at interval edges
    total = float(cumulative[-1])
    if total <= 0:
        return np.empty(0, dtype=np.int64)
    points = unit_stream(seed, total)
    points = points[points < total]
    index = np.searchsorted(cumulative, points, side="right") - 1  # cumulative[index] <= point < cumulative[index + 1]
    seconds = index * interval_s + (points - cumulative[index]) / rates[index]  # rates[index] > 0 by construction
    horizon_ms = int(round(rates.size * interval_s * 1000))
    ms = np.minimum(np.floor(seconds * 1000).astype(np.int64), horizon_ms - 1)
    return ms


def interval_rates_from_counts(offsets_ms: np.ndarray, length_s: int, interval_s: int) -> np.ndarray:
    """Per-interval mean rate (tx/s) of observed arrivals: the ex-post 'true interval mean'."""
    if length_s % interval_s:
        raise ValueError("length_s must be a multiple of interval_s")
    counts = np.bincount(np.asarray(offsets_ms, dtype=np.int64) // 1000, minlength=length_s)
    return counts.reshape(-1, interval_s).sum(axis=1) / float(interval_s)


def write_replay_rate(schedule_ms: np.ndarray, path: str | Path) -> None:
    """One integer ms offset per line (the Caliper replay-rate TEXT format used by the existing shadow replay).
    Refuses to overwrite. Offline artifact only; nothing here runs Caliper."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(path)
    schedule = np.asarray(schedule_ms, dtype=np.int64)
    if schedule.size == 0 or (schedule < 0).any() or (np.diff(schedule) < 0).any():
        raise ValueError("schedule must be non-empty, non-negative and chronological")
    path.write_text("\n".join(str(int(v)) for v in schedule), encoding="utf-8")
