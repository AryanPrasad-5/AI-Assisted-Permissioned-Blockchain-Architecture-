"""Replay windows: UTC-second-aligned slices of a day trace, with burst-exposure statistics and a seeded selection rule.

Windows are never clipped. A window that does not fit inside the day (including its pre-roll) is an error.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .common import DAY_S

# Pools (owner-approved split): D tunes, V selects, F is opened once. Days are never joined.
POOLS = {"D": ("2026-09-21", 0, 43_200), "V": ("2026-09-21", 43_200, 86_400), "F": ("2026-10-12", 0, 86_400)}
PRE_ROLL_S = 60
DEFAULT_LENGTH_S = 600
# 1-s counts above this are outside the validated range (highest rung of P1b Tier 1, one worker, B=10 and B=40).
HIGH_THRESHOLD = 40
# 1-s counts below this are below the lowest measured rate for the {10, 40} menu (P1a measured 10 tx/s).
LOW_THRESHOLD = 10
WINDOW_SEED = 20261114  # fixed before any window statistics were computed


@dataclass(frozen=True)
class Window:
    day: str
    start_s: int  # UTC second of day at which the measured window begins
    length_s: int
    pre_roll_s: int
    offsets_ms: np.ndarray  # int64, ms relative to window start, in [0, length_s * 1000)
    pre_offsets_ms: np.ndarray  # int64, ms relative to window start, in [-pre_roll_s * 1000, 0)


def _check_sorted(day_offsets_ms: np.ndarray) -> None:
    if day_offsets_ms.size and (np.diff(day_offsets_ms) < 0).any():
        raise ValueError("day offsets must be non-decreasing")


def extract_window(day_offsets_ms: np.ndarray, day: str, start_s: int, length_s: int = DEFAULT_LENGTH_S,
                   pre_roll_s: int = PRE_ROLL_S) -> Window:
    if start_s != int(start_s) or length_s != int(length_s) or pre_roll_s != int(pre_roll_s):
        raise ValueError("start_s, length_s and pre_roll_s must be whole seconds (UTC second boundaries)")
    start_s, length_s, pre_roll_s = int(start_s), int(length_s), int(pre_roll_s)
    if length_s <= 0 or pre_roll_s < 0:
        raise ValueError("length_s must be positive and pre_roll_s non-negative")
    if start_s - pre_roll_s < 0 or start_s + length_s > DAY_S:
        raise ValueError("window plus pre-roll does not fit inside the UTC day; windows are never clipped")
    _check_sorted(day_offsets_ms)
    return _slice(day_offsets_ms, day, start_s, length_s, pre_roll_s)


def _slice(day_offsets_ms: np.ndarray, day: str, start_s: int, length_s: int, pre_roll_s: int) -> Window:
    lo = start_s * 1000
    hi = lo + length_s * 1000
    a = np.searchsorted(day_offsets_ms, lo - pre_roll_s * 1000, side="left")
    b = np.searchsorted(day_offsets_ms, lo, side="left")
    c = np.searchsorted(day_offsets_ms, hi, side="left")
    return Window(day, start_s, length_s, pre_roll_s, day_offsets_ms[b:c] - lo, day_offsets_ms[a:b] - lo)


def second_counts(offsets_ms: np.ndarray, length_s: int) -> np.ndarray:
    """Transactions per UTC second (equals tx/s) over [0, length_s)."""
    offsets_ms = np.asarray(offsets_ms, dtype=np.int64)
    if offsets_ms.size and (offsets_ms.min() < 0 or offsets_ms.max() >= length_s * 1000):
        raise ValueError("offsets fall outside the window")
    return np.bincount(offsets_ms // 1000, minlength=length_s).astype(np.int64)


def burst_stats(offsets_ms: np.ndarray, length_s: int) -> dict:
    counts = second_counts(offsets_ms, length_s)
    n = int(counts.sum())
    gaps = np.diff(np.asarray(offsets_ms, dtype=np.int64))
    high = counts > HIGH_THRESHOLD
    low = counts < LOW_THRESHOLD
    mean = counts.mean()
    return {
        "n_tx": n,
        "mean_rate": n / length_s,
        "max_1s": int(counts.max()),
        "p95_1s": float(np.quantile(counts, 0.95)),
        "dispersion_index": float(counts.var(ddof=1) / mean) if mean > 0 and length_s > 1 else float("nan"),
        "seconds_share_gt40": float(high.mean()),
        "seconds_share_ge40": float((counts >= HIGH_THRESHOLD).mean()),
        "tx_share_in_seconds_gt40": float(counts[high].sum() / n) if n else float("nan"),
        "seconds_share_lt10": float(low.mean()),
        "tx_share_in_seconds_lt10": float(counts[low].sum() / n) if n else float("nan"),
        "zero_gap_share": float((gaps == 0).mean()) if gaps.size else float("nan"),
        "gap_le5ms_share": float((gaps <= 5).mean()) if gaps.size else float("nan"),
    }


def tile_table(day_offsets_ms: np.ndarray, pool: str, length_s: int = DEFAULT_LENGTH_S,
               pre_roll_s: int = PRE_ROLL_S) -> pd.DataFrame:
    """Non-overlapping tiles of the pool that have their full pre-roll inside the day."""
    day, pool_start, pool_end = POOLS[pool]
    _check_sorted(day_offsets_ms)
    rows = []
    for start_s in range(pool_start, pool_end - length_s + 1, length_s):
        if start_s - pre_roll_s < 0:
            continue
        window = _slice(day_offsets_ms, day, start_s, length_s, pre_roll_s)
        rows.append({"pool": pool, "day": day, "start_s": start_s, "length_s": length_s, **burst_stats(window.offsets_ms, length_s)})
    return pd.DataFrame(rows)


def select_window(table: pd.DataFrame, seed: int = WINDOW_SEED) -> tuple[pd.Series, int]:
    """Seeded pick of a 'typical' tile: mean rate inside the pool's inter-quartile range and
    high-rate transaction exposure at or below the pool's 75th percentile. Returns (row, n_eligible)."""
    if table.empty:
        raise ValueError("no tiles to select from")
    q_lo, q_hi = table["mean_rate"].quantile([0.25, 0.75])
    exposure_cap = table["tx_share_in_seconds_gt40"].quantile(0.75)
    eligible = table[(table["mean_rate"] >= q_lo) & (table["mean_rate"] <= q_hi)
                     & (table["tx_share_in_seconds_gt40"] <= exposure_cap)].reset_index(drop=True)
    if eligible.empty:
        raise ValueError("selection rule left no eligible tile")
    pick = int(np.random.default_rng(seed).integers(len(eligible)))
    return eligible.iloc[pick], len(eligible)
