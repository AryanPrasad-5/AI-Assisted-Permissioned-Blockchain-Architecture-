"""Median-referenced dispatch-timing metrics (Phase 2C). Pure functions over dispatch-log rows; nothing is started and nothing is written.

The problem this corrects (found in Phase 2, `classify.CANNOT_ASSESS`, last item). Phase 1 `fidelity.py` measures every dispatch lag against the FIRST dispatch's lag:
    rel_first[i] = lag[i] - lag[0],   lag[i] = dispatch_ms[i] - intended_ms[i].
The logged lag carries a constant offset (Caliper's round start is set after the workload initialises: 5.5-8.5 ms in run 005, P4 SF-2) and the first dispatch is the least
representative sample of it (cold connections, first JIT, first gRPC). One slow first dispatch shifts the reference, so every later transaction looks that much early or late,
and N1, N3 and, through the anchor of the realised timeline, N4 and the burst check inherit the error. Here the reference is the MEDIAN lag of the round.

    rel[i] = lag[i] - median(lag)

Is the median technically supported as the reference? For its purpose (removing a constant clock offset): yes. It is not moved by any single outlier, by the first
transaction in particular, nor by up to half of the transactions being late. What it CANNOT do is expose a delay of up to N3's limit (500 ms) that begins right after the first dispatch and
covers 99 % or more of the later transactions. N1 is a 99th percentile, so up to 1 % of dispatches may deviate from the reference unlabelled; N3 tolerates a deviation up to its
limit; and a delay shared by 99 % or more of them IS the reference. Everything else is flagged: a shared delay covering 1 % to 99 % of dispatches (the minority deviates by the whole
delay, on whichever side the median falls), and any larger delay that leaves even one dispatch unaffected (N3 has no tolerance). The first-dispatch reference flags the 20-500 ms case
only because the first dispatch happens to precede the delay. A delay present from the very first dispatch is a constant offset to every relative-lag reference, the first-dispatch
one included; only the median lag itself, against the carried 5.5-8.5 ms band, shows it. Two consequences are kept in the open and not hidden by a threshold:
  * the first-dispatch figures are still computed and returned side by side (`vs_first`), and the reference gap |first lag - median lag| is reported;
  * `drift` reports the median lag of the first and last tenth of the round (a slow drift moves the median but not the ends).
Neither of those two gates anything. Whether a bound on the median lag itself (outside the 5.5-8.5 ms carried from run 005) should be a gate is an OWNER_DECISION_REQUIRED.

Conventions (stated, not assumed):
  * lag, rel: integer milliseconds as logged (dispatch_ms and intended_ms are integers); the median of an even count is the mean of the two middle values (numpy default).
  * p99 of |rel|: numpy.quantile, method "linear" (the default), the same call Phase 1 used, so the only thing that changed against Phase 1 is the reference.
  * "late": N3 counts |rel| > limit (two-sided, as in Phase 1: an entry dispatched far EARLY is as much a failure of the schedule as one dispatched late; replay-rate itself
    cannot dispatch more than 5 ms early). It is therefore at least as strict as "more than 500 ms late".
  * realised timeline for the per-second counts: realised[i] = dispatch_ms[i] - origin, origin = median(dispatch_ms - intended_offset_ms); equivalent to anchoring the median
    dispatch on its own intended offset. Dispatches landing before 0 are clipped into second 0, after the window into the extra bins, exactly as Phase 1 does.
"""

from __future__ import annotations

import numpy as np

REFERENCE = "median"
CARRIED_OFFSET_BAND_MS = (5.5, 8.5)    # CARRIED: run 005, P4 SF-2 (informational flag only; it gates nothing)


def _by_seq(dispatch: list[dict]) -> list[dict]:
    return sorted(dispatch, key=lambda r: r["seq"])


def lag_array(dispatch: list[dict]) -> np.ndarray:
    """dispatch_ms - intended_ms in seq order."""
    return np.array([r["dispatch_ms"] - r["intended_ms"] for r in _by_seq(dispatch)], dtype=float)


def reference_info(dispatch: list[dict]) -> dict:
    """The reference and how far the first dispatch is from it."""
    if not dispatch:
        return {}
    lag = lag_array(dispatch)
    median = float(np.median(lag))
    lo, hi = CARRIED_OFFSET_BAND_MS
    return {"n": int(lag.size), "reference": REFERENCE, "median_lag_ms": median, "first_lag_ms": float(lag[0]), "reference_gap_ms": float(abs(lag[0] - median)),
            "median_inside_carried_offset_band": bool(lo <= median <= hi), "carried_offset_band_ms": list(CARRIED_OFFSET_BAND_MS)}


def lag_metrics(dispatch: list[dict]) -> dict:
    """N1 and N2 inputs against the median reference, with the first-dispatch figures beside them."""
    if not dispatch:
        return {}
    lag = lag_array(dispatch)
    rel_m, rel_f = lag - np.median(lag), lag - lag[0]
    return {**reference_info(dispatch),
            "abs_p99_ms": float(np.quantile(np.abs(rel_m), 0.99)), "abs_max_ms": float(np.abs(rel_m).max()),
            "vs_first": {"abs_p99_ms": float(np.quantile(np.abs(rel_f), 0.99)), "abs_max_ms": float(np.abs(rel_f).max())},
            "logged_lag_consistent": bool(all(r["lag_ms"] == r["dispatch_ms"] - r["intended_ms"] for r in dispatch))}


def drift(dispatch: list[dict]) -> dict:
    """Median lag of the first and last tenth of the round (by seq). Diagnostic: a slow drift moves the median but not the two ends."""
    if len(dispatch) < 10:
        return {"available": False}
    lag = lag_array(dispatch)
    k = max(1, len(lag) // 10)
    head, tail = float(np.median(lag[:k])), float(np.median(lag[-k:]))
    return {"available": True, "tenth": k, "head_median_lag_ms": head, "tail_median_lag_ms": tail, "tail_minus_head_ms": tail - head}


def missed_schedules(dispatch: list[dict], n_scheduled: int, limit_ms: float) -> dict:
    """N3: entries never dispatched plus entries whose |rel| exceeds the limit. `never_dispatched` counts the schedule's seq values with no dispatch row."""
    seqs = {r["seq"] for r in dispatch}
    never = sorted(set(range(n_scheduled)) - seqs)
    lag = lag_array(dispatch) if dispatch else np.array([])
    rel = lag - np.median(lag) if lag.size else lag
    over = int((np.abs(rel) > limit_ms).sum()) if rel.size else 0
    return {"never_dispatched": len(never), "over_limit": over, "missed": len(never) + over, "limit_ms": limit_ms, "max_abs_rel_ms": float(np.abs(rel).max()) if rel.size else None}


def _realised_ms(dispatch: list[dict]) -> np.ndarray:
    by_seq = _by_seq(dispatch)
    origin = float(np.median([r["dispatch_ms"] - r["intended_offset_ms"] for r in by_seq]))
    return np.array([r["dispatch_ms"] - origin for r in by_seq], dtype=float)


def per_second_counts(dispatch: list[dict], scheduled: list[int], length_s: int) -> tuple[np.ndarray, np.ndarray]:
    realised = np.clip(np.floor(_realised_ms(dispatch)), 0, None).astype(np.int64)
    sched = np.asarray(scheduled, dtype=np.int64)
    bins = int(max(length_s, int(realised.max()) // 1000 + 1, int(sched.max()) // 1000 + 1))
    return np.bincount(realised // 1000, minlength=bins)[:bins], np.bincount(sched // 1000, minlength=bins)[:bins]


def count_tv(dispatch: list[dict], scheduled: list[int], length_s: int) -> dict:
    """N4: total-variation distance between scheduled and realised per-second counts, realised time anchored on the median."""
    if not dispatch or not scheduled:
        return {}
    r, s = per_second_counts(dispatch, scheduled, length_s)
    n = max(len(scheduled), 1)
    return {"tv_distance": float(0.5 * np.abs(r - s).sum() / n), "seconds_with_difference": int((r != s).sum()), "max_abs_second_difference": int(np.abs(r - s).max()),
            "n_scheduled": int(len(scheduled)), "n_realised": int(len(dispatch))}


def burst_deviation(dispatch: list[dict] | None, scheduled: list[int], length_s: int, rate_tps: int) -> dict:
    """N6 (diagnostic): per-second realised vs scheduled count over the seconds whose scheduled count is at or above `rate_tps`, realised time anchored on the median."""
    sched = np.asarray(scheduled, dtype=np.int64)
    s_counts = np.bincount(sched // 1000, minlength=length_s)[:length_s]
    burst = np.where(s_counts >= rate_tps)[0]
    out: dict = {"burst_seconds": int(burst.size), "rate_tps": rate_tps, "scheduled_peak": int(s_counts.max()) if s_counts.size else 0}
    if burst.size == 0:
        return {**out, "applicable": False}
    if not dispatch:
        return {**out, "applicable": True, "evaluated": False}
    r, _ = per_second_counts(dispatch, scheduled, length_s)
    dev = np.abs(r[burst] - s_counts[burst]) / s_counts[burst]
    return {**out, "applicable": True, "evaluated": True, "max_rel_dev": float(dev.max()), "mean_rel_dev": float(dev.mean()),
            "realised_peak_in_scheduled_burst_seconds": int(r[burst].max()), "per_second": [[int(b), int(s_counts[b]), int(r[b])] for b in burst[:50]]}
