"""Law-implied value of information for batch-size selection (P3). Offline arithmetic on rates; no Fabric.

The law is the pre-registered batching law of scripts/e1/e1_quicklook_p1b.py (expected_regime), validated only at
CONSTANT rates, one worker, one payload, tau = 2 s, B in {10, 40}, one replicate per rung (P1b Tier 1: 15-40 tx/s;
P1a: 10 and 20 tx/s):
    W   = min((B - 1) / rate, tau)
    p95 = W + 0.025 s   count-cut  (B <  rate * tau)
          W - 0.060 s   timer-cut  (B >  rate * tau)
          W - 0.024 s   boundary   (B == rate * tau)
Everything outside rate in [10, 40] tx/s is EXTRAPOLATION and is reported separately. Applying a constant-rate law to
interval means ignores within-interval burst structure; that is a stated limitation, not a result.

Quantities are law-implied interval p95 (seconds), volume-weighted by the transactions in the interval. They are not a
pooled p95 and not a measurement.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

TAU_S = 2.0
CONSTANT_S = {"count_cut": 0.025, "timer_cut": -0.060, "boundary": -0.024}
MENU = (10, 40)
VALIDATED_RATE = (10.0, 40.0)
LAW_TOLERANCE_S = 0.040  # P1b H1: maximum observed p95 deviation from the law
SESOI_FLOOR_S = 0.01394
SESOI_FRACTION = 0.10


def law_p95_s(rate, batch: int, tau: float = TAU_S) -> np.ndarray:
    """Law-implied p95 (seconds) at a constant rate (tx/s). NaN where rate <= 0 (no transactions, nothing to delay)."""
    rate = np.asarray(rate, dtype=float)
    out = np.full(rate.shape, np.nan)
    ok = rate > 0
    r = rate[ok]
    fill = r * tau
    wait = np.minimum((batch - 1) / r, tau)
    const = np.where(batch < fill, CONSTANT_S["count_cut"], np.where(batch > fill, CONSTANT_S["timer_cut"], CONSTANT_S["boundary"]))
    out[ok] = wait + const
    return out


def _stack(rate, menu=MENU) -> np.ndarray:
    return np.vstack([law_p95_s(rate, b) for b in menu])


def choose(rate_forecast, menu=MENU) -> np.ndarray:
    """Menu index with the lowest law p95 at the forecast rate (ties and undefined forecasts -> first menu entry)."""
    stack = _stack(rate_forecast, menu)
    filled = np.where(np.isnan(stack), 0.0, stack)  # undefined forecast: every entry equal, so index 0
    return np.argmin(filled, axis=0)


def _wmean(values: np.ndarray, weights: np.ndarray) -> float:
    ok = np.isfinite(values) & (weights > 0)
    return float(np.average(values[ok], weights=weights[ok])) if ok.any() else float("nan")


def tier(rate) -> np.ndarray:
    """0 below the validated range, 1 inside [10, 40] tx/s, 2 above it."""
    rate = np.asarray(rate, dtype=float)
    return np.where(rate < VALIDATED_RATE[0], 0, np.where(rate > VALIDATED_RATE[1], 2, 1))


TIER_NAMES = {0: "below_10_extrapolated", 1: "validated_10_to_40", 2: "above_40_extrapolated"}


def summarize(y, horizon_s: float, yhat=None, menu=MENU) -> dict:
    """Law-implied policy values over intervals with realised mean rates `y` (and optional forecasts `yhat`)."""
    y = np.asarray(y, dtype=float)
    weights = y * horizon_s
    truth = _stack(y, menu)
    best_per_interval = np.nanmin(np.where(np.isnan(truth), np.inf, truth), axis=0)
    best_per_interval = np.where(np.isfinite(best_per_interval), best_per_interval, np.nan)
    static = {f"static_B{b}": _wmean(truth[i], weights) for i, b in enumerate(menu)}
    best_static_name = min(static, key=static.get)
    best_static_idx = list(static).index(best_static_name)
    oracle = _wmean(best_per_interval, weights)
    gain_vs_best_static = truth[best_static_idx] - best_per_interval
    sesoi = np.maximum(SESOI_FRACTION * best_per_interval, SESOI_FLOOR_S)
    gap = np.abs(truth[0] - truth[-1]) if len(menu) == 2 else np.full_like(y, np.nan)
    live = (weights > 0) & np.isfinite(best_per_interval)
    out = {
        "intervals": int(len(y)), "intervals_with_traffic": int(live.sum()), "volume_tx": float(weights.sum()),
        **{k: v for k, v in static.items()}, "best_static": best_static_name, "oracle_by_interval": oracle,
        "oracle_headroom_over_best_static_s": static[best_static_name] - oracle,
        "oracle_headroom_over_B40_s": static.get("static_B40", float("nan")) - oracle,
        "share_intervals_headroom_ge_sesoi": float((gain_vs_best_static[live] >= sesoi[live]).mean()) if live.any() else float("nan"),
        "share_volume_headroom_ge_sesoi": float(weights[live & (gain_vs_best_static >= sesoi)].sum() / weights[live].sum()) if live.any() else float("nan"),
        "share_volume_indeterminate_within_2x_law_tolerance": float(weights[live & (gap < 2 * LAW_TOLERANCE_S)].sum() / weights[live].sum()) if live.any() else float("nan"),
        "share_volume_oracle_chooses_B10": float(weights[live & (truth[0] <= truth[-1] + 1e-12)].sum() / weights[live].sum()) if live.any() else float("nan"),
    }
    if yhat is not None:
        yhat = np.asarray(yhat, dtype=float)
        choice = choose(yhat, menu)
        realised = truth[choice, np.arange(len(y))]
        regret = realised - best_per_interval
        out.update({
            "forecast_policy": _wmean(realised, weights),
            "forecast_regret_vs_oracle_s": _wmean(regret, weights),
            "selection_errors": int(((regret > 1e-12) & live).sum()),
            "selection_error_rate": float(((regret > 1e-12) & live).sum() / live.sum()) if live.any() else float("nan"),
            "regret_vs_best_static_s": _wmean(realised - truth[best_static_idx], weights),
            "undefined_forecasts": int((yhat <= 0).sum()),
        })
    return out


def tiered(y, horizon_s: float, yhat=None, menu=MENU) -> pd.DataFrame:
    """One row per exposure tier plus 'all'; tier shares of volume and intervals; headroom within each tier."""
    y = np.asarray(y, dtype=float)
    yh = None if yhat is None else np.asarray(yhat, dtype=float)
    tiers = tier(y)
    total_volume = float((y * horizon_s).sum())
    rows = []
    for key, name in [(None, "all"), *TIER_NAMES.items()]:
        mask = np.ones(len(y), bool) if key is None else tiers == key
        if not mask.any():
            rows.append({"tier": name, "intervals": 0, "share_intervals": 0.0, "share_volume": 0.0})
            continue
        row = summarize(y[mask], horizon_s, None if yh is None else yh[mask], menu)
        row.update({"tier": name, "share_intervals": float(mask.mean()),
                    "share_volume": float((y[mask] * horizon_s).sum() / total_volume) if total_volume else float("nan")})
        rows.append(row)
    return pd.DataFrame(rows).set_index("tier").reset_index()
