"""Leakage-safe benchmark of simple forecasters of the mean arrival rate over the next H seconds (P2).

Target: y(t) = mean rate over [t, t+H), forecast at origin t from counts[0:t] only (lead L = 0).
Splits (owner-approved): D = 21 Sep 00-12Z tunes alpha and regime cut-offs; V = 21 Sep 12-24Z selects the model;
F = 12 Oct is opened exactly once, by a separate process, after the selection is frozen. Days are never joined.
Origins are non-overlapping (every H seconds) so targets never overlap.

Every forecaster is structurally causal (it reads counts[0:t] for forecast[t]); the tests prove it by prefix and
future-perturbation invariance and against an independent per-origin reference implementation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

HORIZONS = (60, 10)  # primary, secondary
MIN_HISTORY_S = 600
POOL_START = {"D": 600, "V": 43_200, "F": 600}  # first origin of each pool
POOL_END = {"D": 43_200, "V": 86_400, "F": 86_400}  # targets must end inside the pool
ALPHA_GRID = (0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5)
THRESHOLDS = (4.5, 19.5)  # tx/s where the batching law changes regime for B=10 and B=40 (tau = 2 s)
TRAIL_S = 300  # past-only regime label: trailing mean rate over the last 300 s
NEAR_TIE = 0.01  # selection: models within 1% of the best validation MAE count as tied; the simpler one wins
SIMPLICITY = ("last_1s", "persistence_interval", "expanding_mean", "rolling_mean_60", "rolling_mean_300",
              "seasonal_naive_60", "seasonal_naive_300", "ewma")
MODELS = SIMPLICITY


def _prefix(counts: np.ndarray) -> np.ndarray:
    return np.concatenate([[0.0], np.cumsum(np.asarray(counts, dtype=float))])


def lookback(model: str, t: np.ndarray, horizon: int) -> tuple[np.ndarray, np.ndarray]:
    """[start, end) of the counts each forecast may read, as functions of the origin(s) t."""
    t = np.asarray(t)
    if model == "last_1s":
        return t - 1, t
    if model == "persistence_interval":
        return t - horizon, t
    if model.startswith("rolling_mean_"):
        return t - int(model.rsplit("_", 1)[1]), t
    if model.startswith("seasonal_naive_"):
        period = int(model.rsplit("_", 1)[1])
        return t - period, t - period + horizon
    if model in ("expanding_mean", "ewma"):
        return np.zeros_like(t), t
    raise ValueError(f"unknown model {model}")


def ewma_states(counts: np.ndarray, alpha: float) -> np.ndarray:
    """e[k] = EWMA of counts[0..k] (e[0] = counts[0]). Causal by construction."""
    counts = np.asarray(counts, dtype=float)
    state = np.empty(len(counts))
    state[0] = counts[0]
    for k in range(1, len(counts)):
        state[k] = alpha * counts[k] + (1.0 - alpha) * state[k - 1]
    return state


def forecast_series(model: str, counts: np.ndarray, horizon: int, alpha: float | None = None) -> np.ndarray:
    """yhat[t] for t = 0..len(counts): the forecast of mean(counts[t:t+H]) made with counts[0:t]. NaN where history is short."""
    counts = np.asarray(counts, dtype=float)
    n = len(counts)
    cs = _prefix(counts)
    t = np.arange(n + 1)
    out = np.full(n + 1, np.nan)
    if model == "last_1s":
        ok = t >= 1
        out[ok] = counts[t[ok] - 1]
    elif model == "persistence_interval":
        ok = t >= horizon
        out[ok] = (cs[t[ok]] - cs[t[ok] - horizon]) / horizon
    elif model.startswith("rolling_mean_"):
        window = int(model.rsplit("_", 1)[1])
        ok = t >= window
        out[ok] = (cs[t[ok]] - cs[t[ok] - window]) / window
    elif model.startswith("seasonal_naive_"):
        period = int(model.rsplit("_", 1)[1])
        if period < horizon:
            raise ValueError("seasonal period must be at least the horizon (it would read the future)")
        ok = t >= period
        out[ok] = (cs[t[ok] - period + horizon] - cs[t[ok] - period]) / horizon
    elif model == "expanding_mean":
        ok = t >= 1
        out[ok] = cs[t[ok]] / t[ok]
    elif model == "ewma":
        if alpha is None:
            raise ValueError("ewma needs alpha")
        out[1:] = ewma_states(counts, alpha)  # forecast at origin t is the state after counts[t-1]
    else:
        raise ValueError(f"unknown model {model}")
    return out


def origins(pool: str, horizon: int) -> np.ndarray:
    return np.arange(POOL_START[pool], POOL_END[pool] - horizon + 1, horizon)


def targets(counts: np.ndarray, org: np.ndarray, horizon: int) -> np.ndarray:
    cs = _prefix(counts)
    return (cs[org + horizon] - cs[org]) / horizon


def trailing_rate(counts: np.ndarray, org: np.ndarray, window: int = TRAIL_S) -> np.ndarray:
    """Past-only regime variable: mean rate over [t - window, t)."""
    cs = _prefix(counts)
    return (cs[org] - cs[org - window]) / window


def regime_cutoffs(trailing_d: np.ndarray) -> tuple[float, float]:
    """Terciles of the trailing rate over D origins only (fixed before V and F are scored)."""
    low, high = np.quantile(trailing_d, [1 / 3, 2 / 3])
    return float(low), float(high)


def regime_labels(trailing: np.ndarray, cutoffs: tuple[float, float]) -> np.ndarray:
    return np.digitize(trailing, cutoffs)  # 0 calm, 1 typical, 2 busy


def lookback_table(model: str, org: np.ndarray, horizon: int) -> pd.DataFrame:
    """Audit record of what each forecast may read; asserts nothing at or after the origin is read."""
    start, end = lookback(model, org, horizon)
    table = pd.DataFrame({"origin_s": org, "lookback_start_s": start, "lookback_end_s": end,
                          "target_start_s": org, "target_end_s": org + horizon})
    if (table["lookback_end_s"] > table["origin_s"]).any() or (table["lookback_start_s"] < 0).any():
        raise AssertionError(f"{model}: lookback leaves [0, origin]")
    return table


def tune_alpha(counts_d: np.ndarray, horizon: int, grid: tuple[float, ...] = ALPHA_GRID) -> tuple[float, pd.DataFrame]:
    """Pick alpha by MAE on D origins. `counts_d` must be D's counts only (the caller passes no later data)."""
    org = origins("D", horizon)
    if len(counts_d) < POOL_END["D"]:
        raise ValueError("counts_d must cover the D pool")
    counts_d = np.asarray(counts_d)[: POOL_END["D"]]
    y = targets(counts_d, org, horizon)
    rows = []
    for alpha in grid:
        yhat = forecast_series("ewma", counts_d, horizon, alpha)[org]
        rows.append({"alpha": alpha, "mae_D": float(np.abs(yhat - y).mean())})
    table = pd.DataFrame(rows)
    return float(table.loc[table["mae_D"].idxmin(), "alpha"]), table


def score(y: np.ndarray, yhat: np.ndarray) -> dict:
    error = yhat - y
    return {"n": int(len(y)), "mae": float(np.abs(error).mean()), "rmse": float(np.sqrt((error ** 2).mean())),
            "bias": float(error.mean()), "wape": float(np.abs(error).sum() / y.sum()) if y.sum() > 0 else float("nan")}


def threshold_counts(y: np.ndarray, yhat: np.ndarray, threshold: float) -> dict:
    """Does the forecast put the interval on the same side of a law threshold as the truth?"""
    truth, call = y > threshold, yhat > threshold
    return {"threshold": threshold, "n_above": int(truth.sum()), "tp": int((truth & call).sum()), "fp": int((~truth & call).sum()),
            "fn": int((truth & ~call).sum()), "agreement": float((truth == call).mean())}


def select_model(val_mae: dict[str, float]) -> str:
    """Lowest validation MAE; models within NEAR_TIE of the best are tied and the simpler one (SIMPLICITY order) wins."""
    best = min(val_mae.values())
    tied = [m for m in SIMPLICITY if m in val_mae and val_mae[m] <= best * (1 + NEAR_TIE)]
    return tied[0]


def dm_test(loss_a: np.ndarray, loss_b: np.ndarray) -> dict:
    """Diebold-Mariano on loss differentials (a - b): Bartlett-kernel HAC variance with bandwidth floor(n^(1/3)),
    Harvey-Leybourne-Newbold small-sample correction, Student-t reference. Negative mean_diff favours a."""
    d = np.asarray(loss_a, dtype=float) - np.asarray(loss_b, dtype=float)
    n = len(d)
    mean = float(d.mean())
    if np.allclose(d, 0.0):
        return {"n": n, "mean_diff": 0.0, "stat": float("nan"), "p": 1.0, "identical": True}
    centred = d - mean
    bandwidth = int(math.floor(n ** (1 / 3)))
    variance = float(centred @ centred) / n
    for lag in range(1, bandwidth + 1):
        variance += 2.0 * (1.0 - lag / (bandwidth + 1)) * float(centred[lag:] @ centred[:-lag]) / n
    if variance <= 0:
        variance = float(centred @ centred) / n
    stat = mean / math.sqrt(variance / n) * math.sqrt((n - 1) / n)
    return {"n": n, "mean_diff": mean, "stat": float(stat), "p": float(2 * stats.t.sf(abs(stat), n - 1)), "identical": False}


def holm(p_values: list[float]) -> list[float]:
    """Holm step-down adjusted p-values, returned in the input order."""
    m = len(p_values)
    order = np.argsort(p_values)
    adjusted = np.empty(m)
    running = 0.0
    for rank, idx in enumerate(order):
        running = max(running, min(1.0, (m - rank) * p_values[idx]))
        adjusted[idx] = running
    return adjusted.tolist()


@dataclass
class PoolResult:
    pool: str
    horizon: int
    org: np.ndarray
    y: np.ndarray
    yhat: dict[str, np.ndarray]
    trailing: np.ndarray


def evaluate_pool(pool: str, counts: np.ndarray, horizon: int, alpha: float) -> PoolResult:
    """Forecast every model at the pool's origins. `counts` is the day's series; forecasts read counts[0:t] only."""
    org = origins(pool, horizon)
    if org[-1] + horizon > len(counts):
        raise ValueError("counts do not cover the pool")
    y = targets(counts, org, horizon)
    yhat = {}
    for model in MODELS:
        full = forecast_series(model, counts, horizon, alpha if model == "ewma" else None)
        lb = lookback_table(model, org, horizon)  # raises on any read at or after the origin
        values = full[org]
        if np.isnan(values).any():
            raise ValueError(f"{model}: missing history at some origins")
        assert (lb["lookback_end_s"] <= org).all()
        yhat[model] = values
    return PoolResult(pool, horizon, org, y, yhat, trailing_rate(counts, org))
