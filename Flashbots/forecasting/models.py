"""Forecasters for the FB-FC-v1 benchmark. Every forecaster is causal: the forecast at origin t reads series[0:t] only.

The simple baselines are the frozen P2 implementations (src/fabric_perf/forecasting/horizon_benchmark.py), reused by
import so both benchmarks compute them identically. This module adds the horizon-dependent model list, the
learned ridge baseline (fit on pool D only, stored as plain JSON) and the exploratory time-of-day profile.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fbcommon as fc  # noqa: E402,F401  (puts src/ on sys.path)

from fabric_perf.forecasting import horizon_benchmark as hb  # noqa: E402

SELECTABLE_ORDER = ("persistence_interval", "seasonal_naive_12", "seasonal_naive_60", "seasonal_naive_300", "rolling_mean_60",
                    "rolling_mean_300", "rolling_mean_900", "expanding_mean", "ewma", "ridge_ar")
REFERENCE = "persistence_interval"
EXPLORATORY = ("tod_profile_0921",)


def prefix(series: np.ndarray) -> np.ndarray:
    return np.concatenate([[0.0], np.cumsum(np.asarray(series, dtype=float))])


def origins(first: int, end: int, horizon: int) -> np.ndarray:
    """Non-overlapping forecast origins t with targets [t, t+H) inside [first, end)."""
    return np.arange(first, end - horizon + 1, horizon)


def targets(series: np.ndarray, org: np.ndarray, horizon: int) -> np.ndarray:
    cs = prefix(series)
    return (cs[org + horizon] - cs[org]) / horizon


def models_for_horizon(horizon: int) -> list[str]:
    """Drop variants that are undefined (seasonal period <= H would read the future) or identical to persistence."""
    out = [REFERENCE]
    out += [f"seasonal_naive_{p}" for p in (12, 60, 300) if p > horizon]
    out += [f"rolling_mean_{w}" for w in (60, 300, 900) if w != horizon]
    out += ["expanding_mean", "ewma", "ridge_ar"]
    return out


def ridge_features(series: np.ndarray, org: np.ndarray, horizon: int, windows: list[int]) -> pd.DataFrame:
    cs = prefix(series)
    org = np.asarray(org)
    cols = {f"mean_last_{w}s": (cs[org] - cs[org - w]) / w for w in windows}
    if horizon < 12:
        cols["seasonal_naive_12"] = (cs[org - 12 + horizon] - cs[org - 12]) / horizon
    frame = pd.DataFrame(cols)
    if (org - max(windows) < 0).any():
        raise ValueError("origin without enough history for the longest feature window")
    return frame


def fit_ridge(series_d: np.ndarray, org_d: np.ndarray, horizon: int, windows: list[int], lambdas: list[float],
              folds: int) -> tuple[dict, pd.DataFrame]:
    """Standardise + ridge, lambda by expanding-window time-series CV on D, refit on all D origins.
    `series_d` must contain pool-D data only; targets of the last origins must end inside it."""
    from sklearn.linear_model import Ridge
    from sklearn.model_selection import TimeSeriesSplit

    if org_d[-1] + horizon > len(series_d):
        raise ValueError("D targets exceed the D series")
    X = ridge_features(series_d, org_d, horizon, windows).to_numpy()
    y = targets(series_d, org_d, horizon)
    rows = []
    for lam in lambdas:
        errors = []
        for train, test in TimeSeriesSplit(n_splits=folds).split(X):
            mean, scale = X[train].mean(axis=0), X[train].std(axis=0)
            scale[scale == 0] = 1.0
            model = Ridge(alpha=lam).fit((X[train] - mean) / scale, y[train])
            pred = np.maximum(model.predict((X[test] - mean) / scale), 0.0)
            errors.append(np.abs(pred - y[test]).mean())
        rows.append({"lambda": lam, "cv_mae_mean": float(np.mean(errors)), "cv_mae_folds": [float(e) for e in errors]})
    table = pd.DataFrame(rows)
    best = float(table.loc[table["cv_mae_mean"].idxmin(), "lambda"])
    mean, scale = X.mean(axis=0), X.std(axis=0)
    scale[scale == 0] = 1.0
    model = Ridge(alpha=best).fit((X - mean) / scale, y)
    params = {"model": "ridge_ar", "horizon_s": horizon, "windows_s": list(windows),
              "features": list(ridge_features(series_d, org_d[:1], horizon, windows).columns),
              "lambda": best, "feature_mean": mean.tolist(), "feature_scale": scale.tolist(),
              "coef_standardised": model.coef_.tolist(), "intercept": float(model.intercept_), "clip_at_zero": True,
              "training_origins": int(len(org_d)), "training_mae_in_sample": float(np.abs(np.maximum(model.predict((X - mean) / scale), 0) - y).mean())}
    return params, table


def ridge_predict(params: dict, series: np.ndarray, org: np.ndarray, horizon: int) -> np.ndarray:
    if params["horizon_s"] != horizon:
        raise ValueError("ridge parameters were fitted for another horizon")
    X = ridge_features(series, org, horizon, params["windows_s"]).to_numpy()
    z = (X - np.asarray(params["feature_mean"])) / np.asarray(params["feature_scale"])
    pred = z @ np.asarray(params["coef_standardised"]) + params["intercept"]
    return np.maximum(pred, 0.0) if params.get("clip_at_zero", True) else pred


def tune_ewma(series_d: np.ndarray, org_d: np.ndarray, horizon: int, grid: list[float]) -> tuple[float, pd.DataFrame]:
    y = targets(series_d, org_d, horizon)
    rows = [{"alpha": a, "mae_D": float(np.abs(hb.forecast_series("ewma", series_d, horizon, a)[org_d] - y).mean())} for a in grid]
    table = pd.DataFrame(rows)
    return float(table.loc[table["mae_D"].idxmin(), "alpha"]), table


def forecast(model: str, series: np.ndarray, org: np.ndarray, horizon: int, alpha: float | None = None,
             ridge: dict | None = None) -> np.ndarray:
    if model == "ridge_ar":
        return ridge_predict(ridge, series, org, horizon)
    values = hb.forecast_series(model, series, horizon, alpha if model == "ewma" else None)[org]
    if np.isnan(values).any():
        raise ValueError(f"{model}: insufficient history at some origins")
    return values


def tod_profile(reference_day: np.ndarray, org: np.ndarray, horizon: int) -> np.ndarray:
    """Same-UTC-time-of-day mean of an EARLIER date's series (exploratory; never applied within one day)."""
    return targets(reference_day, org, horizon)


def select(val_mae: dict[str, float], near_tie: float) -> str:
    best = min(val_mae.values())
    return next(m for m in SELECTABLE_ORDER if m in val_mae and val_mae[m] <= best * (1 + near_tie))


def trailing_rate(arrivals: np.ndarray, org: np.ndarray, window: int = 300) -> np.ndarray:
    cs = prefix(arrivals)
    return (cs[org] - cs[org - window]) / window
