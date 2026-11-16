"""Point-forecast error metrics used by every Flashbots benchmark table.

Sign convention: error = forecast - observed, so a positive bias means over-forecasting.
Zero handling:
  - MAPE is undefined when the observed value is 0. It is computed over observations with y > 0 only, and the
    number of excluded points is returned alongside it (never silently dropped).
  - sMAPE uses 2|e| / (|y| + |yhat|); when both are 0 the term is defined as 0 (a perfect forecast of nothing).
  - WAPE = sum|e| / sum|y|; undefined (NaN) only if every observation is 0.
"""

from __future__ import annotations

import numpy as np


def _arrays(y, yhat) -> tuple[np.ndarray, np.ndarray]:
    y = np.asarray(y, dtype=float)
    yhat = np.asarray(yhat, dtype=float)
    if y.shape != yhat.shape:
        raise ValueError(f"shape mismatch {y.shape} vs {yhat.shape}")
    if y.size == 0:
        raise ValueError("no observations")
    if not (np.isfinite(y).all() and np.isfinite(yhat).all()):
        raise ValueError("non-finite values in y or yhat")
    return y, yhat


def mae(y, yhat) -> float:
    y, yhat = _arrays(y, yhat)
    return float(np.mean(np.abs(yhat - y)))


def rmse(y, yhat) -> float:
    y, yhat = _arrays(y, yhat)
    return float(np.sqrt(np.mean((yhat - y) ** 2)))


def bias(y, yhat) -> float:
    y, yhat = _arrays(y, yhat)
    return float(np.mean(yhat - y))


def wape(y, yhat) -> float:
    """Percent."""
    y, yhat = _arrays(y, yhat)
    denom = np.abs(y).sum()
    return float(100.0 * np.abs(yhat - y).sum() / denom) if denom > 0 else float("nan")


def smape(y, yhat) -> float:
    """Percent, range [0, 200]; 0/0 terms count as 0."""
    y, yhat = _arrays(y, yhat)
    denom = np.abs(y) + np.abs(yhat)
    terms = np.zeros_like(y)
    nz = denom > 0
    terms[nz] = 2.0 * np.abs(yhat[nz] - y[nz]) / denom[nz]
    return float(100.0 * terms.mean())


def mape(y, yhat) -> tuple[float, int]:
    """Percent over y > 0 only; returns (mape, number of zero observations excluded)."""
    y, yhat = _arrays(y, yhat)
    keep = y > 0
    excluded = int((~keep).sum())
    if not keep.any():
        return float("nan"), excluded
    return float(100.0 * np.mean(np.abs(yhat[keep] - y[keep]) / y[keep])), excluded


def all_metrics(y, yhat) -> dict:
    m, excluded = mape(y, yhat)
    return {"n": int(np.asarray(y).size), "mae": mae(y, yhat), "rmse": rmse(y, yhat), "bias": bias(y, yhat),
            "wape_pct": wape(y, yhat), "smape_pct": smape(y, yhat), "mape_pct_y_gt_0": m, "mape_zero_obs_excluded": excluded}
