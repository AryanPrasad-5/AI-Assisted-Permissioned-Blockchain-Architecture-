"""Causal EWMA forecaster, pure python (the WSL runner has no numpy).

It reproduces the frozen recurrence of fabric_perf.forecasting.horizon_benchmark.ewma_states exactly:
    e[0] = counts[0];  e[k] = alpha * counts[k] + (1 - alpha) * e[k-1]
and, like the frozen H=60 selection (alpha 0.005, minimum history 600 s), forecasts the next 60 s as the last state.
Counts are per-second arrival counts, which equal transactions/s for one-second bins.

Causality: predict() receives only the history handed to it. A test changes every value after the origin and requires the same
forecast. The class has no clock, no file access and no reference to the future of any trace.
"""

from __future__ import annotations

import math
from typing import Iterable

ALPHA = 0.005
HORIZON_S = 60
MIN_HISTORY_S = 600


class ForecastError(ValueError):
    pass


def ewma_last(history: Iterable[float], alpha: float = ALPHA) -> float:
    state = None
    for x in history:
        x = float(x)
        if not math.isfinite(x) or x < 0:
            raise ForecastError("history must be finite and non-negative")
        state = x if state is None else alpha * x + (1.0 - alpha) * state
    if state is None:
        raise ForecastError("empty history")
    return state


class CausalEwma:
    """ArrivalForecaster protocol (repo strategies.ArrivalForecaster): predict(history_tps, horizon_seconds) -> tx/s."""

    requires_timestamped_history = False
    model_identity = "ewma_alpha_0.005:H60:min600:causal-pure-python:v1"
    last_result = None

    def __init__(self, alpha: float = ALPHA, horizon_s: int = HORIZON_S, min_history_s: int = MIN_HISTORY_S):
        if (alpha, horizon_s, min_history_s) != (ALPHA, HORIZON_S, MIN_HISTORY_S):
            raise ForecastError("the approved protocol freezes alpha 0.005, H=60 s and a 600 s minimum history")
        self.alpha, self.horizon_s, self.min_history_s = alpha, horizon_s, min_history_s

    def predict(self, history_tps: Iterable[float], horizon_seconds: int) -> float:
        if horizon_seconds != self.horizon_s:
            raise ForecastError(f"frozen EWMA forecasts H={self.horizon_s} s only (got {horizon_seconds})")
        hist = [float(v) for v in history_tps]
        if len(hist) < self.min_history_s:
            raise ForecastError(f"frozen EWMA needs >= {self.min_history_s} s of history (got {len(hist)})")
        return ewma_last(hist, self.alpha)
