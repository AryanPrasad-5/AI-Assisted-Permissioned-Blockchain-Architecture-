"""Reproducible, leakage-safe forecasting utilities for Flashbots arrivals.

The raw archives are treated as separate UTC daily sequences. No state is
carried across the gap between the two dates. All baselines use only values
strictly before each forecast origin.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Iterable, Sequence
import zipfile

import numpy as np
import pandas as pd


BASELINE_NAMES = (
    "persistence",
    "rolling_mean_60s",
    "ewma_alpha_0.2",
    "ewma_alpha_0.5",
    "seasonal_naive_60s",
    "seasonal_naive_300s",
    "poisson_rate_300s",
)


def forecast_baseline(
    name: str,
    history: Sequence[float],
    horizon_bins: int,
    frequency_seconds: int,
) -> np.ndarray:
    """Forecast from a strictly pre-origin history using an approved baseline.

    This is the single-origin counterpart of :func:`evaluate_baselines`.  It
    intentionally accepts only the history available at the forecast origin;
    callers cannot accidentally pass the future portion of a daily series.
    """
    if name not in BASELINE_NAMES:
        raise ValueError(f"Unknown baseline {name!r}")
    if frequency_seconds not in (1, 5, 10):
        raise ValueError("frequency_seconds must be one of 1, 5, or 10")
    if horizon_bins < 1:
        raise ValueError("horizon_bins must be positive")
    values = np.asarray(tuple(history), dtype=float)
    if values.size == 0 or not np.isfinite(values).all() or (values < 0).any():
        raise ValueError("history must contain finite non-negative arrival counts")
    prediction = _forecast(name, values, len(values), horizon_bins, frequency_seconds)
    if np.isnan(prediction).any() or not np.isfinite(prediction).all():
        raise ValueError(
            f"baseline {name!r} lacks enough history for {horizon_bins} bins"
        )
    return prediction


def _csv_entry(archive_path: Path) -> str:
    with zipfile.ZipFile(archive_path) as archive:
        entries = [x.filename for x in archive.infolist() if not x.is_dir() and x.filename.lower().endswith(".csv")]
    if len(entries) != 1:
        raise ValueError(f"Expected exactly one CSV entry in {archive_path}, found {entries}")
    return entries[0]


def build_daily_series(
    archive_path: Path,
    frequency_seconds: int,
    chunk_rows: int = 100_000,
) -> tuple[pd.DataFrame, dict]:
    """Build a complete UTC daily count series by streaming a ZIP CSV."""
    if frequency_seconds not in (1, 5, 10):
        raise ValueError("frequency_seconds must be one of 1, 5, or 10")
    entry = _csv_entry(archive_path)
    counts = None
    day = None
    rows = 0
    invalid = 0
    out_of_day = 0
    with zipfile.ZipFile(archive_path) as archive, archive.open(entry) as raw:
        for chunk in pd.read_csv(raw, chunksize=chunk_rows, encoding="utf-8-sig", low_memory=False):
            rows += len(chunk)
            values = pd.to_numeric(chunk["timestamp_ms"], errors="coerce")
            invalid += int(values.isna().sum())
            values = values.dropna()
            invalid += int((values % 1 != 0).sum())
            if values.empty:
                continue
            timestamps = values.astype("int64").to_numpy()
            chunk_day = pd.to_datetime(int(timestamps.min()), unit="ms", utc=True).date()
            if day is None:
                day = chunk_day
                counts = np.zeros(86_400 // frequency_seconds, dtype=np.int64)
            if chunk_day != day:
                raise ValueError(f"Archive {archive_path} contains more than one UTC day")
            day_start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
            offsets = timestamps - day_start_ms
            valid = (offsets >= 0) & (offsets < 86_400_000)
            out_of_day += int((~valid).sum())
            bins = (offsets[valid] // (frequency_seconds * 1000)).astype(np.int64)
            counts += np.bincount(bins, minlength=len(counts))

    if day is None or counts is None:
        raise ValueError(f"No valid timestamp_ms values found in {archive_path}")
    start = pd.Timestamp(day, tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(start, periods=len(counts), freq=f"{frequency_seconds}s"),
            "arrival_count": counts,
        }
    )
    frame["date_utc"] = str(day)
    frame["frequency_seconds"] = frequency_seconds
    audit = {
        "archive": str(archive_path),
        "csv_entry": entry,
        "date_utc": str(day),
        "source_rows": rows,
        "invalid_timestamp_values": invalid,
        "out_of_day_values": out_of_day,
        "output_bins": len(frame),
        "zero_arrival_bins": int((counts == 0).sum()),
        "total_arrivals": int(counts.sum()),
    }
    return frame, audit


def _forecast(name: str, history: np.ndarray, origin: int, horizon: int, frequency_seconds: int, precomputed: dict[str, np.ndarray] | None = None) -> np.ndarray:
    observed = history[:origin]
    if len(observed) == 0:
        raise ValueError("A forecast origin must have at least one observation")
    if name == "persistence":
        return np.repeat(observed[-1], horizon).astype(float)
    if name == "rolling_mean_60s":
        level = precomputed[name][origin - 1] if precomputed else observed[-max(1, 60 // frequency_seconds):].mean()
        return np.repeat(level, horizon)
    if name == "ewma_alpha_0.2":
        level = precomputed[name][origin - 1] if precomputed else pd.Series(observed).ewm(alpha=0.2, adjust=False).mean().iloc[-1]
        return np.repeat(level, horizon)
    if name == "ewma_alpha_0.5":
        level = precomputed[name][origin - 1] if precomputed else pd.Series(observed).ewm(alpha=0.5, adjust=False).mean().iloc[-1]
        return np.repeat(level, horizon)
    if name.startswith("seasonal_naive_"):
        lag_seconds = int(name.split("_")[-1][:-1])
        lag = lag_seconds // frequency_seconds
        if lag < horizon or origin - lag + horizon > len(history):
            return np.full(horizon, np.nan)
        return history[origin - lag:origin - lag + horizon].astype(float)
    if name == "poisson_rate_300s":
        level = precomputed[name][origin - 1] if precomputed else observed[-max(1, 300 // frequency_seconds):].mean()
        return np.repeat(level, horizon)
    raise ValueError(f"Unknown baseline {name!r}")


def _metric_accumulator() -> dict:
    return {"absolute": 0.0, "squared": 0.0, "actual": 0.0, "points": 0, "origins": 0, "zero_demand_windows": 0, "latency_ns": []}


def _finish_metrics(acc: dict) -> dict:
    if acc["points"] == 0:
        return {"mae": None, "rmse": None, "wape": None, "zero_demand_windows": 0, "forecast_origins": 0, "latency_median_ms": None, "latency_p95_ms": None}
    latency = np.asarray(acc["latency_ns"], dtype=float) / 1_000_000
    return {
        "mae": acc["absolute"] / acc["points"],
        "rmse": float(np.sqrt(acc["squared"] / acc["points"])),
        "wape": (100.0 * acc["absolute"] / acc["actual"]) if acc["actual"] else None,
        "zero_demand_windows": acc["zero_demand_windows"],
        "forecast_origins": acc["origins"],
        "latency_median_ms": float(np.median(latency)),
        "latency_p95_ms": float(np.percentile(latency, 95)),
    }


def evaluate_baselines(
    frame: pd.DataFrame,
    frequency_seconds: int,
    horizon_seconds: int,
    mode: str,
    burst_threshold: float,
    eval_start: int | None = None,
    stride: int = 1,
) -> pd.DataFrame:
    """Evaluate every baseline on identical rolling forecast origins."""
    if mode not in {"within_day", "cross_day"}:
        raise ValueError("mode must be within_day or cross_day")
    values = frame["arrival_count"].to_numpy(dtype=float)
    horizon = horizon_seconds // frequency_seconds
    if horizon < 1 or horizon_seconds % frequency_seconds:
        raise ValueError("horizon_seconds must be a positive multiple of frequency_seconds")
    warmup = max(300 // frequency_seconds, horizon)
    start = max(warmup, eval_start if eval_start is not None else warmup)
    end = len(values) - horizon
    rolling_60 = pd.Series(values).rolling(max(1, 60 // frequency_seconds), min_periods=max(1, 60 // frequency_seconds)).mean().to_numpy()
    rolling_300 = pd.Series(values).rolling(max(1, 300 // frequency_seconds), min_periods=max(1, 300 // frequency_seconds)).mean().to_numpy()
    precomputed = {
        "rolling_mean_60s": rolling_60,
        "ewma_alpha_0.2": pd.Series(values).ewm(alpha=0.2, adjust=False).mean().to_numpy(),
        "ewma_alpha_0.5": pd.Series(values).ewm(alpha=0.5, adjust=False).mean().to_numpy(),
        "poisson_rate_300s": rolling_300,
    }
    accumulators = {(name, regime): _metric_accumulator() for name in BASELINE_NAMES for regime in ("normal", "burst")}
    for origin in range(start, end + 1, stride):
        context_rate = values[max(0, origin - max(60 // frequency_seconds, 1)):origin].mean()
        regime = "burst" if context_rate >= burst_threshold else "normal"
        actual = values[origin:origin + horizon]
        for name in BASELINE_NAMES:
            t0 = time.perf_counter_ns()
            prediction = _forecast(name, values, origin, horizon, frequency_seconds, precomputed)
            elapsed = time.perf_counter_ns() - t0
            if np.isnan(prediction).any():
                continue
            acc = accumulators[(name, regime)]
            error = prediction - actual
            acc["absolute"] += float(np.abs(error).sum())
            acc["squared"] += float(np.square(error).sum())
            acc["actual"] += float(actual.sum())
            acc["points"] += int(len(actual))
            acc["origins"] += 1
            acc["zero_demand_windows"] += int(actual.sum() == 0)
            acc["latency_ns"].append(elapsed)
    rows = []
    for (name, regime), acc in accumulators.items():
        rows.append({"mode": mode, "frequency_seconds": frequency_seconds, "horizon_seconds": horizon_seconds, "baseline": name, "regime": regime, **_finish_metrics(acc)})
    return pd.DataFrame(rows)
