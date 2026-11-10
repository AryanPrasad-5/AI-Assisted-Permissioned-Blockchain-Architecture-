"""Run leakage-safe Flashbots baseline forecasts on processed daily series."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from fabric_perf.forecasting.flashbots import evaluate_baselines


def threshold_from_development(frame: pd.DataFrame, frequency_seconds: int, development_fraction: float = 0.5) -> float:
    values = frame["arrival_count"].to_numpy(dtype=float)
    window = max(1, 60 // frequency_seconds)
    end = max(window, int(len(values) * development_fraction))
    rolling = pd.Series(values[:end]).rolling(window, min_periods=window).mean().dropna()
    return float(rolling.quantile(0.95))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--series-dir", type=Path, default=Path("data/processed/flashbots"))
    parser.add_argument("--out", type=Path, default=Path("experiments/results/flashbots_baseline_metrics.csv"))
    parser.add_argument("--stride", type=int, default=1)
    args = parser.parse_args()
    rows = []
    loaded = {}
    thresholds = {}
    for path in sorted(args.series_dir.glob("series_*.csv.gz")):
        frame = pd.read_csv(path, parse_dates=["timestamp_utc"])
        frequency = int(frame["frequency_seconds"].iloc[0])
        date = str(frame["date_utc"].iloc[0])
        loaded[(date, frequency)] = frame
        thresholds[(date, frequency)] = threshold_from_development(frame, frequency)
    for (date, frequency), frame in sorted(loaded.items()):
        threshold = thresholds[(date, frequency)]
        horizons = [1, 5, 10] if frequency == 1 else ([5, 10] if frequency == 5 else [10])
        for horizon in horizons:
            within = evaluate_baselines(frame, frequency, horizon, "within_day", threshold, eval_start=len(frame) // 2, stride=args.stride)
            within["date_utc"] = date
            within["burst_threshold"] = threshold
            rows.append(within)
            if date == "2026-10-12":
                development_threshold = thresholds.get(("2026-09-21", frequency))
                if development_threshold is None:
                    raise ValueError(f"Missing 2026-09-21 development series for {frequency}s cross-day evaluation")
                cross = evaluate_baselines(frame, frequency, horizon, "cross_day", development_threshold, eval_start=max(300 // frequency, 1), stride=args.stride)
                cross["date_utc"] = date
                cross["burst_threshold"] = development_threshold
                rows.append(cross)
    result = pd.concat(rows, ignore_index=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.out, index=False)
    metadata = {"baseline_names": ["persistence", "rolling_mean_60s", "ewma_alpha_0.2", "ewma_alpha_0.5", "seasonal_naive_60s", "seasonal_naive_300s", "poisson_rate_300s"], "timestamp_field": "timestamp_ms", "within_day_split": "first 50 percent development, second 50 percent evaluation", "cross_day_split": "2026-09-21 development threshold, 2026-10-12 evaluation", "stride": args.stride}
    args.out.with_suffix(".metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"rows": len(result), "output": str(args.out)}, indent=2))


if __name__ == "__main__":
    main()
