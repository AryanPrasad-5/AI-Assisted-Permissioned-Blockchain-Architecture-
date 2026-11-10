                                                                 

                                                                                   
                                                                      
                                                                         
   
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

H1_MIN_GAP = 15.0
H2_MAX_REPORT_GAP = 3.5
H2_MAX_WINDOW_GAP_SHARE = 0.02
H3_MIN_DRAIN_NON_DIVISOR = 1.5
H3_MAX_DRAIN_DIVISOR = 0.7
H4_MIN_STAIR_JUMP = 25.0
RATE_TOLERANCE = 0.02
FAILURE_LIMIT = 0.01
WARMUP_S = 30.0
WINDOW_END_S = 120.0
SPLIT_N = 1000


def summarise_run(tx: pd.DataFrame) -> dict:
                                                                                      
    first_create = tx.create_ms.min()
    last_create = tx.create_ms.max()
    last_finish = tx.final_ms.max()
    n = len(tx)
    success = tx[tx.status == "success"]
    latency = (success.final_ms - success.create_ms) / 1000
    relative = (success.final_ms - first_create) / 1000
    in_window = relative[(relative >= WARMUP_S) & (relative < WINDOW_END_S)]
    ordered = tx.sort_values("create_ms").head(SPLIT_N)
    return {
        "submitted": n,
        "send_rate": n / ((last_create - first_create) / 1000),
        "caliper_throughput": n / ((last_finish - first_create) / 1000),
        "drain_s": (last_finish - last_create) / 1000,
        "max_latency_s": float(latency.max()),
        "p50_latency_s": float(latency.median()),
        "p95_latency_s": float(latency.quantile(0.95)),
        "failure_rate": 1 - len(success) / n,
        "windowed_throughput": len(in_window) / (WINDOW_END_S - WARMUP_S) if relative.max() >= WINDOW_END_S * 0.95 else np.nan,
        "first_1000_completion_rate": len(ordered) / ((ordered.final_ms.max() - ordered.create_ms.min()) / 1000),
    }


def valid_runs(runs: pd.DataFrame) -> pd.DataFrame:
    achieved = (runs.send_rate / runs.rate - 1).abs() <= RATE_TOLERANCE
    return runs[achieved & (runs.failure_rate <= FAILURE_LIMIT)]


def _values(runs, mode, rate, block, column):
    return runs[(runs["mode"] == mode) & (runs.rate == rate) & (runs.block == block)][column].to_numpy()


def _median_gap(runs, mode, rate, column):
    return float(np.median(_values(runs, mode, rate, 40, column)) - np.median(_values(runs, mode, rate, 45, column)))


def evaluate(all_runs: pd.DataFrame) -> dict:
    runs = valid_runs(all_runs)
    rates = sorted(runs.rate.unique())
    per_rate = {}
    for rate in rates:
        forty = _values(runs, "txNumber", rate, 40, "caliper_throughput")
        forty_five = _values(runs, "txNumber", rate, 45, "caliper_throughput")
        gap = _median_gap(runs, "txNumber", rate, "caliper_throughput")
        separated = bool(forty.size and forty_five.size and forty.min() > forty_five.max())
        h1 = gap >= H1_MIN_GAP and separated
        p = float(stats.mannwhitneyu(forty, forty_five, alternative="two-sided", method="exact").pvalue)
        report_gap = _median_gap(runs, "txDuration", rate, "caliper_throughput")
        window_gap = _median_gap(runs, "txDuration", rate, "windowed_throughput")
        h2 = abs(report_gap) <= H2_MAX_REPORT_GAP and abs(window_gap) <= H2_MAX_WINDOW_GAP_SHARE * rate
        h3 = (np.median(_values(runs, "txNumber", rate, 45, "drain_s")) >= H3_MIN_DRAIN_NON_DIVISOR
              and np.median(_values(runs, "txNumber", rate, 40, "drain_s")) <= H3_MAX_DRAIN_DIVISOR)
        per_rate[int(rate)] = {
            "fixed_count_gap": gap, "complete_separation": separated, "exact_mann_whitney_p": p,
            "duration_report_gap": report_gap, "duration_window_gap": window_gap,
            "h1": bool(h1), "h2": bool(h2), "h3": bool(h3),
        }
    h1_all = all(r["h1"] for r in per_rate.values())
    h1_none = not any(r["h1"] for r in per_rate.values())
    full = h1_all and all(r["h2"] and r["h3"] for r in per_rate.values())
    verdict = "SUPPORTED" if full else "REJECTED_FOR_THIS_DEPLOYMENT" if h1_none else "INCONCLUSIVE"
    out = {"verdict": verdict, "per_rate": per_rate, "valid_runs": len(runs), "total_runs": len(all_runs)}
    if (runs.block == 500).any() and len(rates) == 2:
        low, high = rates
        jump = float(np.median(_values(runs, "txNumber", high, 500, "caliper_throughput"))
                     - np.median(_values(runs, "txNumber", low, 500, "caliper_throughput")))
        window = float(np.median(_values(runs, "txDuration", high, 500, "windowed_throughput"))
                       - np.median(_values(runs, "txDuration", low, 500, "windowed_throughput")))
        out["h4"] = {"fixed_count_jump": jump, "duration_window_jump": window,
                     "supported": bool(jump >= H4_MIN_STAIR_JUMP and abs(window - (high - low)) <= 0.02 * high)}
    return out
