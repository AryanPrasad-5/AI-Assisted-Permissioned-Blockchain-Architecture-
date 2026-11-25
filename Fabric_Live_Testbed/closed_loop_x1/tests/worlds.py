"""Synthetic measurement worlds for the X1 decision-logic tests (rows in the exact format x0_round_rows produces)."""
from __future__ import annotations

from Fabric_Live_Testbed.closed_loop_x1 import protocol_x1 as X

RATES = X.RUNG_RATES_TPS
NOISE = {1: -3.0, 2: 0.0, 3: 3.0}      # deterministic replicate offsets (ms)


def row(B, rate, rep, p95, *, saturated=False, lag=5.0, overflow=0, fail=0, short=0):
    sched = int(rate * 60)
    ratio = 0.8 if saturated else 1.0
    return {"run_id": f"cb{B:02d}{rep}", "B": B, "replicate": rep, "round": 1, "rate_tps": float(rate), "scheduled": sched,
            "success": sched - fail - short, "failed": fail, "pending_at_deactivate": 0, "mean_ms": p95 * 0.7, "p50_ms": p95 * 0.6,
            "p95_ms": p95, "p99_ms": p95 * 1.2, "round_wall_s": 60.0 / ratio + 0.1, "lag_p99_ms": lag, "overflow_callbacks": overflow}


def physics(rate, B, block_limit=17.0):
    """p95 = 0.9 * B/rate seconds + 24 ms (the fit to the stored X0 B=10 data); a configuration saturates when rate/B > block_limit."""
    sat = block_limit is not None and rate / B > block_limit
    return 900.0 * B / rate + 24.0 + (4000.0 if sat else 0.0), sat


def world(fn=physics, *, rates=RATES, configs=X.CONFIGS, reps=(1, 2, 3), **kw):
    rows = []
    for b in configs:
        for x in rates:
            p, sat = fn(x, b, **kw) if kw else fn(x, b)
            for r in reps:
                rows.append(row(b, x, r, p + NOISE[r], saturated=sat))
    return rows


# top-ranked windows (same shape as the frozen ranking entries)
def win(rank, start, rates, pre=None):
    return {"rank": rank, "start_s": start, "segment_rates_tps": rates, "preroll_rate_tps": pre or rates[0],
            "warmup_rates_tps": [rates[0]] * 10, "min_rate_tps": min(rates), "max_rate_tps": max(rates), "score_R_robust": 2.0 - rank / 10}


RANKED = [win(1, 20400, [13.6, 12.6, 9.3, 16.4, 25.6, 8.8, 7.1, 10.5, 12.5, 8.9]),
          win(2, 25200, [17.4, 16.8, 17.1, 15.6, 13.6, 13.8, 13.0, 9.0, 9.4, 10.7]),
          win(3, 18000, [12.5, 11.0, 7.5, 10.6, 8.3, 7.0, 7.4, 12.2, 7.0, 15.9])]
