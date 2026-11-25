"""Frozen protocol LPC-v1 and its pre-registered decision rules. Pure python, no I/O, no clock.

Nothing in this file may depend on a controller outcome from the M series. The rules that use X0 data are functions of X0 only.
Changing any constant is a new dated protocol version (a new file), never an edit after X0 has run.
"""

from __future__ import annotations

import math
import statistics

PROTOCOL_ID = "LPC-v1 (live predictive control, 2026-11-25)"

# ---- configuration menu and controllers -----------------------------------------------------------------------------------
MENU_B = (10, 40)
INITIAL_B = 10
BATCH_TIMEOUT = "2s"
FORECAST = {"model": "ewma", "alpha": 0.005, "horizon_s": 60, "min_history_s": 600, "frozen_by": "FB-FC-v1 / P2 (EWMA at H=60 s)"}
PREDICTIVE = {"controller": "repo PredictiveController, defaults unchanged", "latency_penalty_per_ms": 0.01,
              "backlog_penalty_per_tps": 1.0, "minimum_utility_improvement": 0.0, "cooldown_samples": 3,
              "max_telemetry_age_s": 5.0, "horizon_seconds": 60}
REACTIVE_RULE = {"observed": "round mean latency", "high_ms": "M10(r_c) from X0", "low_ratio": 0.8, "consecutive_samples": 2,
                 "cooldown_samples": 0, "max_telemetry_age_s": 5.0}

# ---- X0 crossover scan ----------------------------------------------------------------------------------------------------
X0_RUNG_RATES_TPS = (10, 20, 40, 60, 80, 100, 130, 160, 200)
X0_PREROLL_RATE_TPS = 20
X0_REPLICATES_PER_B = 2
X0_ORDER_SEED = 20261126
X0_HARD_TIMEOUT_S = 1500
CLIENT_LAG_P99_MS = 100.0          # the analyzer's own pre-registered flag threshold (closed_loop.analyze.DISPATCH_LAG_FLAG_P99_MS)
FAILED_FRACTION_LIMIT = 0.005
SESOI_REL = 0.10                   # practical margin: 10% of the better configuration's p95
SESOI_FLOOR_MS = 25.0              # >= 3.5x the measured p95 reproducibility coefficient (RC 6.97 ms, P1a)
CONFIRM_RUNGS = 2                  # B=40 must win by more than the SESOI at this many consecutive client-valid rungs

# ---- window selection and scaling ------------------------------------------------------------------------------------------
WINDOW = {"day": "2026-10-12", "length_s": 600, "alignment_s": 600, "warmup_min": 10, "preroll_min": 1,
          "min_segment_rate_tps": 5.0, "min_robust_ratio": 1.5, "exclude_start_s": [82800],
          "score": "R_robust = second-largest / second-smallest of the ten 60-s mean rates", "tie_break": "earliest start"}
K_MIN, K_MAX, CAP_FRAC, STRADDLE_DELTA, MIN_SEGMENTS_EACH_SIDE = 2.0, 25.0, 0.90, 0.10, 2
FALLBACK_TOP_N = 5

# ---- M series -----------------------------------------------------------------------------------------------------------------
M_ARMS = ("static10", "static40", "reactive", "predictive_live")
M_REPLICATES_TARGET, M_REPLICATES_MIN = 3, 2
M_ORDER_SEED = 20261127


def sesoi_ms(p95_a: float, p95_b: float) -> float:
    return max(SESOI_REL * min(p95_a, p95_b), SESOI_FLOOR_MS)


def classify_round(*, scheduled: int, rows: int, failed: int, lag_p99_ms: float | None, overflow: int | None) -> str:
    """CLIENT_LIMITED (the driver, not Fabric, was late) / INCOMPLETE / FAILED_TX / OK. Fabric saturation is not a validity
    problem: it shows up as latency, delivered fraction and pending transactions, with the driver still on time."""
    if rows != scheduled:
        return "INCOMPLETE"
    if lag_p99_ms is None or lag_p99_ms > CLIENT_LAG_P99_MS or (overflow or 0) > 0:
        return "CLIENT_LIMITED"
    if failed > FAILED_FRACTION_LIMIT * scheduled:
        return "FAILED_TX"
    return "OK"


def analyse_crossover(rung_table: list[dict]) -> dict:
    """rung_table: one row per (B, rate_tps) aggregated over replicates:
         B, rate_tps, p95_ms (mean over reps), p95_reps (list), mean_ms, client_valid (all reps OK), delivered_fraction
       Returns {"outcome", "r_c", "r_max", "detail"} with outcome in CROSSOVER / NO_CROSSOVER_WITHIN_CLIENT_CAPACITY / NOT_ESTABLISHED."""
    rates = sorted({r["rate_tps"] for r in rung_table})
    by = {(r["B"], r["rate_tps"]): r for r in rung_table}
    valid = [x for x in rates if all((b, x) in by and by[(b, x)]["client_valid"] for b in MENU_B)]
    r_max = None
    # client_valid = no CLIENT_LIMITED and no INCOMPLETE round in any replicate (failed transactions do not invalidate a rung)
    for x in rates:                       # highest rung such that it and every lower rung are client-valid for both B
        if x in valid:
            r_max = x
        else:
            break
    if r_max is None:
        return {"outcome": "NOT_ESTABLISHED", "r_c": None, "r_max": None, "detail": "no client-valid rung at the lowest rate"}
    usable = [x for x in rates if x <= r_max]
    verdict = {}
    for x in usable:
        a, b = by[(10, x)], by[(40, x)]
        d = a["p95_ms"] - b["p95_ms"]
        s = sesoi_ms(a["p95_ms"], b["p95_ms"])
        reps = [u - v for u, v in zip(a["p95_reps"], b["p95_reps"])]
        fa, fb = bool(a.get("failed_exceeds")), bool(b.get("failed_exceeds"))
        if fa and not fb:                 # failures are a performance outcome: the configuration that fails loses
            verdict[x] = "B40_BETTER"
        elif fb and not fa:
            verdict[x] = "B10_BETTER"
        elif d > s and all(v > 0 for v in reps):
            verdict[x] = "B40_BETTER"
        elif -d > s and all(v < 0 for v in reps):
            verdict[x] = "B10_BETTER"
        else:
            verdict[x] = "INDISTINGUISHABLE"
    r_c = None
    for i, x in enumerate(usable):
        run = usable[i:i + CONFIRM_RUNGS]
        if len(run) == CONFIRM_RUNGS and all(verdict[y] == "B40_BETTER" for y in run):
            r_c = x
            break
    low_side = any(verdict[x] == "B10_BETTER" for x in usable if r_c is None or x < r_c)
    detail = {"per_rung": verdict, "sesoi_ms": {x: sesoi_ms(by[(10, x)]["p95_ms"], by[(40, x)]["p95_ms"]) for x in usable}}
    if r_c is not None and low_side:
        return {"outcome": "CROSSOVER", "r_c": r_c, "r_max": r_max, "detail": detail}
    if not any(v == "B40_BETTER" for v in verdict.values()):
        return {"outcome": "NO_CROSSOVER_WITHIN_CLIENT_CAPACITY", "r_c": None, "r_max": r_max, "detail": detail}
    return {"outcome": "NOT_ESTABLISHED", "r_c": r_c, "r_max": r_max, "detail": detail}


def choose_k(segment_rates_tps: list[float], r_c: float, r_max: float) -> dict:
    """Pre-registered k formula (uses X0 results r_c, r_max and the window's historical rates only).
       k0 = r_c / sqrt(min(m) * max(m))   (geometric midpoint of the window maps to the crossover)
       k  = min(k0, CAP_FRAC * r_max / max(m), K_MAX)
       feasible iff k >= K_MIN and the scaled window straddles r_c: at least MIN_SEGMENTS_EACH_SIDE segments below
       r_c*(1-delta) and at least as many above r_c*(1+delta)."""
    m = [float(v) for v in segment_rates_tps]
    k0 = r_c / math.sqrt(min(m) * max(m))
    k_cap = CAP_FRAC * r_max / max(m)
    k = round(min(k0, k_cap, K_MAX), 3)
    scaled = [k * v for v in m]
    below = sum(1 for v in scaled if v <= r_c * (1 - STRADDLE_DELTA))
    above = sum(1 for v in scaled if v >= r_c * (1 + STRADDLE_DELTA))
    feasible = k >= K_MIN and below >= MIN_SEGMENTS_EACH_SIDE and above >= MIN_SEGMENTS_EACH_SIDE
    why = None if feasible else ("k below the allowed minimum" if k < K_MIN else "scaled window does not straddle the crossover")
    return {"k": k, "k0": k0, "k_cap": k_cap, "scaled_segment_rates": scaled, "segments_below": below, "segments_above": above,
            "feasible": feasible, "infeasible_reason": why}


def select_k_over_ranked_windows(ranked: list[dict], r_c: float, r_max: float) -> dict:
    """Fallback rule: try the ranked candidates in order (top FALLBACK_TOP_N); first feasible one is used."""
    tried = []
    for cand in ranked[:FALLBACK_TOP_N]:
        res = choose_k(cand["segment_rates_tps"], r_c, r_max)
        tried.append({"start_s": cand["start_s"], **{k: res[k] for k in ("k", "feasible", "infeasible_reason")}})
        if res["feasible"]:
            return {"status": "WINDOW_AND_K_SELECTED", "window": cand, "k": res["k"], "detail": res, "tried": tried}
    return {"status": "NO_FEASIBLE_WINDOW", "window": None, "k": None, "tried": tried,
            "meaning": "crossover is outside what the client-valid range can reach for every ranked window: run no adaptive "
                       "comparison; report that B=10 was not outperformed within the client-valid range"}


def reactive_params(m10_at_rc_ms: float) -> dict:
    high = float(m10_at_rc_ms)
    return {"latency_high_ms": high, "latency_low_ms": round(REACTIVE_RULE["low_ratio"] * high, 3),
            "consecutive_samples": REACTIVE_RULE["consecutive_samples"], "cooldown_samples": REACTIVE_RULE["cooldown_samples"],
            "max_telemetry_age_s": REACTIVE_RULE["max_telemetry_age_s"], "congested_B": 40,
            "rule": "high = mean latency of B=10 at the X0 crossover rate; low = 0.8 * high; decided from X0 only"}


def robust_ratio(rates: list[float]) -> float:
    s = sorted(rates)
    return s[-2] / s[1]


def mean_of(values: list[float]) -> float:
    return statistics.fmean(values)
