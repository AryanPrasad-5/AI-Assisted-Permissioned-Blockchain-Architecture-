"""Protocol M2-v1: frozen constants and the pre-registered design rules. Pure python, no I/O, no clock, no Fabric.

Nothing here depends on an M2 outcome. Every derived quantity (surface, reactive thresholds, window, k) is a deterministic function of the X1
primary decision file (cb01-cb06) and the LPC-v1 window ranking, both of which exist before any M2 run. Changing a constant is a new dated
protocol version (a new file), never an edit after M2 has run.

The question is a side-by-side comparison, not a test that Predictive wins. A result in which all three controllers hold B=2 and perform alike
is a completed comparison.
"""

from __future__ import annotations

import math
import statistics as st

from Fabric_Live_Testbed.closed_loop_v2 import protocol as V2P          # unchanged constants of LPC-v1 (forecast, predictive, SESOI, window)

PROTOCOL_ID = "M2-v1 (live three-controller comparison, 2026-11-25)"

# ---- configuration menu, arms, initial conditions ---------------------------------------------------------------------------
MENU_B = (2, 5, 10)                    # the X1 menu: every member has a measured surface; actuator range policy ACT-RANGE-M2-v1
BATCH_TIMEOUT = "2s"
PRIMARY_ARMS = ("static_b2", "reactive", "predictive_live")
INTERPRETIVE_ARMS = ("static_b10",)    # bridges cl01-cl12 (their Static was B=10); analysed separately, never part of the primary ranking
ARMS = PRIMARY_ARMS + INTERPRETIVE_ARMS
ARM_INITIAL_B = {"static_b2": 2, "reactive": 2, "predictive_live": 2, "static_b10": 10}   # equivalent initial condition for the 3 primary arms
ARM_RUNNER_NAME = {"static_b2": "static", "static_b10": "static", "reactive": "reactive", "predictive_live": "predictive_live"}
REPLICATES_TARGET, REPLICATES_MIN = 3, 2
ORDER_SEED = 20261128
RUN_PREFIX = "cc"                      # cc01..cc12, channels e1-cc01..e1-cc12
ACT_RUN_ID = "ca01"                    # ACTUATION_CHECK run (scripted, excluded from every comparison), channel e1-ca01
ACT_SCHEDULE_B = (5, 10, 5, 2, 2, 2)   # 2->5, 5->10, 10->5, 5->2 : every menu value is a target, both directions
ACT_RATE_TPS = 10.0
HARD_TIMEOUT_S = 1200

# ---- forecaster and predictive controller: LPC-v1 constants, unchanged ----------------------------------------------------
FORECAST = V2P.FORECAST
PREDICTIVE = V2P.PREDICTIVE

# ---- reactive rule: repo ReactiveController (threshold + hysteresis on round mean latency), thresholds from X1 only ----------
REACTIVE = {"controller": "repo ReactiveController (latency threshold with hysteresis), unchanged", "default_B": 2, "congested_B": 5,
            "high_factor": 2.0, "low_ratio": 0.8, "consecutive_samples": 2, "cooldown_samples": 0, "max_telemetry_age_s": 5.0,
            "rule": "congested when the finished round's mean latency >= 2.0 x the highest mean latency B=2 showed in any healthy X1 cell "
                    "(i.e. only degradation worse than anything seen in the healthy range); step to B=5 (2.5x the block rate headroom); "
                    "return to B=2 when the mean falls to 0.8 x the high threshold, 2 consecutive samples, no cooldown"}

# ---- validity, effect sizes, "meaningful difference" ---------------------------------------------------------------------
CLIENT_LAG_P99_MS = V2P.CLIENT_LAG_P99_MS
FAILED_FRACTION_LIMIT = V2P.FAILED_FRACTION_LIMIT
SESOI_REL = V2P.SESOI_REL              # latency: 10% of the better arm's value
SESOI_FLOOR_MS = V2P.SESOI_FLOOR_MS    # and at least 25 ms (>= 3.5 x the measured p95 reproducibility coefficient 6.97 ms)
THROUGHPUT_SESOI_REL = 0.02            # committed tx/s: 2% of the better arm's value
FAILURE_SESOI_ABS = V2P.FAILED_FRACTION_LIMIT    # failure rate: half a percentage point
MEANINGFUL_RULE = ("a difference is MEANINGFUL only if |difference of arm means| > SESOI AND every comparable run of the worse arm is "
                   "worse than every comparable run of the better arm (complete separation); otherwise NOT_DISTINGUISHABLE; "
                   "when every cross-arm pair of runs differs by less than the SESOI the arms are EQUIVALENT_WITHIN_SESOI. "
                   "With n=3 per arm no p-value below 0.10 exists, so no significance claim is made.")

# ---- window and scaling (same rule family as LPC-v1; outcome-blind) -----------------------------------------------------------
WINDOW = V2P.WINDOW
K_MIN, K_MAX, CAP_FRAC = 1.0, V2P.K_MAX, V2P.CAP_FRAC
FALLBACK_TOP_N = V2P.FALLBACK_TOP_N
SURFACE_MIN_RATE_TPS = 5.0             # the lowest X1 rung: scaled segments must stay inside the measured surface


def sesoi_latency_ms(a: float, b: float) -> float:
    return max(SESOI_REL * min(a, b), SESOI_FLOOR_MS)


# ------------------------------------------------------------------------------------------------------------------ X1 -> design
def usable_cells(rung_table: list[dict], menu=MENU_B) -> list[dict]:
    """X1 cells that are valid (>= 2 clean replicates) and unsaturated, for menu members only."""
    return [c for c in rung_table if int(c["B"]) in menu and c.get("cell_valid") and not c.get("saturated") and c.get("mean_ms") is not None]


def surface_points_from_x1(rung_table: list[dict], r_max: float, menu=MENU_B) -> list[dict]:
    pts = []
    for c in usable_cells(rung_table, menu):
        if float(c["rate_tps"]) <= r_max:
            pts.append({"B": int(c["B"]), "rate_tps": float(c["rate_tps"]), "mean_ms": float(c["mean_ms"]), "p95_ms": float(c["p95_ms"]),
                        "delivered_fraction": min(1.0, float(c["delivered_ratio"])), "client_valid": True})
    pts.sort(key=lambda p: (p["B"], p["rate_tps"]))
    for b in menu:
        if sum(1 for p in pts if p["B"] == b) < 2:
            raise ValueError(f"X1 gives fewer than 2 usable cells for B={b}: the local surface cannot be built")
    return pts


def reactive_params_from_x1(rung_table: list[dict], r_max: float) -> dict:
    """Thresholds from X1 only. high = high_factor x the highest mean latency of the default configuration over its healthy X1 cells."""
    healthy = [float(c["mean_ms"]) for c in usable_cells(rung_table, (REACTIVE["default_B"],)) if float(c["rate_tps"]) <= r_max]
    if not healthy:
        raise ValueError("no healthy X1 cell for the default configuration")
    high = REACTIVE["high_factor"] * max(healthy)
    return {"latency_high_ms": round(high, 3), "latency_low_ms": round(REACTIVE["low_ratio"] * high, 3),
            "consecutive_samples": REACTIVE["consecutive_samples"], "cooldown_samples": REACTIVE["cooldown_samples"],
            "max_telemetry_age_s": REACTIVE["max_telemetry_age_s"], "congested_B": REACTIVE["congested_B"],
            "default_B": REACTIVE["default_B"], "max_healthy_default_mean_ms": max(healthy), "rule": REACTIVE["rule"]}


def choose_k(segment_rates_tps: list[float], r_max: float) -> dict:
    """k = min(K_MAX, CAP_FRAC * r_max / max(m)): the largest scale that keeps every scaled segment inside the client-valid range
    (90% of the highest rung that was valid for every menu member in X1). Feasible iff k >= K_MIN and every scaled segment is at or above
    the lowest measured surface rate (so no segment is outside the local surface)."""
    m = [float(v) for v in segment_rates_tps]
    k_cap = CAP_FRAC * r_max / max(m)
    k = round(min(K_MAX, k_cap), 3)
    scaled = [k * v for v in m]
    ok_low = min(scaled) >= SURFACE_MIN_RATE_TPS
    feasible = k >= K_MIN and ok_low
    why = None if feasible else ("k below the allowed minimum" if k < K_MIN else "a scaled segment falls below the lowest measured surface rate")
    return {"k": k, "k_cap": k_cap, "scaled_segment_rates_tps": scaled, "feasible": feasible, "infeasible_reason": why,
            "scaled_min_tps": min(scaled), "scaled_max_tps": max(scaled)}


def select_window(ranked: list[dict], r_max: float) -> dict:
    """First of the top FALLBACK_TOP_N ranked windows (ranking from historical arrivals only) with a feasible k. Deterministic."""
    tried = []
    for cand in ranked[:FALLBACK_TOP_N]:
        res = choose_k(cand["segment_rates_tps"], r_max)
        tried.append({"start_s": cand["start_s"], "rank": cand.get("rank"), "k": res["k"], "feasible": res["feasible"],
                      "infeasible_reason": res["infeasible_reason"]})
        if res["feasible"]:
            return {"status": "WINDOW_AND_K_SELECTED", "window": cand, "k": res["k"], "detail": res, "tried": tried}
    return {"status": "NO_FEASIBLE_WINDOW", "window": None, "k": None, "tried": tried}


def design_from_x1(x1_decision: dict, ranked: list[dict], *, require_primary: bool = True) -> dict:
    """Everything the M2 stage needs, derived from X1 (primary set) and the ranking. Raises ValueError if X1 cannot support the design."""
    runs = list(x1_decision.get("inputs", {}).get("runs_with_rows", []))
    if require_primary and runs != [f"cb{i:02d}" for i in range(1, 7)]:
        raise ValueError(f"M2 is derived from the X1 PRIMARY set cb01-cb06 only (decision file covers {runs})")
    r_max = x1_decision.get("rungs", {}).get("r_max")
    if r_max is None:
        raise ValueError("X1 has no client-valid rung (r_max is null)")
    table = x1_decision["rung_table"]
    surface = surface_points_from_x1(table, float(r_max))
    reactive = reactive_params_from_x1(table, float(r_max))
    sel = select_window(ranked, float(r_max))
    if sel["status"] != "WINDOW_AND_K_SELECTED":
        raise ValueError(f"no ranked window is feasible for r_max={r_max}: {sel['tried']}")
    return {"r_max_tps": float(r_max), "x1_status": x1_decision.get("status"), "surface_points": surface, "reactive": reactive,
            "window_selection": sel, "window": sel["window"], "k": sel["k"]}


def protocol_doc() -> dict:
    """Machine-readable twin of the protocol constants (hashed into the freeze)."""
    return {"protocol": PROTOCOL_ID, "menu_B": list(MENU_B), "batch_timeout": BATCH_TIMEOUT, "arms": list(ARMS), "primary_arms": list(PRIMARY_ARMS),
            "interpretive_arms": list(INTERPRETIVE_ARMS), "arm_initial_B": ARM_INITIAL_B, "replicates_target": REPLICATES_TARGET,
            "replicates_min": REPLICATES_MIN, "order_seed": ORDER_SEED, "run_prefix": RUN_PREFIX, "act_run_id": ACT_RUN_ID,
            "act_schedule_B": list(ACT_SCHEDULE_B), "forecast": FORECAST, "predictive": PREDICTIVE, "reactive": REACTIVE,
            "sesoi": {"latency_rel": SESOI_REL, "latency_floor_ms": SESOI_FLOOR_MS, "throughput_rel": THROUGHPUT_SESOI_REL,
                      "failure_rate_abs": FAILURE_SESOI_ABS, "meaningful_rule": MEANINGFUL_RULE},
            "validity": {"client_lag_p99_ms": CLIENT_LAG_P99_MS, "failed_fraction_limit": FAILED_FRACTION_LIMIT},
            "window_rule": {"ranking": "LPC-v1 historical-arrival ranking, top-5, first feasible", "k": "min(K_MAX, 0.9 * r_max / max segment rate)",
                            "k_min": K_MIN, "k_max": K_MAX, "cap_frac": CAP_FRAC, "surface_min_rate_tps": SURFACE_MIN_RATE_TPS,
                            "window_constants": WINDOW},
            "design_inputs": "X1 primary decision cb01-cb06 (surface, reactive threshold, r_max); LPC-v1 ranking of the 2026-10-12 arrivals",
            "not_tuned_on_m2": "no threshold, forecast parameter, k, window or menu is changed after any M2 run"}


# ------------------------------------------------------------------------------------------------------------------ comparison rules
def mean(values: list[float]) -> float:
    return st.fmean(values)


def sd(values: list[float]) -> float | None:
    return st.stdev(values) if len(values) > 1 else None


def compare_values(a: list[float], b: list[float], sesoi: float, *, lower_is_better: bool) -> dict:
    """a, b: one value per comparable run of each arm. Returns the verdict under MEANINGFUL_RULE (a is the arm named first)."""
    if not a or not b:
        return {"verdict": "NOT_ASSESSABLE", "reason": "an arm has no comparable run"}
    d = mean(a) - mean(b)
    pair_diffs = [x - y for x in a for y in b]
    worse_is_a = (d > 0) if lower_is_better else (d < 0)
    if abs(d) > sesoi:
        w, bt = (a, b) if worse_is_a else (b, a)
        complete = (min(w) > max(bt)) if lower_is_better else (max(w) < min(bt))
        # for lower-is-better the worse arm has the larger values; for higher-is-better the worse arm has the smaller values
        verdict = ("A_WORSE" if worse_is_a else "B_WORSE") if complete else "NOT_DISTINGUISHABLE"
        if verdict != "NOT_DISTINGUISHABLE":
            verdict = "MEANINGFUL_" + verdict
    elif all(abs(p) < sesoi for p in pair_diffs):
        verdict = "EQUIVALENT_WITHIN_SESOI"
    else:
        verdict = "NOT_DISTINGUISHABLE"
    return {"verdict": verdict, "difference_of_means": d, "sesoi": sesoi, "min_pair_diff": min(pair_diffs), "max_pair_diff": max(pair_diffs),
            "n_a": len(a), "n_b": len(b), "exact_permutation_p_two_sided": exact_permutation_p(a, b)}


def exact_permutation_p(a: list[float], b: list[float]) -> float | None:
    """Exact two-sided permutation test on the difference of means (descriptive; with 3 v 3 the smallest possible p is 0.10)."""
    from itertools import combinations
    n = len(a) + len(b)
    if len(a) < 2 or len(b) < 2 or n > 16:
        return None
    pool = list(a) + list(b)
    obs = abs(mean(a) - mean(b))
    total = hits = 0
    for idx in combinations(range(n), len(a)):
        ga = [pool[i] for i in idx]
        gb = [pool[i] for i in range(n) if i not in idx]
        total += 1
        hits += abs(mean(ga) - mean(gb)) >= obs - 1e-12
    return hits / total


def finite(x) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)
