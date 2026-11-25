"""Protocol X1-v1 and its pre-registered decision rules. Pure python, no I/O, no clock, no Fabric.

Nothing in here may depend on a controller outcome. Every rule is a deterministic function of the X1 round rows (and the frozen
LPC-v1 window ranking). Changing any constant is a new dated protocol version, never an edit after X1 has run.

Row format (one dict per measured round; built by closed_loop_v2.analysis_v2.x0_round_rows from the unchanged analyzer output):
  run_id, B, replicate, round, rate_tps, scheduled, success, failed, pending_at_deactivate, mean_ms, p50_ms, p95_ms, p99_ms,
  round_wall_s, lag_p99_ms, overflow_callbacks
"""

from __future__ import annotations

import math
import statistics as st

PROTOCOL_ID = "X1-v1 (block-size decision headroom, exploratory follow-up to X0, 2026-11-25)"
LPC_PROTOCOL_SHA256 = "9b0bfa74bc6a1ae1525372932d3e65e93652979ae1957ec6d9570ed06f2be956"   # pins the window ranking X1 reuses

# ---- design ---------------------------------------------------------------------------------------------------------------
CONFIGS = (2, 5, 10)                    # MaxMessageCount candidates; B=10 is the X0 baseline
BASELINE_B = 10
BATCH_TIMEOUT = "2s"
RUNG_RATES_TPS = (5, 10, 15, 20, 25, 30, 35, 40, 50, 60)
PREROLL_RATE_TPS = 20
REPLICATES_PER_B = 3
MIN_VALID_REPS = 2                       # a (config, rung) cell needs this many non-client-limited replicates
ORDER_SEED = 20261125
HARD_TIMEOUT_S = 1500
RUN_PREFIX = "cb"                        # runs cb01..cb09, channels e1-cb01..e1-cb09

# ---- validity and saturation (the driver being late is NOT Fabric saturation) ---------------------------------------------
CLIENT_LAG_P99_MS = 100.0                # the analyzer's pre-registered dispatch-lag flag (same as X0)
FAILED_FRACTION_LIMIT = 0.005
DELIVERED_RATIO_MIN = 0.95               # committed tx/s over offered tx/s (success / round wall)/(scheduled / 60 s)

# ---- effect size ----------------------------------------------------------------------------------------------------------
SESOI_REL = 0.10                         # same practical margin as X0: 10% of the better configuration's p95
SESOI_FLOOR_MS = 25.0                    # >= 3.5 x the measured p95 reproducibility coefficient (RC 6.97 ms, P1a)
CONFIRM_RUNGS = 2                        # a configuration is "decisive" at this many consecutive client-valid rungs to form a regime

# ---- headroom gate (live comparison only if ALL hold on the structurally selected window) ----------------------------------
HEADROOM_ORACLE_MIN = 0.10               # oracle (perfect-foresight, no switching cost) p95 gain over the tuned static configuration
MIN_ABS_GAIN_MS = 25.0                   # and the absolute gain must exceed the SESOI floor
MIN_GAIN_SEGMENTS = 2                    # the gain must come from at least this many segments (not one spike); persistence is a DIAGNOSTIC only
MIN_REGIME_SHARE = 0.20                  # a menu member must be oracle-best on at least this share of the window's segments
MIN_MENU_SIZE = 2
K_MIN, K_MAX, CAP_FRAC, STRADDLE_DELTA, MIN_SEGMENTS_EACH_SIDE = 1.0, 25.0, 0.90, 0.10, 2
FALLBACK_TOP_N = 5

OK, CLIENT_LIMITED, INCOMPLETE, SATURATED = "OK", "CLIENT_LIMITED", "INCOMPLETE", "FABRIC_SATURATED"


def sesoi_ms(p95_a: float, p95_b: float) -> float:
    return max(SESOI_REL * min(p95_a, p95_b), SESOI_FLOOR_MS)


def classify_round(row: dict) -> str:
    """CLIENT_LIMITED / INCOMPLETE are validity problems (excluded). FABRIC_SATURATED is a result (driver on time, Fabric behind)."""
    scheduled = int(row["scheduled"])
    if int(row["success"]) + int(row["failed"]) != scheduled:
        return INCOMPLETE
    lag = row.get("lag_p99_ms")
    if lag is None or lag > CLIENT_LAG_P99_MS or (row.get("overflow_callbacks") or 0) > 0:
        return CLIENT_LIMITED
    if int(row["failed"]) > FAILED_FRACTION_LIMIT * scheduled:
        return SATURATED
    wall = float(row["round_wall_s"])
    if wall <= 0 or (int(row["success"]) / wall) / (scheduled / 60.0) < DELIVERED_RATIO_MIN:
        return SATURATED
    return OK


# ------------------------------------------------------------------------------------------------------------------ rung table
def build_rung_table(rows: list[dict], min_valid_reps: int = MIN_VALID_REPS) -> list[dict]:
    out = []
    for b, rate in sorted({(r["B"], r["rate_tps"]) for r in rows}):
        reps = sorted((r for r in rows if r["B"] == b and r["rate_tps"] == rate), key=lambda r: r["replicate"])
        classes = [classify_round(r) for r in reps]
        valid = [(r, c) for r, c in zip(reps, classes) if c not in (CLIENT_LIMITED, INCOMPLETE)]
        n_valid, n_sat = len(valid), sum(1 for _, c in valid if c == SATURATED)
        cell = {"B": b, "rate_tps": rate, "n_reps": len(reps), "n_valid": n_valid, "classes": classes,
                "cell_valid": n_valid >= min_valid_reps, "any_client_limited": any(c in (CLIENT_LIMITED, INCOMPLETE) for c in classes)}
        if n_valid:
            cell.update({"p95_reps": [r["p95_ms"] for r, _ in valid], "p95_ms": st.fmean(r["p95_ms"] for r, _ in valid),
                         "mean_ms": st.fmean(r["mean_ms"] for r, _ in valid),
                         "delivered_ratio": st.fmean((r["success"] / r["round_wall_s"]) / (r["scheduled"] / 60.0) for r, _ in valid),
                         "saturated": 2 * n_sat >= n_valid, "n_saturated": n_sat,
                         "pending_max": max((r.get("pending_at_deactivate") or 0) for r, _ in valid)})
        out.append(cell)
    return out


def rung_status(table: list[dict], configs=CONFIGS) -> dict[float, str]:
    """VALID: every configuration has >= MIN_VALID_REPS non-client-limited replicates. Otherwise the rung is excluded:
    CO_LOCATION_AMBIGUOUS when only some configurations were affected (the shared machine cannot separate client from Fabric load),
    CLIENT_LIMITED when all were, MISSING when a cell is absent."""
    by = {(c["B"], c["rate_tps"]): c for c in table}
    status = {}
    for rate in sorted({c["rate_tps"] for c in table}):
        cells = [by.get((b, rate)) for b in configs]
        if any(c is None for c in cells):
            status[rate] = "MISSING"
        elif all(c["cell_valid"] for c in cells):
            status[rate] = "VALID"
        else:
            hit = [c["any_client_limited"] for c in cells]
            status[rate] = "CLIENT_LIMITED" if all(hit) else "CO_LOCATION_AMBIGUOUS"
    return status


def compare(a: dict, b: dict) -> str:
    """Pairwise verdict for cells a and b of the same rung: 'A', 'B' (the better one) or 'TIE'. Unpaired separation rule: the p95 gap
    must exceed the SESOI AND every valid replicate of the worse cell must exceed every valid replicate of the better cell."""
    if a["saturated"] and not b["saturated"]:
        return "B"
    if b["saturated"] and not a["saturated"]:
        return "A"
    if a["saturated"] and b["saturated"]:
        return "TIE"
    d, s = a["p95_ms"] - b["p95_ms"], sesoi_ms(a["p95_ms"], b["p95_ms"])
    if d > s and min(a["p95_reps"]) > max(b["p95_reps"]):
        return "B"
    if -d > s and min(b["p95_reps"]) > max(a["p95_reps"]):
        return "A"
    return "TIE"


def analyse_rungs(table: list[dict], configs=CONFIGS) -> dict:
    status = rung_status(table, configs)
    by = {(c["B"], c["rate_tps"]): c for c in table}
    rates = sorted(status)
    r_max = None
    for x in rates:                         # contiguous from below
        if status[x] == "VALID":
            r_max = x
        else:
            break
    if r_max is None:
        return {"r_max": None, "usable_rates": [], "status": status, "verdicts": {}, "regimes": {}, "regime_configs": [],
                "ordering_reversal": False, "transition_rate": None, "winner_sequence": []}
    usable = [x for x in rates if x <= r_max]
    verdicts, winners = {}, []
    for x in usable:
        pair = {}
        for i, a in enumerate(configs):
            for b in configs[i + 1:]:
                v = compare(by[(a, x)], by[(b, x)])
                pair[f"{a}v{b}"] = a if v == "A" else b if v == "B" else "TIE"
        win = [c for c in configs if all(pair[f"{min(c, o)}v{max(c, o)}"] == c for o in configs if o != c)]
        w = win[0] if win else None
        verdicts[x] = {"pairs": pair, "winner": w, "sesoi_ms": {str(c): sesoi_ms(by[(c, x)]["p95_ms"], min(by[(o, x)]["p95_ms"] for o in configs))
                                                              for c in configs}}
        winners.append(w)
    regimes = {}
    for c in configs:                       # decisive at CONFIRM_RUNGS consecutive usable rungs
        runs = [usable[i:i + CONFIRM_RUNGS] for i in range(len(usable) - CONFIRM_RUNGS + 1)
                if all(winners[i + j] == c for j in range(CONFIRM_RUNGS))]
        if runs:
            regimes[c] = sorted({x for r in runs for x in r})
    reg = sorted(regimes, key=lambda c: min(regimes[c]))
    transition = None
    if len(reg) >= 2:                       # first hand-over between two confirmed regimes, in rate order
        a, b = reg[0], reg[1]
        transition = round(math.sqrt(max(regimes[a]) * min(regimes[b])), 3)
    return {"r_max": r_max, "usable_rates": usable, "status": status, "verdicts": verdicts, "regimes": regimes,
            "regime_configs": reg, "ordering_reversal": len(reg) >= 2, "transition_rate": transition,
            "winner_sequence": [[x, w] for x, w in zip(usable, winners)]}


# ------------------------------------------------------------------------------------------------------------------ surface
class Surface:
    """Per-configuration (p95, mean, saturated) over the usable rungs, log-rate linear interpolation, None outside the measured range.
    A rate between a saturated and an unsaturated rung counts as saturated (no claim that the configuration works there)."""

    def __init__(self, table: list[dict], usable: list[float], configs=CONFIGS):
        by = {(c["B"], c["rate_tps"]): c for c in table}
        self.configs = tuple(configs)
        self.pts = {b: [(x, by[(b, x)]["p95_ms"], by[(b, x)]["mean_ms"], bool(by[(b, x)]["saturated"])) for x in usable if (b, x) in by
                        and by[(b, x)].get("p95_ms") is not None] for b in configs}
        self.lo = min(usable) if usable else None
        self.hi = max(usable) if usable else None

    def clamp(self, x: float) -> float:
        return min(max(x, self.lo), self.hi)

    def at(self, b: int, x: float):
        pts = self.pts[b]
        if not pts or x < pts[0][0] - 1e-9 or x > pts[-1][0] + 1e-9:
            return None
        for xe, pe, me, se in pts:                      # a measured rung is returned as measured (its own saturation flag)
            if abs(xe - x) <= 1e-9:
                return pe, me, se
        for (x0, p0, m0, s0), (x1, p1, m1, s1) in zip(pts, pts[1:]):
            if x0 - 1e-9 <= x <= x1 + 1e-9:
                t = (math.log(x) - math.log(x0)) / (math.log(x1) - math.log(x0))
                return p0 + t * (p1 - p0), m0 + t * (m1 - m0), (s0 or s1)
        x0, p0, m0, s0 = pts[-1]
        return p0, m0, s0


def evaluate_workload(surface: Surface, rates: list[float], preroll_rate: float | None = None) -> dict:
    """Headroom of a per-segment configuration choice on a workload given as per-minute rates (tx/s). Everything is read from the
    surface measured in X1; no controller is run. Policies: static (best fixed config), oracle (perfect foresight, zero switching cost: THE GATE),
    persistence (choose by the previous minute's rate; a DIAGNOSTIC of how unsafe a no-skill rule is near a saturation cliff, never a gate).
    Saturated configurations are never chosen by oracle/persistence when an unsaturated one exists."""
    n = len(rates)
    pts = [{b: surface.at(b, x) for b in surface.configs} for x in rates]
    cov = [i for i, per in enumerate(pts) if all(v is not None for v in per.values())]
    out = {"n_segments": n, "n_covered": len(cov), "coverage": len(cov) / n if n else 0.0}
    if not cov:
        return {**out, "evaluable": False}

    def oracle_choice(per):
        pool = {b: v for b, v in per.items() if not v[2]} or per
        return min(pool, key=lambda b: (pool[b][0], b))

    static = {b: {"mean_p95_ms": st.fmean(pts[i][b][0] for i in cov), "saturated_segments": sum(1 for i in cov if pts[i][b][2])}
              for b in surface.configs}
    tuned = min(static, key=lambda b: (static[b]["saturated_segments"], static[b]["mean_p95_ms"], b))
    o_choice = {i: oracle_choice(pts[i]) for i in cov}
    o_mean = st.fmean(pts[i][o_choice[i]][0] for i in cov)
    o_sat = sum(1 for i in cov if pts[i][o_choice[i]][2])
    prev = list(rates[:-1])
    first_prev = preroll_rate if preroll_rate is not None else rates[0]
    prev = [first_prev] + prev
    p_choice = {}
    for i in cov:
        per_prev = {b: surface.at(b, surface.clamp(prev[i])) for b in surface.configs}
        p_choice[i] = oracle_choice(per_prev)
    p_mean = st.fmean(pts[i][p_choice[i]][0] for i in cov)
    p_sat = sum(1 for i in cov if pts[i][p_choice[i]][2])
    s_mean = static[tuned]["mean_p95_ms"]
    gain_segments = sum(1 for i in cov if pts[i][tuned][0] - pts[i][o_choice[i]][0] > sesoi_ms(pts[i][tuned][0], pts[i][o_choice[i]][0]))
    share = {b: sum(1 for i in cov if o_choice[i] == b) / len(cov) for b in surface.configs}
    menu = sorted({tuned} | {b for b, v in share.items() if v >= MIN_REGIME_SHARE})
    return {**out, "evaluable": True, "static": static, "tuned_static_B": tuned, "static_mean_p95_ms": s_mean,
            "oracle_mean_p95_ms": o_mean, "persistence_mean_p95_ms": p_mean,
            "headroom_oracle": (s_mean - o_mean) / s_mean, "headroom_oracle_ms": s_mean - o_mean,
            "headroom_persistence": (s_mean - p_mean) / s_mean, "headroom_persistence_ms": s_mean - p_mean,
            "oracle_saturated_segments": o_sat, "persistence_saturated_segments": p_sat, "segments_with_sesoi_gain": gain_segments,
            "oracle_share": share, "menu": menu, "oracle_choice": [o_choice[i] for i in cov],
            "persistence_choice": [p_choice[i] for i in cov]}


def gate_failures(ev: dict, loo: list[dict]) -> list[str]:
    """Pre-registered conditions on the selected window's evaluation (ev) and its leave-one-replicate-out re-evaluations (loo)."""
    f = []

    def one(e, tag):
        if not e.get("evaluable") or e["coverage"] < 1.0:
            f.append(f"{tag}: workload not fully covered by the measured range")
            return
        if e["headroom_oracle"] < HEADROOM_ORACLE_MIN or e["headroom_oracle_ms"] < MIN_ABS_GAIN_MS:
            f.append(f"{tag}: oracle headroom {e['headroom_oracle']:.3f} ({e['headroom_oracle_ms']:.1f} ms) below "
                     f"{HEADROOM_ORACLE_MIN} / {MIN_ABS_GAIN_MS} ms")
        if e["segments_with_sesoi_gain"] < MIN_GAIN_SEGMENTS:
            f.append(f"{tag}: only {e['segments_with_sesoi_gain']} segments gain more than the SESOI (< {MIN_GAIN_SEGMENTS})")
        if e["oracle_saturated_segments"]:
            f.append(f"{tag}: the oracle itself hits a saturated configuration on {e['oracle_saturated_segments']} segments")
        if len(e["menu"]) < MIN_MENU_SIZE:
            f.append(f"{tag}: menu {e['menu']} has fewer than {MIN_MENU_SIZE} configurations")

    one(ev, "all replicates")
    for j, e in enumerate(loo):
        one(e, f"leave-out-{e.get('left_out', j)}")
    return f


# ------------------------------------------------------------------------------------------------------------------ window and k
def choose_k(segment_rates_tps: list[float], r_x: float, r_max: float) -> dict:
    """Structural rule (uses the transition rate and r_max only; never a headroom value):
       k0 = r_x / sqrt(min m * max m);  k = min(k0, CAP_FRAC * r_max / max m, K_MAX);  feasible iff k >= K_MIN and >= 2 scaled segments lie
       below r_x(1-delta) and >= 2 above r_x(1+delta)."""
    m = [float(v) for v in segment_rates_tps]
    k0 = r_x / math.sqrt(min(m) * max(m))
    k_cap = CAP_FRAC * r_max / max(m)
    k = round(min(k0, k_cap, K_MAX), 3)
    scaled = [k * v for v in m]
    below = sum(1 for v in scaled if v <= r_x * (1 - STRADDLE_DELTA))
    above = sum(1 for v in scaled if v >= r_x * (1 + STRADDLE_DELTA))
    feasible = k >= K_MIN and below >= MIN_SEGMENTS_EACH_SIDE and above >= MIN_SEGMENTS_EACH_SIDE
    why = None if feasible else ("k below 1 (the window is already above the transition at natural scale)" if k < K_MIN
                                 else "scaled window does not straddle the transition rate")
    return {"k": k, "k0": k0, "k_cap": k_cap, "scaled_segment_rates": scaled, "segments_below": below, "segments_above": above,
            "feasible": feasible, "infeasible_reason": why}


def select_window(ranked: list[dict], r_x: float, r_max: float) -> dict:
    tried = []
    for cand in ranked[:FALLBACK_TOP_N]:
        res = choose_k(cand["segment_rates_tps"], r_x, r_max)
        tried.append({"start_s": cand["start_s"], "k": res["k"], "feasible": res["feasible"], "infeasible_reason": res["infeasible_reason"]})
        if res["feasible"]:
            return {"status": "WINDOW_AND_K_SELECTED", "window": cand, "k": res["k"], "detail": res, "tried": tried}
    return {"status": "NO_FEASIBLE_WINDOW", "window": None, "k": None, "tried": tried}


# ------------------------------------------------------------------------------------------------------------------ decision
def decide(rows: list[dict], ranked_windows: list[dict], extra_workloads: dict[str, list[float]] | None = None) -> dict:
    """The whole pre-registered chain as ONE deterministic function of the X1 measurements and the frozen window ranking."""
    table = build_rung_table(rows)
    A = analyse_rungs(table)
    out = {"protocol": PROTOCOL_ID, "n_rows": len(rows), "rung_table": table, "rungs": A}
    if A["r_max"] is None:
        out.update({"status": "STOP_NOT_ESTABLISHED", "reason": "NO_CLIENT_VALID_RUNG_AT_THE_LOWEST_RATE",
                    "meaning": "X1 could not establish any client-valid rung: no conclusion about block size headroom"})
        return out
    surface = Surface(table, A["usable_rates"])
    out["descriptive_workloads"] = {}
    for name, rates in {"all_eligible_windows_k1": [r for w in ranked_windows for r in w["segment_rates_tps"]],
                        **(extra_workloads or {})}.items():
        out["descriptive_workloads"][name] = evaluate_workload(surface, rates)
    if not A["ordering_reversal"]:
        dom = A["regime_configs"][0] if A["regime_configs"] else None
        out.update({"status": "STOP_NO_HEADROOM", "reason": "NO_ORDERING_REVERSAL",
                    "meaning": "no two configurations are each decisively best at confirmed rungs: one fixed configuration "
                               f"({'B=' + str(dom) if dom else 'no decisive winner'}) is enough in the client-valid range; the adaptive "
                               f"comparison is not run (highest valid rung r_max = {A['r_max']} tx/s)"})
        return out
    sel = select_window(ranked_windows, A["transition_rate"], A["r_max"])
    out["window_selection"] = sel
    if sel["status"] != "WINDOW_AND_K_SELECTED":
        out.update({"status": "STOP_NO_HEADROOM", "reason": "NO_FEASIBLE_WINDOW",
                    "meaning": "no ranked window straddles the transition at an achievable load (k >= 1, within the client-valid range)"})
        return out
    win, k = sel["window"], sel["k"]
    rates = [k * m for m in win["segment_rates_tps"]]
    pre = k * win["preroll_rate_tps"]
    ev = evaluate_workload(surface, rates, pre)
    loo = []
    for j in sorted({r["replicate"] for r in rows}):
        rows_j = [r for r in rows if r["replicate"] != j]
        t_j = build_rung_table(rows_j, min_valid_reps=1)
        s_j = Surface(t_j, A["usable_rates"])
        e = evaluate_workload(s_j, rates, pre)
        e["left_out"] = j
        loo.append(e)
    fails = gate_failures(ev, loo)
    out.update({"selected_window": {"start_s": win["start_s"], "rank": win["rank"], "k": k, "scaled_segment_rates_tps": rates,
                                    "scaled_preroll_rate_tps": pre, "requires_rate_scaling": k > 1.0},
                "selected_evaluation": ev, "leave_one_replicate_out": loo, "gate_failures": fails})
    if fails:
        out.update({"status": "STOP_NO_HEADROOM", "reason": "HEADROOM_GATE_NOT_MET",
                    "meaning": "an ordering reversal exists but the pre-registered headroom conditions are not met on the structurally "
                               "selected window; the live controller comparison is not run"})
    else:
        out.update({"status": "GO_LIVE_DESIGN", "reason": "HEADROOM_ESTABLISHED", "menu": ev["menu"],
                    "meaning": "pre-registered headroom conditions met; design (not run) a separately authorized live comparison "
                               f"with menu {ev['menu']} on window {win['start_s']} at k={k}"})
    return out
