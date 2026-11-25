"""Offline analysis for v2 (stdlib). Reads only what the runs wrote (arm.json from the stage, analysis.json / run_result.json from the
unchanged closed_loop analyzer). Nothing here runs Fabric. Pure functions over dicts so they are testable with synthetic data.

  x0_round_rows      one row per (X0 run, rate rung): latency, delivered/offered, driver lag, class
  build_rung_table   aggregate replicates per (B, rate) for protocol.analyse_crossover
  build_surface      the LocalSurfacePredictor input (client-valid rungs only; delivered fraction normalized per B)
  forecast_accuracy  per predictive_live run: EWMA forecast vs the realized next-minute rate, against the naive persistence forecast
  regret_tables      decision regret: model-based (X0 surface, realized rate) and realized (measured segment vs the best static)
  m_arm_summary      run-level throughput / mean / p95 and configuration changes per arm
"""

from __future__ import annotations

import math
import statistics as st

from . import protocol as P


# ------------------------------------------------------------------------------------------------------------------ X0
def x0_round_rows(arm: dict, analysis: dict) -> list[dict]:
    rate_of = {int(k): float(v) for k, v in arm["x0"]["rate_of_round"].items()}
    rows = []
    for r in analysis["rounds"]:
        if "error" in r or not r.get("measured"):
            continue
        idx = int(r["round"])
        lat = r["latency_ms"]
        d = r.get("dispatch", {})
        lag = (d.get("lag_ms") or {}).get("p99") if d.get("present") else None
        cls = P.classify_round(scheduled=int(r["scheduled"]), rows=int(r["rows"]), failed=int(r["failed"]), lag_p99_ms=lag,
                               overflow=d.get("overflow_callbacks"))
        wall_s = r["round_wall_ms"] / 1000.0
        rows.append({"run_id": arm["run_id"], "B": int(arm["x0"]["B"]), "replicate": int(arm["replicate"]), "round": idx,
                     "rate_tps": rate_of[idx], "scheduled": int(r["scheduled"]), "success": int(r["success"]), "failed": int(r["failed"]),
                     "pending_at_deactivate": r.get("pending_at_deactivate"), "mean_ms": lat["mean"], "p50_ms": lat["p50"],
                     "p95_ms": lat["p95"], "p99_ms": lat["p99"], "round_wall_s": wall_s, "lag_p99_ms": lag,
                     "overflow_callbacks": d.get("overflow_callbacks"), "class": cls})
    return rows


def build_rung_table(round_rows: list[dict]) -> list[dict]:
    out = []
    keys = sorted({(r["B"], r["rate_tps"]) for r in round_rows})
    for b, rate in keys:
        reps = sorted((r for r in round_rows if r["B"] == b and r["rate_tps"] == rate), key=lambda r: r["replicate"])
        ok = all(r["class"] not in ("CLIENT_LIMITED", "INCOMPLETE") for r in reps) and len(reps) >= P.X0_REPLICATES_PER_B
        out.append({"B": b, "rate_tps": rate, "n_reps": len(reps), "p95_reps": [r["p95_ms"] for r in reps],
                    "p95_ms": st.fmean(r["p95_ms"] for r in reps), "mean_ms": st.fmean(r["mean_ms"] for r in reps),
                    "round_wall_s": st.fmean(r["round_wall_s"] for r in reps), "client_valid": ok,
                    "classes": [r["class"] for r in reps],
                    "failed_exceeds": any(r["failed"] > P.FAILED_FRACTION_LIMIT * r["scheduled"] for r in reps),
                    "pending_max": max((r["pending_at_deactivate"] or 0) for r in reps)})
    return out


def build_surface(rung_table: list[dict], r_max: float | None) -> list[dict]:
    """Points for the local predictor: client-valid rungs up to r_max. delivered_fraction = reference wall / wall, capped at 1,
    where the reference is that B's lowest-rate valid rung (so the drain tail of an unsaturated round is not read as backlog)."""
    pts = []
    for b in P.MENU_B:
        rows = sorted((r for r in rung_table if r["B"] == b and r["client_valid"] and (r_max is None or r["rate_tps"] <= r_max)),
                      key=lambda r: r["rate_tps"])
        if not rows:
            continue
        ref = rows[0]["round_wall_s"]
        for r in rows:
            pts.append({"B": b, "rate_tps": r["rate_tps"], "mean_ms": r["mean_ms"], "p95_ms": r["p95_ms"],
                        "delivered_fraction": min(1.0, ref / r["round_wall_s"]), "client_valid": True})
    return pts


# ------------------------------------------------------------------------------------------------------------------ M
def realized_rates(arm: dict) -> dict[int, float]:
    """Offered rate of each round from the staged schedule (the realized arrival process; verified against the recorder by the analyzer)."""
    return {r["index"]: r["n_tx"] / (r["length_ms"] / 1000.0) for r in arm["rounds"]}


def forecast_accuracy(arm: dict, decisions: list[dict]) -> dict:
    """Decision d forecasts the mean arrival rate of segment d (round d+1) from rounds <= d. Naive = the rate of round d."""
    rate = realized_rates(arm)
    rows = []
    for d in decisions:
        meta = d.get("controller_metadata") or {}
        f = meta.get("forecast_tps")
        i = d["decision_index"]
        if f is None or (i + 1) not in rate:
            rows.append({"decision": i, "forecast_tps": None, "abstained": d.get("abstained")})
            continue
        actual, naive = rate[i + 1], rate[i]
        rows.append({"decision": i, "forecast_tps": f, "realized_tps": actual, "naive_persistence_tps": naive,
                     "err_ewma": f - actual, "err_naive": naive - actual, "abstained": d.get("abstained")})
    scored = [r for r in rows if r.get("realized_tps") is not None]
    summary = None
    if scored:
        mae_e = st.fmean(abs(r["err_ewma"]) for r in scored)
        mae_n = st.fmean(abs(r["err_naive"]) for r in scored)
        summary = {"n": len(scored), "mae_ewma_tps": mae_e, "mae_naive_tps": mae_n, "bias_ewma_tps": st.fmean(r["err_ewma"] for r in scored),
                   "skill_vs_naive": (1 - mae_e / mae_n) if mae_n > 0 else None}
    return {"rows": rows, "summary": summary}


def _b_in_force(round_entry: dict) -> int | None:
    c = round_entry.get("committed_under_B")
    if not c:
        return None
    return int(max(c.items(), key=lambda kv: kv[1])[0])


def regret_tables(arm: dict, analysis: dict, predictor, static_segment_p95: dict[int, dict[int, float]] | None = None) -> dict:
    """Per measured segment s (round s+1):
         model regret   = surface mean latency at the B in force and the realized rate - the surface minimum over B at that rate
                          (blank where the rate is outside the X0 surface: not identifiable)
         realized regret = measured p95 of this segment - the lower of the two static arms' measured p95 for the same segment
                          (static_segment_p95[B][segment], means over their replicates; blank if not supplied)"""
    from fabric_perf.controllers.base import FabricConfig
    rates = realized_rates(arm)
    out = []
    for e in analysis["rounds"]:
        if "error" in e or not e.get("measured"):
            continue
        s = int(e["segment"])
        b = _b_in_force(e)
        row = {"segment": s, "rate_tps": rates[e["round"]], "B_in_force": b, "p95_ms": e["latency_ms"]["p95"], "mean_ms": e["latency_ms"]["mean"]}
        try:
            cfg = {bb: predictor.predict(rates[e["round"]], FabricConfig(max_message_count=bb)).latency_ms for bb in P.MENU_B}
            best = min(cfg.values())
            row.update({"surface_mean_ms": {str(k): v for k, v in cfg.items()}, "model_regret_ms": (cfg[b] - best) if b in cfg else None})
        except ValueError:
            row.update({"surface_mean_ms": None, "model_regret_ms": None})
        if static_segment_p95:
            best_static = min(static_segment_p95[bb][s] for bb in P.MENU_B)
            row["best_static_p95_ms"] = best_static
            row["realized_regret_p95_ms"] = e["latency_ms"]["p95"] - best_static
        out.append(row)
    return {"rows": out}


def m_arm_summary(runs: list[dict]) -> dict:
    """runs: dicts with arm_label, run_level (analysis['run_level_measured']), n_changes. Returns arm -> means (descriptive)."""
    arms: dict[str, list[dict]] = {}
    for r in runs:
        arms.setdefault(r["arm_label"], []).append(r)
    out = {}
    for arm, rs in arms.items():
        def f(key):
            vals = [x["run_level"][key] if key != "p95" and key != "mean" else x["run_level"]["latency_ms"][key] for x in rs]
            return st.fmean(vals), (st.stdev(vals) if len(vals) > 1 else None)
        out[arm] = {"runs": len(rs), "throughput_tps": f("throughput_tps_over_round_walls"), "mean_ms": f("mean"), "p95_ms": f("p95"),
                    "config_changes_mean": st.fmean(x["n_changes"] for x in rs)}
    return out


def crossover_report(round_rows: list[dict]) -> dict:
    table = build_rung_table(round_rows)
    res = P.analyse_crossover(table)
    surface = build_surface(table, res["r_max"])
    return {"rung_table": table, "crossover": res, "surface_points": surface}


def finite(x) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)
