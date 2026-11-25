"""Offline analysis for M2 (stdlib). Reads only what the runs wrote (arm.json from the stage, analysis.json / run_result.json from the unchanged
closed_loop analyzer). Pure functions over dicts, testable with synthetic data. Nothing here runs Fabric.

  run_record        one flat record per run: throughput, mean/p95/p99 latency, success/failure, decisions, configuration changes, per-segment B
  summarize_arms    per arm: mean / sd / min / max over comparable runs
  pairwise          arm-vs-arm verdicts under protocol_m2.MEANINGFUL_RULE (SESOI + complete separation)
  decision_table    what each controller selected, how often it changed, how often it abstained
  model_regret      model-based (X1 surface) mean-latency regret of the B actually in force per segment (a model quantity, not a measurement)
  final_table       the single Static vs Reactive vs Predictive table plus the mechanical verdict
"""

from __future__ import annotations

import statistics as st
from collections import Counter

from . import protocol_m2 as P
from Fabric_Live_Testbed.closed_loop_v2 import protocol as V2P

METRICS = ("throughput_tps", "delivered_ratio", "mean_ms", "p95_ms", "p99_ms", "failure_rate", "config_changes", "decisions_changed")
LABELS = {"static_b2": "Static (B=2)", "reactive": "Reactive", "predictive_live": "Predictive", "static_b10": "Static B=10 (interpretive)"}


# ------------------------------------------------------------------------------------------------------------------ per run
def _round_class(r: dict) -> str:
    d = r.get("dispatch") or {}
    lag = (d.get("lag_ms") or {}).get("p99") if d.get("present") else None
    return V2P.classify_round(scheduled=int(r["scheduled"]), rows=int(r["rows"]), failed=int(r["failed"]), lag_p99_ms=lag,
                              overflow=d.get("overflow_callbacks"))


def _modal_b(entry: dict) -> int | None:
    c = entry.get("committed_under_B")
    return int(max(c.items(), key=lambda kv: kv[1])[0]) if c else None


def run_record(arm: dict, analysis: dict, run_result: dict, *, independent_pass: bool | None = None) -> dict:
    rl = analysis["run_level_measured"]
    lat = rl["latency_ms"]
    measured = [r for r in analysis["rounds"] if "error" not in r and r.get("measured")]
    classes = [_round_class(r) for r in measured]
    verdict = run_result.get("verdict")
    decisions = analysis.get("decisions") or []
    transitions = [t for t in (analysis.get("transitions") or []) if t.get("success")]
    sched = int(rl["scheduled_measured"])
    regimes = analysis.get("configuration_regimes") or []
    segments = []
    for r in measured:
        segments.append({"segment": r["segment"], "rate_tps": r["offered_tps"], "B": _modal_b(r), "mean_ms": r["latency_ms"]["mean"],
                         "p95_ms": r["latency_ms"]["p95"], "success": r["success"], "failed": r["failed"], "class": _round_class(r),
                         "pending_at_deactivate": r.get("pending_at_deactivate")})
    rec = {"run_id": arm["run_id"], "arm_label": arm["arm_label"], "replicate": arm.get("replicate"), "initial_B": arm["initial_B"],
           "verdict": verdict, "analysis_flags": analysis.get("flags"), "independent_pass": independent_pass,
           "round_classes": dict(Counter(classes)), "n_client_limited": classes.count("CLIENT_LIMITED"), "n_incomplete": classes.count("INCOMPLETE"),
           "valid_run": verdict in ("VALID", "VALID_WITH_FLAGS") and not analysis.get("problems"),
           "scheduled": sched, "success": int(rl["success"]), "failed": int(rl["failed"]),
           "success_rate": rl["success"] / sched if sched else None, "failure_rate": rl["failed"] / sched if sched else None,
           "throughput_tps": rl["throughput_tps_over_round_walls"], "offered_tps": rl["offered_tps"],
           "delivered_ratio": (rl["throughput_tps_over_round_walls"] / rl["offered_tps"]) if rl["offered_tps"] else None,
           "mean_ms": lat["mean"], "p50_ms": lat["p50"], "p95_ms": lat["p95"], "p99_ms": lat["p99"], "max_ms": lat["max"],
           "n_decisions": len(decisions), "decisions_changed": sum(1 for d in decisions if d.get("changed")),
           "decisions_abstained": sum(1 for d in decisions if d.get("abstained")),
           "target_counts": {str(k): v for k, v in sorted(Counter(int(d["target_B"]) for d in decisions).items())},
           "config_changes": len(transitions), "ledger_config_blocks": max(0, len(regimes) - 1),
           "B_by_segment": [s["B"] for s in segments], "segments": segments}
    rec["comparable"] = bool(rec["valid_run"] and rec["n_client_limited"] == 0 and rec["n_incomplete"] == 0 and independent_pass is not False)
    if arm["arm"] == "predictive_live":
        rec["forecast"] = forecast_accuracy(arm, decisions)
    return rec


def realized_rates(arm: dict) -> dict[int, float]:
    return {r["index"]: r["n_tx"] / (r["length_ms"] / 1000.0) for r in arm["rounds"]}


def forecast_accuracy(arm: dict, decisions: list[dict]) -> dict:
    """Decision d forecasts the mean arrival rate of segment d (round d+1) from rounds <= d. Naive persistence = the rate of round d."""
    rate = realized_rates(arm)
    rows = []
    for d in decisions:
        f = (d.get("controller_metadata") or {}).get("forecast_tps")
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
        mae_e, mae_n = st.fmean(abs(r["err_ewma"]) for r in scored), st.fmean(abs(r["err_naive"]) for r in scored)
        mape = st.fmean(abs(r["err_ewma"]) / r["realized_tps"] for r in scored if r["realized_tps"] > 0)
        summary = {"n": len(scored), "mae_ewma_tps": mae_e, "mae_naive_tps": mae_n, "mape_ewma": mape,
                   "bias_ewma_tps": st.fmean(r["err_ewma"] for r in scored), "skill_vs_naive": (1 - mae_e / mae_n) if mae_n > 0 else None}
    return {"rows": rows, "summary": summary}


def model_regret(arm: dict, record: dict) -> list[dict]:
    """Per measured segment: surface mean latency of the B in force minus the surface minimum over the menu, at the realized offered rate.
    MODEL quantity from the staged X1 surface (predictive arm only carries the surface); blank where the rate is outside the surface."""
    pl = arm.get("predictive_live")
    if not pl:
        return []
    from Fabric_Live_Testbed.closed_loop_v2.independent import interp
    pts, out = pl["surface_points"], []
    for s in record["segments"]:
        try:
            lat = {b: interp(pts, b, float(s["rate_tps"]), "mean_ms") for b in P.MENU_B}
        except ValueError:
            out.append({"segment": s["segment"], "rate_tps": s["rate_tps"], "B_in_force": s["B"], "regret_ms": None})
            continue
        best = min(lat, key=lambda b: (lat[b], b))
        out.append({"segment": s["segment"], "rate_tps": s["rate_tps"], "B_in_force": s["B"], "surface_best_B": best,
                    "regret_ms": (lat[s["B"]] - lat[best]) if s["B"] in lat else None})
    return out


# ------------------------------------------------------------------------------------------------------------------ per arm
def _select(records: list[dict], arm_label: str, analysis_set: str) -> list[dict]:
    rs = [r for r in records if r["arm_label"] == arm_label]
    return [r for r in rs if (r["comparable"] if analysis_set == "comparable" else r["valid_run"])]


def summarize_arms(records: list[dict], *, analysis_set: str = "comparable") -> dict:
    out = {}
    for label in P.ARMS:
        all_runs = [r for r in records if r["arm_label"] == label]
        rs = _select(records, label, analysis_set)
        row = {"runs_total": len(all_runs), "runs_used": len(rs), "run_ids_used": [r["run_id"] for r in rs],
               "run_ids_excluded": [r["run_id"] for r in all_runs if r not in rs]}
        for m in METRICS:
            vals = [float(r[m]) for r in rs if r.get(m) is not None]
            row[m] = {"mean": st.fmean(vals) if vals else None, "sd": st.stdev(vals) if len(vals) > 1 else None,
                      "min": min(vals) if vals else None, "max": max(vals) if vals else None, "values": vals}
        row["success_rate"] = {"mean": st.fmean([r["success_rate"] for r in rs]) if rs else None}
        row["failed_tx_total"] = sum(r["failed"] for r in rs)
        row["decisions_per_run"] = st.fmean([r["n_decisions"] for r in rs]) if rs else None
        row["abstentions_per_run"] = st.fmean([r["decisions_abstained"] for r in rs]) if rs else None
        tc = Counter()
        for r in rs:
            for b, n in r["target_counts"].items():
                tc[b] += n
        row["target_counts"] = dict(sorted(tc.items(), key=lambda kv: int(kv[0])))
        out[label] = row
    return out


def pairwise(summary: dict, a: str, b: str) -> dict:
    """Verdicts for arm a vs arm b (a named first). All metrics under the pre-registered rule."""
    sa, sb = summary[a], summary[b]
    res = {}
    for m, lower_better, sesoi in (
            ("p95_ms", True, lambda x, y: P.sesoi_latency_ms(x, y)), ("mean_ms", True, lambda x, y: P.sesoi_latency_ms(x, y)),
            ("throughput_tps", False, lambda x, y: P.THROUGHPUT_SESOI_REL * max(x, y)),
            ("failure_rate", True, lambda x, y: P.FAILURE_SESOI_ABS)):
        va, vb = sa[m]["values"], sb[m]["values"]
        if not va or not vb:
            res[m] = {"verdict": "NOT_ASSESSABLE"}
            continue
        res[m] = P.compare_values(va, vb, sesoi(st.fmean(va), st.fmean(vb)), lower_is_better=lower_better)
    return res


def decision_table(records: list[dict]) -> dict:
    """Per arm (all valid runs): selections by B, applied changes, abstentions, and agreement with the Static B=2 configuration."""
    out = {}
    for label in P.ARMS:
        rs = [r for r in records if r["arm_label"] == label and r["valid_run"]]
        tc = Counter()
        for r in rs:
            for b, n in r["target_counts"].items():
                tc[int(b)] += n
        n_dec = sum(tc.values())
        out[label] = {"runs": len(rs), "decisions": n_dec, "selected_B_counts": dict(sorted(tc.items())),
                      "decisions_selecting_B2": tc.get(2, 0), "decisions_changed": sum(r["decisions_changed"] for r in rs),
                      "config_changes_applied": sum(r["config_changes"] for r in rs), "abstentions": sum(r["decisions_abstained"] for r in rs),
                      "B_in_force_by_segment": {r["run_id"]: r["B_by_segment"] for r in rs}}
    prim = [out[a] for a in P.PRIMARY_ARMS if out[a]["runs"]]
    out["all_primary_arms_held_B2"] = bool(prim) and all(set(x["selected_B_counts"]) <= {2} and x["config_changes_applied"] == 0 for x in prim)
    return out


# ------------------------------------------------------------------------------------------------------------------ final table
def _fmt(m: dict, digits: int = 1, scale: float = 1.0, pct: bool = False) -> str:
    if m["mean"] is None:
        return "n/a"
    f = lambda v: f"{v * scale:.{digits}f}{'%' if pct else ''}"  # noqa: E731
    s = f(m["mean"])
    if m["sd"] is not None:
        s += f" ± {f(m['sd'])}"
    return s


def final_table(records: list[dict], *, analysis_set: str = "comparable") -> dict:
    summary = summarize_arms(records, analysis_set=analysis_set)
    dec = decision_table(records)
    cols = [a for a in P.ARMS if summary[a]["runs_total"]]
    header = ["Metric (mean ± sd over runs)"] + [LABELS[a] for a in cols]
    rows = [["Runs used / run (valid)"] + [f"{summary[a]['runs_used']} / {summary[a]['runs_total']}" for a in cols],
            ["Throughput, committed tx/s"] + [_fmt(summary[a]["throughput_tps"], 2) for a in cols],
            ["Delivered / offered"] + [_fmt(summary[a]["delivered_ratio"], 2, 100, True) for a in cols],
            ["Mean latency, ms"] + [_fmt(summary[a]["mean_ms"], 1) for a in cols],
            ["p95 latency, ms"] + [_fmt(summary[a]["p95_ms"], 1) for a in cols],
            ["p99 latency, ms"] + [_fmt(summary[a]["p99_ms"], 1) for a in cols],
            ["Transaction success rate"] + [("n/a" if summary[a]["success_rate"]["mean"] is None else f"{100 * summary[a]['success_rate']['mean']:.3f}%") for a in cols],
            ["Failed transactions (total)"] + [str(summary[a]["failed_tx_total"]) for a in cols],
            ["Configuration changes applied / run"] + [_fmt(summary[a]["config_changes"], 2) for a in cols],
            ["Decisions / run (changed / abstained)"] + [f"{summary[a]['decisions_per_run'] if summary[a]['decisions_per_run'] is not None else 'n/a'}"
                                                         f" ({_fmt(summary[a]['decisions_changed'], 1)} / "
                                                         f"{'n/a' if summary[a]['abstentions_per_run'] is None else round(summary[a]['abstentions_per_run'], 1)})"
                                                         for a in cols],
            ["Block size selected (decisions per B)"] + [", ".join(f"B={b}: {n}" for b, n in summary[a]["target_counts"].items()) or "n/a" for a in cols]]
    fc = [r["forecast"]["summary"] for r in records if r["arm_label"] == "predictive_live" and r["valid_run"] and r.get("forecast", {}).get("summary")]
    if fc:
        rows.append(["Forecast MAE, tx/s (EWMA vs persistence)"] + [
            (f"{st.fmean(f['mae_ewma_tps'] for f in fc):.2f} vs {st.fmean(f['mae_naive_tps'] for f in fc):.2f}" if a == "predictive_live" else "-") for a in cols])
    pw = {}
    for a, b in (("reactive", "static_b2"), ("predictive_live", "static_b2"), ("predictive_live", "reactive"), ("static_b10", "static_b2")):
        if a in summary and b in summary and summary[a]["runs_used"] and summary[b]["runs_used"]:
            pw[f"{a}_vs_{b}"] = pairwise(summary, a, b)
    verdict = verdict_text(summary, pw, dec)
    return {"analysis_set": analysis_set, "columns": cols, "header": header, "rows": rows, "summary": summary, "pairwise": pw, "decisions": dec,
            "verdict": verdict, "markdown": to_markdown(header, rows)}


def verdict_text(summary: dict, pw: dict, dec: dict) -> dict:
    prim = list(P.PRIMARY_ARMS)
    short = [a for a in prim if summary[a]["runs_used"] < P.REPLICATES_MIN]
    if short:
        return {"status": "INSUFFICIENT_REPLICATION", "text": f"fewer than {P.REPLICATES_MIN} comparable runs for: {', '.join(LABELS[a] for a in short)}; "
                                                              "no comparison is claimed"}
    ranked = sorted(prim, key=lambda a: summary[a]["p95_ms"]["mean"])
    meaningful = []
    for key, res in pw.items():
        if key.endswith("static_b2") and key.startswith("static_b10"):
            continue                                   # interpretive pair, reported separately
        for m in ("p95_ms", "mean_ms", "throughput_tps", "failure_rate"):
            if res.get(m, {}).get("verdict", "").startswith("MEANINGFUL"):
                meaningful.append((key, m, res[m]["verdict"], res[m]["difference_of_means"]))
    best = ranked[0]
    if not meaningful:
        text = (f"No meaningful difference among Static (B=2), Reactive and Predictive on p95, mean latency, throughput or failures under the "
                f"pre-registered rule (SESOI + complete separation). Lowest p95 by point estimate: {LABELS[best]} "
                f"({summary[best]['p95_ms']['mean']:.1f} ms), which is not a demonstrated advantage.")
        status = "NO_MEANINGFUL_DIFFERENCE"
    else:
        text = "Meaningful differences (under the pre-registered rule): " + "; ".join(f"{k} on {m}: {v} ({d:+.3g})" for k, m, v, d in meaningful)
        status = "MEANINGFUL_DIFFERENCE_FOUND"
    if dec.get("all_primary_arms_held_B2"):
        text += " Every controller selected B=2 in every decision and applied no configuration change, so the three arms ran the same configuration."
    return {"status": status, "best_by_p95_point_estimate": best, "ranking_by_p95": ranked, "meaningful": meaningful, "text": text}


def to_markdown(header: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines) + "\n"
