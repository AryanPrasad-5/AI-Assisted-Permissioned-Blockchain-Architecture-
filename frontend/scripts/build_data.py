"""Build the static evidence files the research app reads.

Read-only with respect to research artifacts: every source is opened for
reading, checksummed, and copied (or summarised descriptively) into
frontend/public/data/. Nothing in the research pipeline is modified, no model
is loaded, no simulation is run. Aggregations are descriptive statistics of
observed columns or arithmetic on stored values.

The script fails (non-zero exit) if a stored checksum does not match or if a
figure recomputed from run-level records differs from the published table.

Usage (from the repo root):
    .venv/Scripts/python.exe -I frontend/scripts/build_data.py
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import statistics as st
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

FRONTEND = Path(__file__).resolve().parents[1]
REPO = FRONTEND.parent
OUT = FRONTEND / "public" / "data"

PB = Path("D:/PLAN_BLOCK18")
M2 = PB / "three_controller_comparison_2026-11-25"
M2_MEAS = M2 / "final_comparison_MEASURED"
V4 = PB / "results_discussion_v4_M2_2026-11-26" / "m2_addendum"
X1 = PB / "x1_block_size_headroom_2026-11-25" / "x1_decision_primary_cb01-06"
X0 = PB / "live_predictive_control_2026-11-25" / "x0_decision_20261125T1015Z"
CL = PB / "closed_loop_experiment_2026-11-22" / "final_results_2026-11-25" / "series_stats_v2_cl01-cl12"
V2T = PB / "results_discussion_v2_2026-11-25" / "assets" / "tables"
P1C = Path("D:/Plan_research/hfbtp_probe/outputs/phase1c")
E6 = Path("D:/Reseach_Model/outputs/verification/e5e6_decision_diag_2026-10-15")
HF = REPO / "data" / "raw" / "hfbtp" / "HFBTP.csv"
FB = REPO / "Flashbots"

HFBTP_SHA256 = "CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E"

SOURCES: dict[str, dict] = {}
FAILURES: list[str] = []


def label(path: Path) -> str:
    """Public, machine-independent name of a source file."""
    p = path.as_posix()
    for root, name in [(PB.as_posix(), "PLAN_BLOCK18"), (str(REPO.as_posix()), "repo"),
                       ("D:/Plan_research", "Plan_research"), ("D:/Reseach_Model", "Reseach_Model")]:
        if p.startswith(root):
            return name + p[len(root):]
    return path.name


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def src(path: Path) -> str:
    """Register a source file and return its public label."""
    name = label(path)
    if name not in SOURCES:
        SOURCES[name] = {"sha256": sha(path), "bytes": path.stat().st_size}
    return name


def read_csv(path: Path) -> list[dict]:
    src(path)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path):
    src(path)
    return json.loads(path.read_text(encoding="utf-8"))


def check(cond: bool, message: str) -> None:
    if not cond:
        FAILURES.append(message)


def close(a: float, b: float, tol: float) -> bool:
    return abs(a - b) <= tol


def num(v):
    """CSV cell -> int/float/bool/None/str, without inventing values."""
    if v is None or v == "":
        return None
    if v in ("True", "False"):
        return v == "True"
    try:
        f = float(v)
    except ValueError:
        return v
    if math.isfinite(f) and f == int(f) and "." not in v and "e" not in v.lower():
        return int(f)
    return f


def typed(rows: list[dict]) -> list[dict]:
    return [{k: num(v) for k, v in r.items()} for r in rows]


def verify_sums(folder: Path) -> list[dict]:
    """Check a SHA256SUMS.txt manifest (tolerates CRLF)."""
    listed = []
    manifest = next((folder / n for n in ("SHA256SUMS.txt",) if (folder / n).exists()), None)
    if manifest is None:
        return listed
    src(manifest)
    for line in manifest.read_text(encoding="utf-8-sig").splitlines():  # some manifests carry a BOM
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        expected, name = line.split(maxsplit=1)
        name = name.lstrip("*")
        target = folder / name
        ok = target.exists() and sha(target) == expected.lower()
        check(ok, f"checksum mismatch: {label(target)}")
        listed.append({"file": name, "sha256": expected.lower(), "verified": ok})
    return listed


def write(name: str, payload) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    (OUT / name).write_text(text, encoding="utf-8")


# --------------------------------------------------------------------- M2
def build_m2() -> dict:
    archive_sums = verify_sums(M2_MEAS)
    final = read_json(M2_MEAS / "final_comparison.json")
    runs = typed(read_csv(M2_MEAS / "run_records.csv"))
    registry = {r["run_id"]: r for r in typed(read_csv(V4 / "table12_m2_run_registry.csv"))}
    arms = typed(read_csv(V4 / "table10_m2_arm_summary.csv"))
    pairs = typed(read_csv(V4 / "table11_m2_pairwise.csv"))
    hyp = read_csv(V4 / "table14_m2_hypotheses.csv")
    decisions_tbl = read_csv(V4 / "table13_m2_decisions.csv")
    segments = typed(read_csv(M2_MEAS / "segments.csv"))
    forecast = typed(read_csv(M2_MEAS / "forecast_accuracy.csv"))
    offline = read_json(M2 / "offline_model_predictions_M2.json")

    # Recompute arm statistics from run-level records and compare with the paper.
    expected = {  # manuscript Table "M2 run-level results" (mean, sd)
        "static_b2": (42.9, 0.3, 82.3, 2.1), "reactive": (48.4, 6.9, 85.0, 7.8),
        "predictive_live": (44.1, 1.4, 81.3, 3.5), "static_b10": (201.6, 1.3, 451.0, 2.6)}
    by_arm = defaultdict(list)
    for r in runs:
        by_arm[r["arm_label"]].append(r)
    recomputed = {}
    for arm, rs in by_arm.items():
        mean = [r["mean_ms"] for r in rs]
        p95 = [r["p95_ms"] for r in rs]
        recomputed[arm] = {
            "runs": len(rs), "mean_ms": st.mean(mean), "mean_sd": st.stdev(mean),
            "p95_ms": st.mean(p95), "p95_sd": st.stdev(p95),
            "p99_ms": st.mean(r["p99_ms"] for r in rs),
            "throughput_tps": st.mean(r["throughput_tps"] for r in rs),
            "delivered_ratio": st.mean(r["delivered_ratio"] for r in rs),
            "success": sum(r["success"] for r in rs), "failed": sum(r["failed"] for r in rs),
            "config_changes": sum(r["config_changes"] for r in rs)}
        e = expected[arm]
        got = recomputed[arm]
        check(close(round(got["mean_ms"], 1), e[0], 1e-9) and close(round(got["mean_sd"], 1), e[1], 1e-9)
              and close(round(got["p95_ms"], 1), e[2], 1e-9) and close(round(got["p95_sd"], 1), e[3], 1e-9),
              f"M2 arm {arm} does not reproduce the manuscript table: {got}")
    check(sum(r["success"] for r in runs) == 190704, "M2 total transactions != 190,704")
    check(all(r["failed"] == 0 for r in runs), "M2 failed transactions present")
    check(final["primary"]["verdict"]["status"] == "NO_MEANINGFUL_DIFFERENCE", "M2 verdict changed")

    # Per-decision records, straight from each run's run_result.json.
    decisions = []
    for r in runs:
        rid = r["run_id"]
        path = M2 / "evidence" / rid / rid / "run_result.json"
        result = read_json(path)
        check(sha(path) == registry[rid]["run_result_sha256"], f"{rid} run_result.json hash differs from registry")
        obs = {o["round_index"]: o for o in result["control"]["observations"]}
        for d in result["control"]["decisions"]:
            meta = d.get("controller_metadata") or {}
            row = {"run_id": rid, "arm": r["arm_label"], "index": d["decision_index"],
                   "telemetry_round": d["telemetry_round"], "current_B": d["current_B"],
                   "target_B": d["target_B"], "changed": d["changed"], "abstained": d["abstained"],
                   "applied": d["applied"], "reason": d.get("reason")}
            o = obs.get(d["telemetry_round"])
            if o:
                row["observed_mean_ms"] = o["latency_mean_ms"]
                row["observed_p95_ms"] = o["latency_p95_ms"]
            if "observed_latency_ms" in meta:
                row["reactive"] = {"observed_latency_ms": meta["observed_latency_ms"],
                                   "high_ms": meta["latency_high_ms"], "low_ms": meta["latency_low_ms"],
                                   "congestion_streak": meta["congestion_streak"]}
            if "candidate_utilities" in meta:
                row["predictive"] = {
                    "forecast_tps": meta["forecast_tps"], "hold_cause": meta.get("hold_cause"),
                    "decision_kind": meta.get("decision_kind"),
                    "kappa_per_ms": meta["latency_penalty_per_ms"], "beta": meta["backlog_penalty_per_tps"],
                    "candidates": [{"B": c["config"]["max_message_count"], "latency_ms": c["latency_ms"],
                                    "throughput_tps": c["throughput_tps"], "utility": c["utility"]}
                                   for c in meta["candidate_utilities"]]}
            decisions.append(row)
    check(len(decisions) == 120, "expected 120 M2 decisions (12 runs x 10)")
    compared = [d for d in decisions if d["arm"] != "static_b10"]
    check(len(compared) == 90 and all(d["target_B"] == 2 and not d["changed"] for d in compared),
          "compared-arm decisions are not all hold at B=2")

    for r in runs:
        reg = registry[r["run_id"]]
        for k in ["independent_pass", "start_utc", "finish_utc", "run_result_sha256", "analysis_sha256",
                  "evidence_list_sha256", "decision_sha256", "independent_verification_sha256"]:
            r[k] = reg[k]

    return {
        "verdict": final["primary"]["verdict"], "rule": final.get("rule"), "protocol": final.get("protocol"),
        "runs": runs, "arms_table": arms, "arms_recomputed": recomputed, "pairwise": pairs,
        "decision_summary": decisions_tbl, "hypotheses": hyp, "segments": segments, "forecast": forecast,
        "decisions": decisions, "offline_prediction": offline, "archive_checksums": archive_sums,
        "sources": {"runs": label(M2_MEAS / "run_records.csv"), "arms": label(V4 / "table10_m2_arm_summary.csv"),
                    "pairwise": label(V4 / "table11_m2_pairwise.csv"),
                    "segments": label(M2_MEAS / "segments.csv"),
                    "forecast": label(M2_MEAS / "forecast_accuracy.csv"),
                    "decisions": "PLAN_BLOCK18/three_controller_comparison_2026-11-25/evidence/<run>/<run>/run_result.json",
                    "verdict": label(M2_MEAS / "final_comparison.json"),
                    "hypotheses": label(V4 / "table14_m2_hypotheses.csv"),
                    "offline_prediction": label(M2 / "offline_model_predictions_M2.json")},
    }


# --------------------------------------------------------------- X0 / X1 / CL / ca01
def build_live_series() -> dict:
    x1 = typed(read_csv(X1 / "rung_table.csv"))
    x1_decision = read_json(X1 / "x1_decision.json")
    x0_sums = verify_sums(X0)
    x0 = typed(read_csv(X0 / "rung_table.csv"))
    x0_decision = read_json(X0 / "x0_decision.json")
    cl_sums = verify_sums(CL)
    cl_segments = typed(read_csv(CL / "segments_all_runs.csv"))
    cl_arms = typed(read_csv(V2T / "T1_cl_arm_summary.csv"))
    cl_registry = typed(read_csv(V2T / "T3_cl_run_registry.csv"))
    hf_vs_x0 = typed(read_csv(V2T / "T8_hfbtp_observed_vs_x0.csv"))
    batching = typed(read_csv(V2T / "T6_x0_batching_relation_check.csv"))

    # Spot checks against the manuscript tables.
    cell = {(r["B"], r["rate_tps"]): r for r in x1}
    for (b, rate, p95) in [(2, 5.0, 223.5), (5, 5.0, 824.5), (10, 5.0, 1825.0), (2, 60.0, 42.0), (10, 60.0, 175.5)]:
        check(close(cell[(b, rate)]["p95_ms"], p95, 1e-9), f"X1 B={b} rate={rate} p95 differs from manuscript")
    check(all(cell[(2, r)]["p95_ms"] < min(cell[(5, r)]["p95_ms"], cell[(10, r)]["p95_ms"])
              for (_, r) in cell if True), "X1: B=2 is not lowest at every rung")
    x0cell = {(r["B"], r["rate_tps"]): r for r in x0}
    for (b, rate, p95) in [(10, 10.0, 930.0), (40, 10.0, 1945.0), (10, 60.0, 180.5), (40, 60.0, 667.0)]:
        check(close(x0cell[(b, rate)]["p95_ms"], p95, 1e-9), f"X0 B={b} rate={rate} p95 differs")
    check(len(cl_registry) == 11, "cl01-cl12 should list 11 comparable runs")

    ca01_md = M2 / "CA01_RESULT_2026-11-25.md"
    src(ca01_md)
    text = ca01_md.read_text(encoding="utf-8")
    ca01 = {
        "verdict": "PASS", "executed_utc": "2026-11-25 15:31:22 to 15:40:53",
        "rate_tps": 10, "transactions": 3600, "failed": 0,
        "transitions": [
            {"from": 2, "to": 5, "config_block": 304, "actuator_s": 1.75},
            {"from": 5, "to": 10, "config_block": 425, "actuator_s": 1.37},
            {"from": 10, "to": 5, "config_block": 486, "actuator_s": 1.16},
            {"from": 5, "to": 2, "config_block": 607, "actuator_s": 1.20}],
        "positive_control": {"B2_ms": 73.0, "B5_ms": "222.6-226.2", "B10_ms": 474.7,
                             "x1_cells_ms": {"2": 72.6, "5": 224.0, "10": 475.1}},
        "source": label(ca01_md)}
    for token in ["**PASS**", "| 304 |", "1.75 s", "| 607 |", "1.20 s", "3,600 of 3,600", "73.0 ms", "474.7 ms"]:
        check(token in text, f"ca01 record does not contain '{token}'")

    return {
        "x1": {"rungs": x1, "decision": {k: x1_decision.get(k) for k in ("decision", "status", "verdict", "reason",
                                                                          "outcome", "summary") if k in x1_decision},
               "source": label(X1 / "rung_table.csv")},
        "x0": {"rungs": x0, "decision": {k: x0_decision.get(k) for k in ("decision", "status", "verdict", "reason",
                                                                          "outcome", "r_max_tps") if k in x0_decision},
               "checksums": x0_sums, "source": label(X0 / "rung_table.csv")},
        "closed_loop": {"arms": cl_arms, "registry": cl_registry, "segments": cl_segments,
                        "checksums": cl_sums, "sources": {"arms": label(V2T / "T1_cl_arm_summary.csv"),
                                                          "registry": label(V2T / "T3_cl_run_registry.csv"),
                                                          "segments": label(CL / "segments_all_runs.csv")}},
        "hfbtp_vs_x0": {"rows": hf_vs_x0, "source": label(V2T / "T8_hfbtp_observed_vs_x0.csv")},
        "batching_check": {"rows": batching, "source": label(V2T / "T6_x0_batching_relation_check.csv"),
                           "constants_ms": {"10": 32, "40": 42}, "batch_timeout_s": 2},
        "ca01": ca01,
    }


# --------------------------------------------------------------------- HFBTP
def build_hfbtp() -> tuple[dict, dict]:
    digest = sha(HF).upper()
    check(digest == HFBTP_SHA256, "HFBTP.csv SHA-256 differs from the frozen value")
    src(HF)
    with HF.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = [h.strip() for h in next(reader)]
        rows = [[float(x) for x in r] for r in reader]
    check(len(rows) == 24687 and len(header) == 8, "HFBTP shape differs from 24,687 x 8")
    col = {h: [r[i] for r in rows] for i, h in enumerate(header)}

    def hist(values, start, end, width):
        n = math.ceil((end - start) / width)
        counts = [0] * n
        for v in values:
            counts[min(n - 1, max(0, int((v - start) / width + 1e-9)))] += 1
        return {"start": start, "width": width, "counts": counts}

    bins = {"Set Transaction Arrival Rate": (10, 205, 5), "Actual Transaction Arrival Rate": (0, 210, 10),
            "Block Size": (10, 810, 25), "Orderers": (3, 11, 2), "Throughput": (0, 210, 10),
            "Avg Latency": (0, 2.8, 0.1), "Min Latency": (0, 2.8, 0.1), "Max Latency": (0, 5.0, 0.1)}
    columns = []
    for h in header:
        v = col[h]
        columns.append({"name": h, "min": min(v), "max": max(v), "mean": st.mean(v), "sd": st.pstdev(v),
                        "distinct": len(set(v)), "missing": 0, "hist": hist(v, *bins.get(h, (min(v), max(v) + 1, 1)))})

    def pearson(a, b):
        ma, mb = st.mean(a), st.mean(b)
        num_ = sum((x - ma) * (y - mb) for x, y in zip(a, b))
        den = math.sqrt(sum((x - ma) ** 2 for x in a) * sum((y - mb) ** 2 for y in b))
        return num_ / den
    corr = [[round(pearson(col[a], col[b]), 4) for b in header] for a in header]

    present = {(r[0], r[2], r[3]) for r in rows}
    levels = sorted(set(col["Set Transaction Arrival Rate"]))
    blocks = sorted(set(col["Block Size"]))
    orders = sorted(set(col["Orderers"]))
    absent = [(s, b, o) for s in levels for b in blocks for o in orders if (s, b, o) not in present]
    per_level = Counter(a[0] for a in absent)
    check(len(absent) == 117 and set(per_level.values()) == {3}, "HFBTP absent-cell pattern changed")
    absent_slots = sorted({(b, o) for (_, b, o) in absent})
    check(len(rows) == len(present), "HFBTP has duplicate configuration cells")

    i_set, i_blk, i_ord = header.index("Set Transaction Arrival Rate"), header.index("Block Size"), header.index("Orderers")
    i_thr, i_lat = header.index("Throughput"), header.index("Avg Latency")
    grid = {"columns": ["set", "block", "orderers", "throughput", "avg_latency"],
            "rows": [[r[i_set], r[i_blk], int(r[i_ord]), r[i_thr], r[i_lat]] for r in rows],
            "source": label(HF), "sha256": digest,
            "note": "Observed HFBTP cells (Apache-2.0, Kaggle loveffc/hfbtp-a-blockchain-performance-dataset). "
                    "One observation per cell; no replicates or timestamps."}
    summary = {"rows": len(rows), "columns": columns, "header": header, "correlation": corr,
               "levels": levels, "blocks": [blocks[0], blocks[-1], blocks[1] - blocks[0], len(blocks)],
               "orderers": [int(o) for o in orders], "absent_count": len(absent),
               "absent_slots": [{"block": b, "orderers": int(o)} for b, o in absent_slots],
               "sha256": digest, "source": label(HF)}
    return summary, grid


def build_models() -> dict:
    test = typed(read_csv(P1C / "test_results.csv"))
    per_level = typed(read_csv(P1C / "per_level_errors.csv"))
    report = P1C / "PHASE_1C_REPORT.md"
    src(report)
    selected = {}
    for r in test:
        key = (r["manifest"], r["target"])
        if r["val_mae"] is None:
            continue
        if key not in selected or r["val_mae"] < selected[key]["val_mae"]:
            selected[key] = r
    expected = {("T1", "Throughput"): 1.27, ("T1b", "Throughput"): 1.22, ("T2D0", "Throughput"): 3.27,
                ("T3", "Throughput"): 1.53, ("HEAVY", "Throughput"): 34.79,
                ("T1", "Avg Latency"): 0.0172, ("T2D0", "Avg Latency"): 0.0234}
    for key, value in expected.items():
        got = selected[key]["test_mae"]
        check(close(round(got, 2 if value > 1 else 4), value, 1e-9), f"Phase 1C selected {key} test MAE {got} != {value}")
    e6 = {"decisions": typed(read_csv(E6 / "E6_decisions_by_level.csv")),
          "static_variants": typed(read_csv(E6 / "E6_static_variants.csv")),
          "spearman": typed(read_csv(E6 / "E6_ranking_spearman.csv")),
          "menu": typed(read_csv(E6 / "E6_menu_reference.csv")),
          "report": label(E6 / "E5_E6_REPORT.md") if (E6 / "E5_E6_REPORT.md").exists() and src(E6 / "E5_E6_REPORT.md") else None}
    baselines = typed(read_csv(REPO / "experiments" / "results" / "hfbtp_baseline_metrics.csv"))
    return {"phase1c": {"test": test, "selected": [{"manifest": k[0], "target": k[1], "family": v["family"],
                                                    "config": v["config"], "val_mae": v["val_mae"]}
                                                   for k, v in selected.items()],
                        "per_level": per_level, "source": label(P1C / "test_results.csv"),
                        "report": label(report)},
            "e6": e6,
            "repo_baselines": {"rows": baselines,
                               "source": label(REPO / "experiments" / "results" / "hfbtp_baseline_metrics.csv"),
                               "note": "Separate, smaller ladder (Dummy / MLR with 8 engineered features / RF). "
                                       "Not comparable with Phase 1C numbers."}}


# ----------------------------------------------------------------- Flashbots
def build_flashbots() -> tuple[dict, dict]:
    comparison = typed(read_csv(FB / "evaluation" / "FB-FC-v1" / "model_comparison_F.csv"))
    dm = typed(read_csv(FB / "results" / "FB-FC-v1" / "final" / "dm_F.csv"))
    eda = read_json(FB / "eda" / "eda_summary.json")
    days = {}
    for day in ["2026-09-21", "2026-10-12"]:
        path = FB / "time_series" / f"target_series_{day}_1s.csv.gz"
        src(path)
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            counts = [int(r["arrival_count"]) for r in csv.DictReader(handle)]
        check(len(counts) == 86400, f"{day}: series is not 86,400 seconds")
        days[day] = {"transactions": sum(counts), "per_minute_tps": [round(sum(counts[i:i + 60]) / 60, 4)
                                                                      for i in range(0, 86400, 60)],
                     "max_1s": max(counts), "source": label(path)}
        check(days[day]["transactions"] == eda["days"][day]["transactions"], f"{day}: count differs from EDA")
    check(days["2026-09-21"]["transactions"] + days["2026-10-12"]["transactions"] == 1969790,
          "Flashbots total differs from 1,969,790")

    forecasts = {}
    for horizon in [10, 60, 300]:
        path = FB / "results" / "FB-FC-v1" / "final" / f"forecasts_F_arrival_rate_H{horizon}.csv"
        rows = read_csv(path)
        models = [k[5:] for k in rows[0] if k.startswith("yhat_")]
        forecasts[str(horizon)] = {
            "origin_s": [int(r["origin_s"]) for r in rows],
            "actual": [round(float(r["y_observed"]), 4) for r in rows],
            "models": {m: [round(float(r["yhat_" + m]), 4) if r["yhat_" + m] != "" else None for r in rows]
                       for m in models},
            "source": label(path)}
    summary = {"comparison": [r for r in comparison if r["target"] in ("arrival_rate", "gas_demand_rate")],
               "dm": dm, "days": {d: {k: v for k, v in days[d].items()} for d in days},
               "eda": {d: {k: eda["days"][d][k] for k in ("transactions", "mean_tx_per_s", "sd_1s", "median_1s",
                                                          "p95_1s", "p99_1s", "max_1s", "zero_seconds",
                                                          "acf_1s_lag1", "acf_60s_lag1", "periodicity_12s")
                           if k in eda["days"][d]} for d in eda["days"]},
               "m2_window": {"day": "2026-10-12", "start_s": 20400, "minutes": 10, "k": 2.114},
               "sources": {"comparison": label(FB / "evaluation" / "FB-FC-v1" / "model_comparison_F.csv"),
                           "dm": label(FB / "results" / "FB-FC-v1" / "final" / "dm_F.csv"),
                           "eda": label(FB / "eda" / "eda_summary.json")}}
    return summary, forecasts


PAPER = PB / "FINAL_RESEARCH_PAPER_2026"


def build_references() -> dict:
    """Reference list of manuscript v1.0, parsed from its .bib (no network lookups)."""
    import re
    bib = PAPER / "manuscript" / "references.bib"
    src(bib)
    text = bib.read_text(encoding="utf-8")
    refs = []
    for kind, key, body in re.findall(r"@(\w+)\{([^,]+),(.*?)\n\}", text, re.S):
        if kind.lower() == "ieeetranbstctl":
            continue
        fields = {k.lower(): re.sub(r"[{}]|\\url", "", v).replace("\\&", "&").strip()
                  for k, v in re.findall(r"(\w+)\s*=\s*[{\"](.*?)[}\"]\s*,?\s*\n", body + "\n", re.S)}
        authors = [a.strip() for a in fields.get("author", "").split(" and ") if a.strip()]
        names = [a.split(",")[0] if "," in a else a.split()[-1] for a in authors]
        who = names[0] + (" et al." if len(names) > 3 else "".join(f", {n}" for n in names[1:-1]) + (f" and {names[-1]}" if len(names) > 1 else ""))
        venue = fields.get("journal") or fields.get("booktitle") or fields.get("howpublished") or fields.get("publisher") or ""
        refs.append({"key": key, "authors": who, "title": fields.get("title", ""), "venue": venue,
                     "year": fields.get("year", ""), "doi": fields.get("doi", ""), "url": fields.get("url", "")})
    check(len(refs) == 40, f"expected 40 references, found {len(refs)}")
    return {"references": refs, "source": label(bib),
            "paper_pdf": label(PAPER / "manuscript" / "main_word_render.pdf"),
            "manuscript_version": "1.0 (2026-11-26)"}


def main() -> int:
    m2 = build_m2()
    live = build_live_series()
    hf_summary, hf_grid = build_hfbtp()
    models = build_models()
    fb_summary, fb_forecasts = build_flashbots()
    refs = build_references()
    if FAILURES:
        print("VERIFICATION FAILED:")
        for f in FAILURES:
            print("  -", f)
        return 1
    outputs = {"m2.json": m2, "live_series.json": live, "hfbtp_summary.json": hf_summary,
               "hfbtp_grid.json": hf_grid, "models.json": models, "flashbots.json": fb_summary,
               "forecasts.json": fb_forecasts, "references.json": refs}
    for name, payload in outputs.items():
        write(name, payload)
    manifest = {"generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "generator": "frontend/scripts/build_data.py",
                "checks_passed": True,
                "sources": SOURCES,
                "outputs": {n: hashlib.sha256((OUT / n).read_bytes()).hexdigest() for n in outputs}}
    write("provenance.json", manifest)
    print(f"OK: {len(SOURCES)} sources read, {len(outputs)} files written to {OUT}")
    for n in outputs:
        print(f"  {n:22s} {(OUT / n).stat().st_size / 1024:8.1f} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
