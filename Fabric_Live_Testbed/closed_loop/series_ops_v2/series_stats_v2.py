"""Series statistics v2 (S5-A1, written 2026-11-22 BEFORE cl02 ran; hash bound in S5_AMENDMENT_A1_2026-11-22.md).
Read-only on the evidence; writes CSV/JSON/PNG into a NEW folder.

Inclusion (primary): a run is COMPARABLE when its S5-A1 decision file says CONTINUE (recorded verdict VALID or
VALID_WITH_FLAGS with only the permitted round-6 DISPATCH_LAG flag, every other independent check passing). Runs that
are not comparable are listed in runs_all.csv and excluded from the arm statistics. Recorded verdicts are never changed.

Scenarios (all reported; FULL is primary):
  FULL                  every measured segment of every comparable run
  EXCL_FLAGGED_SEGMENTS drop only the run-segments whose dispatch-lag p99 > 100 ms
  EXCL_ROUND6_ALL       drop round 6 (the burst segment) from EVERY run, all arms alike
  EXCL_FLAGGED_RUNS     drop every run with any recorded flag
Unit of analysis = run (n=3 per arm when complete). Run-level latency statistics are recomputed from the per-transaction
recorder rows (nearest rank). Throughput = successes / sum of round wall-clock durations over the included segments.
Differences vs Static: Welch t interval and exact two-sided permutation p over run labels (descriptive; with 3 vs 3 the
smallest attainable p is 0.10, so no significance claim is possible).

Usage: python series_stats_v2.py <evidence root: D: layout clNN/{clNN, caliper_run_workspace/clNN, clNN_verification}>
                                 <decision dir with clNN.json> <stage dir> <new out dir>
"""
import csv
import glob
import hashlib
import itertools
import json
import math
import statistics as st
import sys
from pathlib import Path

from scipy import stats

ROOT, DEC, STAGE, OUTD = (Path(a) for a in sys.argv[1:5])
OUTD.mkdir(parents=True, exist_ok=False)
ARMS = ["static", "reactive-R1", "predictive", "reactive-R0"]
FLAG_P99 = 100.0
VERDICT_CHECKS = {"runner verdict exactly VALID", "analysis verdict exactly VALID"}


def nr(v, q):
    v = sorted(v)
    return v[max(0, math.ceil(q * len(v)) - 1)] if v else None


def wcsv(name, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with (OUTD / name).open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


runs, per_tx, seg_rows, lag_rows, act_rows = [], {}, [], [], []
for rd in sorted(p for p in ROOT.glob("cl[0-9][0-9]") if p.is_dir()):
    rid = rd.name
    arm = json.loads((STAGE / rid / "arm.json").read_text(encoding="utf-8"))
    rr = json.loads((rd / rid / "run_result.json").read_text(encoding="utf-8"))
    an = json.loads((rd / rid / "analysis.json").read_text(encoding="utf-8"))
    iv = json.loads((rd / f"{rid}_verification" / "independent_verification.json").read_text(encoding="utf-8"))
    decp = DEC / f"{rid}.json"
    dec = json.loads(decp.read_text(encoding="utf-8")) if decp.exists() else {}
    ws = rd / "caliper_run_workspace" / rid
    failing = sorted(c["check"] for c in iv["checks"] if not c["pass"])
    lags_by_round = {}
    for r in arm["rounds"]:
        i, lab = r["index"], r["label"]
        rows = [x for f in sorted(glob.glob(str(ws / "recorder" / f"*_{lab}.csv"))) for x in csv.DictReader(open(f, encoding="utf-8"))]
        lat = [int(x["final_ms"]) - int(x["create_ms"]) for x in rows if x["status"] == "success"]
        man = json.loads(Path(glob.glob(str(ws / "recorder" / f"*_{lab}.manifest.json"))[0]).read_text(encoding="utf-8"))["clock"]
        wall = (man["wall1_ms"] - man["wall0_ms"]) / 1000
        dl = [float(x["lag_ms"]) for x in csv.DictReader(open(ws / f"dispatch_r{i:02d}.csv", encoding="utf-8"))]
        ivr = next(x for x in iv["rounds_independent"] if x["round"] == i)
        B = ivr["committed_under_B"][0] if len(ivr["committed_under_B"]) == 1 else "/".join(map(str, ivr["committed_under_B"]))
        per_tx[(rid, i)] = {"lat": lat, "wall": wall, "measured": r["measured"], "B": B, "lag_p99": nr(dl, 0.99), "failed": sum(1 for x in rows if x["status"] != "success")}
        lags_by_round[i] = nr(dl, 0.99)
        seg_rows.append({"run_id": rid, "arm": arm["arm_label"], "round": i, "label": lab, "measured": r["measured"], "B": B,
                         "scheduled": r["n_tx"], "success": len(lat), "failed": per_tx[(rid, i)]["failed"], "wall_s": round(wall, 3),
                         "offered_tps": r["n_tx"] / (r["length_ms"] / 1000), "throughput_tps": len(lat) / wall if wall else None,
                         "lat_mean_ms": st.fmean(lat) if lat else None, "lat_p50_ms": nr(lat, .5), "lat_p95_ms": nr(lat, .95),
                         "lat_p99_ms": nr(lat, .99), "dispatch_lag_p99_ms": nr(dl, .99), "dispatch_flagged": nr(dl, .99) > FLAG_P99})
        lag_rows.append({"run_id": rid, "arm": arm["arm_label"], "round": i, "B": B, "n": len(dl), "lag_mean_ms": st.fmean(dl),
                         "lag_p50_ms": nr(dl, .5), "lag_p95_ms": nr(dl, .95), "lag_p99_ms": nr(dl, .99), "lag_max_ms": max(dl),
                         "n_over_50ms": sum(x > 50 for x in dl), "n_over_100ms": sum(x > 100 for x in dl)})
    for t in rr["control"]["transitions"]:
        act_rows.append({"run_id": rid, "arm": arm["arm_label"], "decision": t["decision_index"], "from_B": t["from_B"], "to_B": t["to_B"],
                         "success": t.get("success"), "ledger_config_block": t.get("ledger_config_block"), "actuator_ms": t.get("duration_ms"),
                         "commit_latency_ms": t.get("commit_latency_ms"), "total_adaptation_latency_ms": t.get("total_adaptation_latency_ms")})
    decs = rr["control"]["decisions"]
    runs.append({"run_id": rid, "arm": arm["arm_label"], "order": arm["order_position"], "channel": arm["channel"],
                 "recorded_runner_verdict": rr["verdict"], "recorded_analysis_verdict": an["verdict"], "recorded_flags": ";".join(an["flags"]),
                 "s5a1_decision": dec.get("decision", "NONE"), "independent_failing_checks": ";".join(failing),
                 "independent_pass_except_verdict": set(failing) <= VERDICT_CHECKS,
                 "comparable": dec.get("decision") == "CONTINUE" and set(failing) <= VERDICT_CHECKS,
                 "decisions_B40": sum(1 for d in decs if d["target_B"] == 40), "abstentions": sum(1 for d in decs if d.get("abstained")),
                 "transitions_applied": sum(1 for t in rr["control"]["transitions"] if t.get("success")),
                 "B_trajectory": "".join("H" if per_tx[(rid, r["index"])]["B"] == 40 else ("L" if per_tx[(rid, r["index"])]["B"] == 10 else "M")
                                         for r in arm["rounds"] if r["measured"]),
                 "final_B": rr["final_config"]["max_message_count"], "max_round_dispatch_p99_ms": max(lags_by_round.values()),
                 "caliper_s": rr["caliper"]["duration_s"]})
comp = [r for r in runs if r["comparable"]]


def run_level(rid, keep):
    lat, wall, n_fail = [], 0.0, 0
    for (r_, i), v in per_tx.items():
        if r_ == rid and v["measured"] and keep(rid, i, v):
            lat += v["lat"]
            wall += v["wall"]
            n_fail += v["failed"]
    return {"n": len(lat), "throughput_tps": len(lat) / wall if wall else None, "lat_mean_ms": st.fmean(lat) if lat else None,
            "lat_p50_ms": nr(lat, .5), "lat_p95_ms": nr(lat, .95), "lat_p99_ms": nr(lat, .99), "failed": n_fail}


SCEN = {
    "FULL": (lambda r: True, lambda rid, i, v: True),
    "EXCL_FLAGGED_SEGMENTS": (lambda r: True, lambda rid, i, v: v["lag_p99"] <= FLAG_P99),
    "EXCL_ROUND6_ALL": (lambda r: True, lambda rid, i, v: i != 6),
    "EXCL_FLAGGED_RUNS": (lambda r: not r["recorded_flags"], lambda rid, i, v: True),
}
METRICS = ["throughput_tps", "lat_mean_ms", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "failed"]
run_level_rows, summ, diffs = [], [], []
for sc, (run_ok, seg_ok) in SCEN.items():
    rows = []
    for r in comp:
        if run_ok(r):
            rl = run_level(r["run_id"], seg_ok)
            rows.append({"scenario": sc, "run_id": r["run_id"], "arm": r["arm"], **rl})
    run_level_rows += rows
    for arm in ARMS:
        xs = [x for x in rows if x["arm"] == arm]
        if not xs:
            continue
        e = {"scenario": sc, "arm": arm, "runs": len(xs), "run_ids": " ".join(x["run_id"] for x in xs)}
        for m in METRICS:
            vals = [float(x[m]) for x in xs]
            mu = st.fmean(vals)
            sd = st.stdev(vals) if len(vals) > 1 else float("nan")
            half = stats.t.ppf(0.975, len(vals) - 1) * sd / len(vals) ** 0.5 if len(vals) > 1 else float("nan")
            e.update({f"{m}_mean": mu, f"{m}_sd": sd, f"{m}_cv_pct": 100 * sd / mu if mu else float("nan"),
                      f"{m}_ci95_lo": mu - half, f"{m}_ci95_hi": mu + half, f"{m}_min": min(vals), f"{m}_max": max(vals)})
        summ.append(e)
    base = [x for x in rows if x["arm"] == "static"]
    for arm in ARMS[1:]:
        xs = [x for x in rows if x["arm"] == arm]
        if len(xs) < 2 or len(base) < 2:
            continue
        for m in ["throughput_tps", "lat_mean_ms", "lat_p95_ms"]:
            a, b = [float(x[m]) for x in xs], [float(x[m]) for x in base]
            d = st.fmean(a) - st.fmean(b)
            va, vb = st.variance(a) / len(a), st.variance(b) / len(b)
            se = (va + vb) ** 0.5
            df = (va + vb) ** 2 / (va ** 2 / (len(a) - 1) + vb ** 2 / (len(b) - 1)) if se else float("nan")
            half = stats.t.ppf(0.975, df) * se if se else float("nan")
            pooled, obs, cnt, tot = a + b, abs(d), 0, 0
            for idx in itertools.combinations(range(len(pooled)), len(a)):
                g1 = [pooled[i] for i in idx]
                g2 = [pooled[i] for i in range(len(pooled)) if i not in idx]
                tot += 1
                cnt += abs(st.fmean(g1) - st.fmean(g2)) >= obs - 1e-9
            diffs.append({"scenario": sc, "arm_minus_static": arm, "metric": m, "diff": d, "pct_of_static": 100 * d / st.fmean(b),
                          "welch_se": se, "welch_df": df, "ci95_lo": d - half, "ci95_hi": d + half, "perm_p_two_sided": cnt / tot,
                          "perm_min_attainable_p": 2 / tot, "n_arm": len(a), "n_static": len(b)})

# per-segment, arm-by-arm (same frozen workload per segment for every arm)
segcmp = []
for rnd in sorted({s["round"] for s in seg_rows if s["measured"]}):
    row = {"round": rnd}
    for arm in ARMS:
        ss = [s for s in seg_rows if s["round"] == rnd and s["arm"] == arm and s["run_id"] in {c["run_id"] for c in comp}]
        if ss:
            row[f"{arm}_lat_mean_ms"] = st.fmean(s["lat_mean_ms"] for s in ss)
            row[f"{arm}_lat_mean_range_ms"] = max(s["lat_mean_ms"] for s in ss) - min(s["lat_mean_ms"] for s in ss)
            row[f"{arm}_p95_mean_ms"] = st.fmean(s["lat_p95_ms"] for s in ss)
            row[f"{arm}_thr_mean_tps"] = st.fmean(s["throughput_tps"] for s in ss)
            row[f"{arm}_B"] = "/".join(str(s["B"]) for s in ss)
            row[f"{arm}_dispatch_p99_max_ms"] = max(s["dispatch_lag_p99_ms"] for s in ss)
    segcmp.append(row)

wcsv("runs_all.csv", runs)
wcsv("run_level_by_scenario.csv", run_level_rows)
wcsv("arm_summary_by_scenario.csv", summ)
wcsv("differences_vs_static_by_scenario.csv", diffs)
wcsv("segments_all_runs.csv", seg_rows)
wcsv("segment_comparison_by_arm.csv", segcmp)
wcsv("dispatch_fidelity_by_run_round.csv", lag_rows)
wcsv("config_updates.csv", act_rows)

import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
COL = {"static": "#2a78d6", "reactive-R1": "#eb6834", "predictive": "#1baf7a", "reactive-R0": "#a86f00"}
MK = {"static": "o", "reactive-R1": "s", "predictive": "^", "reactive-R0": "D"}
ink2, grid = "#52514e", "#e4e3df"


def style(ax):
    for s_ in ("top", "right"):
        ax.spines[s_].set_visible(False)
    for s_ in ("left", "bottom"):
        ax.spines[s_].set_color(grid)
    ax.tick_params(colors=ink2, labelsize=9)
    ax.grid(axis="y", color=grid, linewidth=0.8)
    ax.set_axisbelow(True)


full = [x for x in run_level_rows if x["scenario"] == "FULL"]
arms_present = [a for a in ARMS if any(x["arm"] == a for x in full)]
fig, axes = plt.subplots(1, 3, figsize=(11, 3.6), dpi=150)
for ax, (m, t) in zip(axes, [("throughput_tps", "Throughput (tx/s)"), ("lat_mean_ms", "Mean latency (ms)"), ("lat_p95_ms", "p95 latency (ms)")]):
    for i, a in enumerate(arms_present):
        vals = [x[m] for x in full if x["arm"] == a]
        ax.scatter([i + (j - (len(vals) - 1) / 2) * 0.09 for j in range(len(vals))], vals, color=COL[a], marker=MK[a], s=42, zorder=3,
                   edgecolors="white", linewidths=1.2)
        ax.hlines(st.fmean(vals), i - 0.25, i + 0.25, color=COL[a], linewidth=2)
    ax.set_xticks(range(len(arms_present)), [a.replace("reactive-", "R-") for a in arms_present])
    ax.set_title(t, loc="left", fontsize=10)
    style(ax)
fig.suptitle("Run-level measured results (FULL scenario; dots = runs, bar = arm mean)", x=0.01, ha="left", fontsize=10)
fig.tight_layout()
fig.savefig(OUTD / "fig_run_level_by_arm.png")
plt.close(fig)

fig, ax = plt.subplots(figsize=(9, 4.2), dpi=150)
seen = set()
for r in comp:
    pts = sorted((s["round"], s["lat_mean_ms"], s["B"]) for s in seg_rows if s["run_id"] == r["run_id"] and s["measured"])
    ax.plot([p[0] for p in pts], [p[1] for p in pts], color=COL[r["arm"]], linewidth=1.4, alpha=0.85, label=None if r["arm"] in seen else r["arm"])
    for p in pts:
        if p[2] == 40:
            ax.scatter([p[0]], [p[1]], color=COL[r["arm"]], marker="D", s=24, zorder=3)
    seen.add(r["arm"])
ax.axvspan(5.5, 6.5, color="#f3e6c4", zorder=0)
ax.text(6, ax.get_ylim()[1], "round 6 (burst)", ha="center", va="top", fontsize=8, color=ink2)
ax.set_xlabel("Measured round (60 s of F_82800 each). Diamonds = committed at B=40", color=ink2)
ax.set_ylabel("Mean latency (ms)", color=ink2)
ax.set_title("Per-segment mean latency, every comparable run", loc="left", fontsize=10)
style(ax)
ax.legend(frameon=False, fontsize=9)
fig.tight_layout()
fig.savefig(OUTD / "fig_segment_latency_all_runs.png")
plt.close(fig)

fig, ax = plt.subplots(figsize=(9, 0.45 * len(runs) + 1.4), dpi=150)
for i, r in enumerate(runs):
    for j, ch in enumerate(r["B_trajectory"]):
        ax.add_patch(plt.Rectangle((j + 0.5, i - 0.4), 1, 0.8, color="#a86f00" if ch == "H" else "#cfe0f5"))
ax.set_xlim(0.5, 10.5)
ax.set_ylim(-0.6, len(runs) - 0.4)
ax.set_yticks(range(len(runs)), [r["run_id"] + " " + r["arm"].replace("reactive-", "R-") for r in runs], fontsize=8)
ax.set_xticks(range(1, 11))
ax.set_xlabel("Measured round (dark = committed at B=40, light = B=10; from ledger config blocks)", color=ink2)
ax.set_title("Live configuration in force per segment", loc="left", fontsize=10)
for s_ in ("top", "right"):
    ax.spines[s_].set_visible(False)
fig.tight_layout()
fig.savefig(OUTD / "fig_configuration_timeline.png")
plt.close(fig)

fig, ax = plt.subplots(figsize=(9, 3.8), dpi=150)
seen = set()
for r in runs:
    pts = sorted((x["round"], x["lag_p99_ms"]) for x in lag_rows if x["run_id"] == r["run_id"])
    ax.plot([p[0] for p in pts], [p[1] for p in pts], color=COL[r["arm"]], marker=MK[r["arm"]], markersize=4, linewidth=1.1,
            alpha=0.85, label=None if r["arm"] in seen else r["arm"])
    seen.add(r["arm"])
ax.axhline(FLAG_P99, color="#c43c3c", linewidth=1, linestyle="--")
ax.text(0, FLAG_P99 + 3, "pre-registered 100 ms fidelity flag", fontsize=8, color="#c43c3c")
ax.set_xlabel("Caliper round (0 = pre-roll)", color=ink2)
ax.set_ylabel("Client dispatch lag p99 (ms)", color=ink2)
ax.set_title("Client dispatch fidelity per round, every run", loc="left", fontsize=10)
style(ax)
ax.legend(frameon=False, fontsize=9)
fig.tight_layout()
fig.savefig(OUTD / "fig_dispatch_lag_p99.png")
plt.close(fig)

(OUTD / "inputs.json").write_text(json.dumps({"script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                              "argv": sys.argv[1:], "runs": [r["run_id"] for r in runs],
                                              "comparable": [r["run_id"] for r in comp]}, indent=1), encoding="utf-8")
sums = "".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in sorted(OUTD.iterdir()) if p.is_file())
(OUTD / "SHA256SUMS.txt").write_text(sums, encoding="ascii", newline="\n")
print(json.dumps({"runs": len(runs), "comparable": len(comp), "summary": [{k: v for k, v in e.items() if k.endswith("_mean") or k in ("scenario", "arm", "runs")} for e in summ]}, indent=1, default=str)[:8000])
