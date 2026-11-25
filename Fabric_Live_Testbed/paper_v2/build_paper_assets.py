"""Tables and figures for Results and Discussion v2 (cl01-cl12 series + X0 crossover scan + stored offline results).

Reads only existing, immutable evidence and writes into a NEW output directory (refuses to overwrite). No Fabric/Docker/Caliper
access, no model fitting, no simulation. The only computation is descriptive: per-arm means, ratios, and a one-parameter-per-B
consistency check of the batching relation (disclosed as such in the paper text).

Run (Windows venv, repo root):
  .venv/Scripts/python.exe -B Fabric_Live_Testbed/paper_v2/build_paper_assets.py --out D:/PLAN_BLOCK18/results_discussion_v2_2026-11-25/assets
"""
from __future__ import annotations

import argparse
import hashlib
import json
import statistics as st
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

PLAN = Path("D:/PLAN_BLOCK18")
CL_DIR = PLAN / "closed_loop_experiment_2026-11-22/final_results_2026-11-25/series_stats_v2_cl01-cl12"
LPC = PLAN / "live_predictive_control_2026-11-25"
X0_STAGE = LPC / "stage_x0"
X0_EVIDENCE = LPC / "evidence"
X0_DECISION = LPC / "x0_decision_20261125T1015Z/x0_decision.json"
FB_SCORES = REPO / "Flashbots/results/FB-FC-v1/final/scores_F.csv"
FB_FINAL = REPO / "Flashbots/results/FB-FC-v1/final/final_report.json"

# Categorical slots 1-4 of the validated reference palette (validate_palette.js: adjacent CVD dE 9.1, normal-vision dE 22.9).
# Slots 3 and 4 are below 3:1 contrast on white, so every series is also direct-labelled and has its own marker shape.
C_BLUE, C_ORANGE, C_AQUA, C_YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, INK2, GRID = "#1a1a19", "#52514e", "#e4e3df"
ARMS = [  # key in the csv, display label, colour, marker
    ("static", "Static B=10", C_BLUE, "o"),
    ("reactive-R1", "Reactive R1\n(2000/1600 ms)", C_ORANGE, "s"),
    ("predictive", "Predictive\n(stored decisions)", C_AQUA, "^"),
    ("reactive-R0", "Reactive R0\n(500/200 ms)", C_YELLOW, "D"),
]
ARM_LABEL = {k: lab.replace("\n", " ") for k, lab, _, _ in ARMS}


def sha256(p: Path) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def style():
    plt.rcParams.update({
        "figure.facecolor": "white", "axes.facecolor": "white", "savefig.facecolor": "white", "font.size": 9,
        "font.family": "DejaVu Sans", "text.color": INK, "axes.labelcolor": INK2, "axes.edgecolor": "#b9b8b2",
        "xtick.color": INK2, "ytick.color": INK2, "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True, "legend.frameon": False,
        "axes.titlesize": 10, "axes.titleweight": "regular", "axes.titlelocation": "left", "lines.linewidth": 1.6,
    })


def save(fig, out: Path, name: str):
    for ext in ("png", "pdf"):
        fig.savefig(out / "figures" / f"{name}.{ext}", dpi=300, bbox_inches="tight")
    plt.close(fig)


def md_table(df: pd.DataFrame, floatfmt="{:.1f}") -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, r in df.iterrows():
        cells = []
        for c in cols:
            v = r[c]
            cells.append(floatfmt.format(v) if isinstance(v, (float, np.floating)) and not pd.isna(v) else ("" if pd.isna(v) else str(v)))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


# ---------------------------------------------------------------- cl01-cl12
def cl_tables(out: Path, md: list):
    runs = pd.read_csv(CL_DIR / "runs_all.csv")
    rl = pd.read_csv(CL_DIR / "run_level_by_scenario.csv")
    rl = rl[rl.scenario == "FULL"].copy()
    rows = []
    for key, label, _, _ in ARMS:
        d = rl[rl.arm == key]
        a = runs[runs.arm == key]
        rows.append({"arm": ARM_LABEL[key], "runs": len(d), "run_ids": " ".join(d.run_id), "throughput_tps": d.throughput_tps.mean(),
                     "mean_latency_ms": d.lat_mean_ms.mean(), "p95_latency_ms": d.lat_p95_ms.mean(), "p99_latency_ms": d.lat_p99_ms.mean(),
                     "failed_tx": int(d.failed.sum()), "config_changes_per_run": a.transitions_applied.mean(),
                     "decisions_to_B40_per_run": a.decisions_B40.mean(), "abstentions_per_run": a.abstentions.mean()})
    t1 = pd.DataFrame(rows)
    t1.to_csv(out / "tables/T1_cl_arm_summary.csv", index=False)
    diff = pd.read_csv(CL_DIR / "differences_vs_static_by_scenario.csv")
    t2 = diff[(diff.scenario == "FULL") & diff.metric.isin(["throughput_tps", "lat_mean_ms", "lat_p95_ms"])].copy()
    t2.to_csv(out / "tables/T2_cl_difference_vs_static.csv", index=False)
    t3 = runs[["run_id", "arm", "channel", "recorded_runner_verdict", "recorded_flags", "decisions_B40", "abstentions", "transitions_applied",
               "B_trajectory", "max_round_dispatch_p99_ms"]].copy()
    t3.to_csv(out / "tables/T3_cl_run_registry.csv", index=False)
    md.append("### Table 1. Series cl01-cl12: run-level arm means (FULL scenario, 10 measured segments)\n\n" + md_table(t1.drop(columns=["run_ids"]), "{:.2f}"))
    md.append("### Table 2. Difference from Static B=10 (descriptive; n = 3 vs 3, or 2 for R0)\n\n" + md_table(
        t2[["arm_minus_static", "metric", "diff", "pct_of_static", "ci95_lo", "ci95_hi", "perm_min_attainable_p"]], "{:.3f}"))
    md.append("### Table 3. cl run registry\n\n" + md_table(t3, "{:.0f}"))
    return runs, rl, t1


def fig_cl_runlevel(out: Path, rl: pd.DataFrame):
    fig, axes = plt.subplots(1, 3, figsize=(9.2, 3.3))
    metrics = [("throughput_tps", "Throughput (tx/s)", 12.0), ("lat_mean_ms", "Mean latency (ms)", 900.0), ("lat_p95_ms", "p95 latency (ms)", 2300.0)]
    for ax, (m, title, ymax) in zip(axes, metrics):
        for i, (key, _, col, mk) in enumerate(ARMS):
            d = rl[rl.arm == key][m].values
            jit = np.linspace(-0.12, 0.12, len(d)) if len(d) > 1 else [0]
            ax.scatter(i + np.array(jit), d, s=34, color=col, marker=mk, edgecolor="white", linewidth=0.8, zorder=3)
            ax.hlines(d.mean(), i - 0.28, i + 0.28, color=col, lw=2, zorder=2)
            ax.text(i, d.mean() + ymax * 0.04, f"{d.mean():.1f}" if m != "throughput_tps" else f"{d.mean():.2f}", ha="center", va="bottom", fontsize=8, color=INK)
        ax.set_ylim(0, ymax)
        ax.set_xlim(-0.6, 3.6)
        ax.set_xticks(range(4))
        ax.set_xticklabels(["Static\nB=10", "R1", "Pred.\n(stored)", "R0"], fontsize=8)
        ax.set_title(title)
        ax.grid(axis="x", visible=False)
    fig.suptitle("Series cl01-cl12: run-level results (each point = one run; line = arm mean; axes start at zero)", x=0.01, ha="left", fontsize=9, color=INK2)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    save(fig, out, "fig1_cl_run_level")


def fig_cl_segments(out: Path):
    seg = pd.read_csv(CL_DIR / "segments_all_runs.csv")
    seg = seg[seg.measured].copy()
    fig, axes = plt.subplots(3, 1, figsize=(7.2, 7.4), sharex=True, gridspec_kw={"height_ratios": [1, 2.2, 0.9]})
    ref = seg[seg.arm == "static"].groupby("round").offered_tps.mean()
    axes[0].bar(ref.index, ref.values, color="#c9c8c2", width=0.62)
    axes[0].set_ylabel("offered rate\n(tx/s)")
    axes[0].set_title("(a) Offered arrival rate per 60 s segment (identical schedule in every arm)")
    for key, label, col, mk in ARMS:
        d = seg[seg.arm == key]
        for _, g in d.groupby("run_id"):
            axes[1].plot(g["round"], g.lat_mean_ms, color=col, lw=0.8, alpha=0.35)
        m = d.groupby("round").lat_mean_ms.mean()
        axes[1].plot(m.index, m.values, color=col, marker=mk, ms=5, lw=1.7, markeredgecolor="white", markeredgewidth=0.6)
    axes[1].annotate("R0 (500/200 ms)", (10, seg[(seg.arm == "reactive-R0") & (seg["round"] == 10)].lat_mean_ms.mean()), xytext=(8, 0),
                     textcoords="offset points", fontsize=8, color=INK, va="center")
    axes[1].annotate("Static, R1 and Predictive:\nlines coincide (B=10)", (10, seg[(seg.arm == "static") & (seg["round"] == 10)].lat_mean_ms.mean()),
                     xytext=(8, -14), textcoords="offset points", fontsize=8, color=INK, va="center")
    axes[1].set_ylabel("mean latency (ms)")
    axes[1].set_ylim(0, 1300)
    axes[1].set_title("(b) Mean latency per segment (thin = individual runs, marker line = arm mean)")
    ypos = {"static": 3, "reactive-R1": 2, "predictive": 1, "reactive-R0": 0}
    for key, _, col, mk in ARMS:
        d = seg[seg.arm == key]
        for _, g in d.groupby("run_id"):
            axes[2].scatter(g["round"], [ypos[key]] * len(g), c=[col if b == 10 else INK for b in g.B], s=[22 if b == 10 else 52 for b in g.B],
                            marker=mk, edgecolor="white", linewidth=0.5, alpha=0.9)
    axes[2].set_yticks([0, 1, 2, 3])
    axes[2].set_yticklabels(["R0", "Pred. (stored)", "R1", "Static"])
    axes[2].set_ylim(-0.6, 3.6)
    axes[2].set_title("(c) Configuration in force (small coloured = B=10, large dark = B=40)")
    axes[2].set_xlabel("measured segment (60 s each)")
    axes[2].set_xticks(range(1, 11))
    axes[1].set_xlim(0.4, 11.6)
    fig.tight_layout()
    save(fig, out, "fig2_cl_segments")


def fig_cl_actions(out: Path, runs: pd.DataFrame):
    fig, axes = plt.subplots(1, 3, figsize=(9.2, 2.9))
    cols = [("transitions_applied", "Configuration changes\napplied per run"), ("decisions_B40", "Decisions selecting B=40\nper run"), ("abstentions", "Abstentions per run")]
    for ax, (c, title) in zip(axes, cols):
        for i, (key, _, col, mk) in enumerate(ARMS):
            d = runs[runs.arm == key][c].values
            ax.scatter(i + np.linspace(-0.12, 0.12, len(d)) if len(d) > 1 else [i], d, s=36, color=col, marker=mk, edgecolor="white", linewidth=0.8, zorder=3)
            ax.hlines(d.mean(), i - 0.28, i + 0.28, color=col, lw=2)
        ax.set_ylim(-0.5, 6.3 if c == "decisions_B40" else 4.3 if c == "abstentions" else 1.8)
        ax.set_xlim(-0.6, 3.6)
        ax.set_xticks(range(4))
        ax.set_xticklabels(["Static\nB=10", "R1", "Pred.\n(stored)", "R0"], fontsize=8)
        ax.set_title(title, fontsize=9)
        ax.grid(axis="x", visible=False)
    fig.tight_layout()
    save(fig, out, "fig3_cl_controller_actions")


# ---------------------------------------------------------------- X0
def x0_tables(out: Path, md: list):
    from Fabric_Live_Testbed.closed_loop_v2 import analysis_v2 as A
    from Fabric_Live_Testbed.closed_loop_v2 import x0_decide as XD

    rows = XD.load_x0_rows(X0_STAGE, X0_EVIDENCE)
    rr = pd.DataFrame(rows)
    rr["delivered_tps"] = rr.success / rr.round_wall_s
    rr.to_csv(out / "tables/T5_x0_round_level.csv", index=False)
    rung = pd.DataFrame(A.build_rung_table(rows))
    rung["p95_reps"] = rung.p95_reps.apply(lambda v: " / ".join(f"{x:.0f}" for x in v))
    rung["classes"] = rung.classes.apply(lambda v: " / ".join(v))
    rung.to_csv(out / "tables/T4_x0_rung_table.csv", index=False)
    dec = json.loads(X0_DECISION.read_text(encoding="utf-8"))
    # batching-relation consistency check: mean latency = min((B-1)/rate, 2 s)/2 + c_B, one c_B per B from the client-valid rungs
    def w_mean(B, r):
        return min((B - 1) / r, 2.0) / 2.0 * 1000.0
    chk = []
    fit_c = {}
    for B in (10, 40):
        d = rung[(rung.B == B) & rung.client_valid]
        fit_c[B] = float(np.median([row.mean_ms - w_mean(B, row.rate_tps) for row in d.itertuples()]))
    for row in rung.itertuples():
        pred = w_mean(row.B, row.rate_tps) + fit_c[row.B]
        chk.append({"B": row.B, "rate_tps": row.rate_tps, "client_valid": row.client_valid, "mean_ms_measured": row.mean_ms,
                    "wait_term_ms": w_mean(row.B, row.rate_tps), "c_B_ms_fitted": fit_c[row.B], "mean_ms_model": pred, "residual_ms": row.mean_ms - pred})
    t6 = pd.DataFrame(chk)
    t6.to_csv(out / "tables/T6_x0_batching_relation_check.csv", index=False)
    md.append("### Table 4. X0 rung table (p95 per replicate, ms; classes per replicate)\n\n" + md_table(
        rung[["B", "rate_tps", "p95_reps", "p95_ms", "mean_ms", "client_valid", "classes"]], "{:.1f}"))
    md.append("### Table 6. Batching-relation consistency check (one fitted constant per B, client-valid rungs; descriptive, not a validation)\n\n" + md_table(
        t6, "{:.1f}"))
    return rr, rung, t6, dec, fit_c


def _x0_series(ax, rung, B, col, mk, ycol, label):
    d = rung[rung.B == B].sort_values("rate_tps")
    v = d[d.client_valid]
    ax.plot(v.rate_tps, v[ycol], color=col, lw=1.4, alpha=0.9, zorder=2)  # line through client-valid rungs only
    ax.scatter(v.rate_tps, v[ycol], s=40, color=col, marker=mk, edgecolor="white", linewidth=0.8, zorder=3)
    nv = d[~d.client_valid]
    ax.scatter(nv.rate_tps, nv[ycol], s=40, facecolor="white", edgecolor=col, marker=mk, linewidth=1.4, zorder=3)


def fig_x0_latency(out: Path, rr: pd.DataFrame, rung: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(9.4, 3.9), gridspec_kw={"width_ratios": [1.25, 1]})
    ax = axes[0]
    for B, col, mk in ((10, C_BLUE, "o"), (40, C_ORANGE, "s")):
        _x0_series(ax, rung, B, col, mk, "p95_ms", f"B={B}")
        d = rr[rr.B == B]
        ax.scatter(d.rate_tps * (1.012 if B == 40 else 0.988), d.p95_ms, s=9, color=col, alpha=0.45, zorder=1)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([10, 20, 40, 60, 100, 160, 200])
    ax.set_xticklabels(["10", "20", "40", "60", "100", "160", "200"])
    ax.set_xlabel("offered rate (tx/s), one 60 s round per rung")
    ax.set_ylabel("p95 latency (ms)")
    ax.axvline(60, color="#8a8984", lw=1, ls="--")
    ax.text(60 * 1.04, 12000, "frozen valid-rung\nceiling 60 tx/s", fontsize=7.5, color=INK2, va="top")
    ax.set_ylim(60, 14000)
    ax.text(12, 340, "B=10", color=C_BLUE, fontsize=9, fontweight="bold")
    ax.text(12, 2700, "B=40", color=C_ORANGE, fontsize=9, fontweight="bold")
    ax.set_title("(a) p95 latency vs offered rate")
    ax.text(0.02, 0.02, "large: replicate mean (hollow = excluded as client-limited); small: replicates;\nline joins client-valid rungs", transform=ax.transAxes, fontsize=7, color=INK2)
    ax = axes[1]
    d10 = rung[rung.B == 10].set_index("rate_tps")
    d40 = rung[rung.B == 40].set_index("rate_tps")
    rates = sorted(set(d10.index) & set(d40.index))
    ratio = np.array([d40.loc[r, "p95_ms"] / d10.loc[r, "p95_ms"] for r in rates])
    valid = np.array([bool(d10.loc[r, "client_valid"] and d40.loc[r, "client_valid"]) for r in rates])
    ax.plot(np.array(rates)[valid], ratio[valid], color="#8a8984", lw=1, zorder=1)
    ax.scatter(np.array(rates)[valid], ratio[valid], s=42, color=INK, zorder=3)
    ax.scatter(np.array(rates)[~valid], ratio[~valid], s=42, facecolor="white", edgecolor=INK, linewidth=1.4, zorder=3)
    ax.axhline(1.0, color=C_ORANGE, lw=1.3)
    ax.text(10.5, 1.06, "B=40 better above this line" if False else "ratio = 1: no difference", fontsize=7.5, color=C_ORANGE, va="bottom")
    ax.set_yscale("log")
    ax.set_xscale("log")
    ax.set_xticks([10, 20, 40, 60, 100, 160, 200])
    ax.set_xticklabels(["10", "20", "40", "60", "100", "160", "200"])
    ax.set_ylim(0.1, 10)
    ax.set_xlabel("offered rate (tx/s)")
    ax.set_ylabel("p95(B=40) / p95(B=10)")
    ax.set_title("(b) p95 ratio; above 1 = B=10 faster")
    r80 = rates.index(80.0)
    ax.annotate("cx02 B=10 stall\n(client-limited round)", (80, ratio[r80]), xytext=(95, 0.17), fontsize=7.5, color=INK2,
                arrowprops=dict(arrowstyle="-", color="#8a8984", lw=0.8))
    fig.tight_layout()
    save(fig, out, "fig4_x0_latency_vs_rate")


def fig_x0_model(out: Path, rung: pd.DataFrame, fit_c: dict):
    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    rr = np.geomspace(8, 220, 200)
    seg = pd.read_csv(CL_DIR / "segments_all_runs.csv")
    seg = seg[seg.measured & (seg.dispatch_flagged == False)]  # noqa: E712
    for B, col, mk in ((10, C_BLUE, "o"), (40, C_ORANGE, "s")):
        mdl = [min((B - 1) / r, 2.0) / 2.0 * 1000.0 + fit_c[B] for r in rr]
        ax.plot(rr, mdl, color=col, lw=1.3, ls="--", zorder=2)
        s = seg[seg.B == B]
        ax.scatter(s.offered_tps, s.lat_mean_ms, s=11, color=col, alpha=0.35, marker=mk, zorder=1, linewidth=0)
        _x0_series(ax, rung, B, col, mk, "mean_ms", f"B={B}")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([10, 20, 40, 60, 100, 160, 200])
    ax.set_xticklabels(["10", "20", "40", "60", "100", "160", "200"])
    ax.set_xlabel("offered rate (tx/s)")
    ax.set_ylabel("mean latency (ms)")
    ax.text(150, 420, "B=40", color=C_ORANGE, fontsize=9, fontweight="bold")
    ax.text(150, 130, "B=10", color=C_BLUE, fontsize=9, fontweight="bold")
    ax.set_title("Mean latency follows min((B-1)/rate, 2 s)/2 + constant", fontsize=9.5)
    ax.text(0.02, 0.03, "large markers: X0 rungs (hollow = client-limited); small faint dots: cl-series segments\n"
            f"dashed: model with fitted constants c10 = {fit_c[10]:.0f} ms, c40 = {fit_c[40]:.0f} ms (one constant per B, X0 valid rungs)",
            transform=ax.transAxes, fontsize=7, color=INK2, va="bottom")
    fig.tight_layout()
    save(fig, out, "fig5_batching_relation")


def fig_x0_lag(out: Path, rr: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    for B, col, mk in ((10, C_BLUE, "o"), (40, C_ORANGE, "s")):
        d = rr[rr.B == B]
        lim = d["class"].isin(["CLIENT_LIMITED", "INCOMPLETE"])
        ax.scatter(d[~lim].rate_tps, d[~lim].lag_p99_ms.clip(lower=1), s=30, color=col, marker=mk, edgecolor="white", linewidth=0.6, zorder=3)
        ax.scatter(d[lim].rate_tps, d[lim].lag_p99_ms.clip(lower=1), s=34, facecolor="white", edgecolor=col, marker=mk, linewidth=1.4, zorder=3)
    ax.axhline(100, color="#8a8984", ls="--", lw=1)
    ax.text(10.3, 118, "client-limited if p99 dispatch lag > 100 ms", fontsize=7.5, color=INK2)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([10, 20, 40, 60, 100, 160, 200])
    ax.set_xticklabels(["10", "20", "40", "60", "100", "160", "200"])
    ax.set_xlabel("offered rate (tx/s)")
    ax.set_ylabel("dispatch lag p99 (ms)")
    ax.text(0.02, 0.94, "blue circle = B=10 round, orange square = B=40 round;\nhollow = classified client-limited (lag p99 > 100 ms)", transform=ax.transAxes, fontsize=7.5, color=INK2, va="top")
    ax.set_title("Load-driver (Caliper) dispatch lag per X0 round", fontsize=9.5)
    fig.tight_layout()
    save(fig, out, "fig6_x0_dispatch_lag")


# ---------------------------------------------------------------- offline forecasting (stored)
def forecast_assets(out: Path, md: list):
    s = pd.read_csv(FB_SCORES)
    s = s[(s.pool == "F")]
    rows = []
    for (tgt, h), g in s.groupby(["target", "horizon_s"]):
        naive = g[g.model == "persistence_interval"].iloc[0]
        sel = g[g.selected == True]  # noqa: E712
        for r in sel.itertuples():
            rows.append({"target": tgt, "horizon_s": h, "selected_model": r.model, "test_mae": r.mae, "naive_mae": naive.mae,
                         "skill_vs_naive_pct": 100 * r.skill_mae_vs_persistence, "bias": r.bias, "smape_pct": r.smape_pct, "n_origins": r.n})
    t7 = pd.DataFrame(rows).sort_values(["target", "horizon_s"])
    t7.to_csv(out / "tables/T7_forecast_accuracy_test_day.csv", index=False)
    md.append("### Table 7. Offline workload forecasting, test day 2026-10-12 (stored FB-FC-v1 scores; not recomputed)\n\n" + md_table(t7, "{:.3f}"))
    a = t7[t7.target == "arrival_rate"]
    fig, ax = plt.subplots(figsize=(5.8, 3.4))
    x = np.arange(len(a))
    ax.bar(x - 0.19, a.naive_mae, 0.36, color="#c9c8c2", label="naive (last interval)")
    ax.bar(x + 0.19, a.test_mae, 0.36, color=C_BLUE, label="selected model")
    for i, r in enumerate(a.itertuples()):
        ax.text(i + 0.19, r.test_mae + 0.08, f"{r.skill_vs_naive_pct:+.0f}%", ha="center", fontsize=8, color=INK)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{int(h)} s" for h in a.horizon_s])
    ax.set_xlabel("forecast horizon")
    ax.set_ylabel("test MAE (tx/s)")
    ax.legend(loc="upper right")
    ax.set_title("Arrival-rate forecast error on the held-out day (label = skill vs naive)", fontsize=9)
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    save(fig, out, "fig7_forecast_accuracy_offline")
    return t7


HFBTP_CSV = REPO / "data/raw/hfbtp/HFBTP.csv"
HFBTP_SHA256 = "ce295d6451787fefcfe2101579238cda4bede8539bc302e6a482aa4e5782bd5e"  # recorded in the project context; checked at run time


def hfbtp_assets(out: Path, md: list, rung: pd.DataFrame):
    """Descriptive cross-deployment comparison: HFBTP OBSERVED values (Orderers = 3, one observation per cell, Fabric 2.3, different
    hardware) against the X0 live measurements. Reads the observed columns only; no model, no prediction, no test."""
    got = sha256(HFBTP_CSV)
    if got != HFBTP_SHA256:
        raise SystemExit(f"HFBTP.csv sha256 mismatch: {got}")
    d = pd.read_csv(HFBTP_CSV)
    d.columns = [c.strip() for c in d.columns]
    d = d[(d["Orderers"] == 3) & d["Block Size"].isin([10, 40])]
    rows = []
    for rate in sorted(rung.rate_tps.unique()):
        row = {"rate_tps": rate}
        for B in (10, 40):
            h = d[(d["Set Transaction Arrival Rate"] == rate) & (d["Block Size"] == B)]
            x = rung[(rung.B == B) & (rung.rate_tps == rate)].iloc[0]
            row[f"hfbtp_B{B}_avg_latency_ms"] = float(h["Avg Latency"].iloc[0]) * 1000.0
            row[f"hfbtp_B{B}_throughput_tps"] = float(h["Throughput"].iloc[0])
            row[f"x0_B{B}_mean_latency_ms"] = float(x.mean_ms)
            row[f"x0_B{B}_client_valid"] = bool(x.client_valid)
        row["hfbtp_ratio_B40_over_B10"] = row["hfbtp_B40_avg_latency_ms"] / row["hfbtp_B10_avg_latency_ms"]
        row["x0_ratio_B40_over_B10"] = row["x0_B40_mean_latency_ms"] / row["x0_B10_mean_latency_ms"]
        rows.append(row)
    t8 = pd.DataFrame(rows)
    t8.to_csv(out / "tables/T8_hfbtp_observed_vs_x0.csv", index=False)
    md.append("### Table 8. HFBTP observed (Orderers = 3, one observation per cell) vs X0 mean latency; descriptive, different deployments\n\n" + md_table(t8, "{:.1f}"))
    fig, ax = plt.subplots(figsize=(6.6, 4.1))
    for B, col, mk in ((10, C_BLUE, "o"), (40, C_ORANGE, "s")):
        ax.plot(t8.rate_tps, t8[f"hfbtp_B{B}_avg_latency_ms"], color=col, ls="--", lw=1.2, marker="x", ms=6, alpha=0.9, zorder=2)
        x = t8[t8[f"x0_B{B}_client_valid"]]
        ax.plot(x.rate_tps, x[f"x0_B{B}_mean_latency_ms"], color=col, lw=1.6, marker=mk, ms=6, markeredgecolor="white", zorder=3)
        nx = t8[~t8[f"x0_B{B}_client_valid"]]
        ax.scatter(nx.rate_tps, nx[f"x0_B{B}_mean_latency_ms"], s=36, facecolor="white", edgecolor=col, marker=mk, linewidth=1.4, zorder=3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([10, 20, 40, 60, 100, 160, 200])
    ax.set_xticklabels(["10", "20", "40", "60", "100", "160", "200"])
    ax.set_xlabel("offered rate (tx/s)")
    ax.set_ylabel("mean latency (ms)")
    ax.text(0.03, 0.04, "solid, filled: this testbed (X0; hollow = client-limited)\ndashed, x: HFBTP observed (Fabric 2.3, other hardware, 1 obs/cell)", transform=ax.transAxes, fontsize=7.5, color=INK2)
    ax.annotate("HFBTP B=10 collapses\nat 160 tx/s", (160, t8.loc[t8.rate_tps == 160, "hfbtp_B10_avg_latency_ms"].iloc[0]), xytext=(50, 1350), fontsize=7.5, color=INK2,
                arrowprops=dict(arrowstyle="-", color="#8a8984", lw=0.8))
    ax.set_ylim(60, 1800)
    ax.text(11, 120, "B=10", color=C_BLUE, fontsize=9, fontweight="bold")
    ax.text(12, 1300, "B=40", color=C_ORANGE, fontsize=9, fontweight="bold")
    ax.set_title("Mean latency: HFBTP observed vs this testbed", fontsize=9.5)
    fig.tight_layout()
    save(fig, out, "fig8_hfbtp_observed_vs_x0")
    return t8


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    out = a.out
    out.mkdir(parents=True, exist_ok=False)
    (out / "figures").mkdir()
    (out / "tables").mkdir()
    style()
    md: list[str] = ["# Tables (generated; values are the CSVs in this folder)\n"]
    runs, rl, _ = cl_tables(out, md)
    fig_cl_runlevel(out, rl)
    fig_cl_segments(out)
    fig_cl_actions(out, runs)
    rr, rung, t6, dec, fit_c = x0_tables(out, md)
    fig_x0_latency(out, rr, rung)
    fig_x0_model(out, rung, fit_c)
    fig_x0_lag(out, rr)
    forecast_assets(out, md)
    hfbtp_assets(out, md, rung)
    (out / "tables/tables.md").write_text("\n\n".join(md) + "\n", encoding="utf-8")
    meta = {"script_sha256": sha256(Path(__file__)), "x0_decision_sha256": sha256(X0_DECISION), "x0_status": dec["status"], "fit_c_ms": fit_c,
            "matplotlib": matplotlib.__version__, "numpy": np.__version__, "pandas": pd.__version__, "python": sys.version.split()[0]}
    (out / "assets_provenance.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(meta, indent=1))


if __name__ == "__main__":
    main()
