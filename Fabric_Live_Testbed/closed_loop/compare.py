"""Cross-arm comparison (Windows/offline). Reads each run's run_result.json + analysis.json; writes CSV tables, figures and
SHA256SUMS into a NEW output folder. Runs whose verdict is not VALID / VALID_WITH_FLAGS, and every ACTUATION_CHECK run, are
listed but excluded from the strategy comparison. Descriptive only: no test statistic at n per arm this small.

  python -m Fabric_Live_Testbed.closed_loop.compare --results <dir with <run_id>/ folders> --out <new dir>
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
import sys
from pathlib import Path

ARM_ORDER = ["static", "reactive-R1", "predictive", "reactive-R0"]
COLORS = {"static": "#2a78d6", "reactive-R1": "#eb6834", "predictive": "#1baf7a", "reactive-R0": "#eda100"}
MARKERS = {"static": "o", "reactive-R1": "s", "predictive": "^", "reactive-R0": "D"}
COMPARABLE = ("VALID", "VALID_WITH_FLAGS")


def load_runs(results: Path) -> list[dict]:
    runs = []
    for rr in sorted(results.glob("*/run_result.json")):
        res = json.loads(rr.read_text(encoding="utf-8"))
        an_path = rr.parent / "analysis.json"
        res["_analysis"] = json.loads(an_path.read_text(encoding="utf-8")) if an_path.exists() else None
        runs.append(res)
    return runs


def run_row(res: dict) -> dict:
    a = res.get("_analysis") or {}
    rl = a.get("run_level_measured") or {}
    lat = rl.get("latency_ms") or {}
    dec = (res.get("control") or {}).get("decisions", [])
    trans = (res.get("control") or {}).get("transitions", [])
    return {"run_id": res["run_id"], "arm": res["arm"], "verdict": res.get("verdict"), "comparable": res.get("verdict") in COMPARABLE and "ACTUATION_CHECK" not in res["arm"],
            "scheduled": rl.get("scheduled_measured"), "success": rl.get("success"), "failed": rl.get("failed"),
            "offered_tps": rl.get("offered_tps"), "throughput_tps": rl.get("throughput_tps_over_round_walls"),
            "latency_mean_ms": lat.get("mean"), "latency_p50_ms": lat.get("p50"), "latency_p95_ms": lat.get("p95"),
            "latency_p99_ms": lat.get("p99"), "latency_max_ms": lat.get("max"),
            "decisions": len(dec), "abstentions": sum(1 for d in dec if d.get("abstained")),
            "decisions_selecting_B40": sum(1 for d in dec if d.get("target_B") == 40),
            "transitions_applied": sum(1 for t in trans if t.get("success")), "transitions_failed": sum(1 for t in trans if not t.get("success")),
            "final_B": (res.get("final_config") or {}).get("max_message_count"),
            "reconciled": (a.get("reconciliation") or {}).get("complete"), "flags": ";".join(a.get("flags", [])),
            "problems": ";".join(a.get("problems", [])), "step_failure": json.dumps(res.get("step_failure")) if res.get("step_failure") else ""}


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def summarize(rows: list[dict]) -> list[dict]:
    out = []
    for arm in ARM_ORDER + sorted({r["arm"] for r in rows} - set(ARM_ORDER)):
        rs = [r for r in rows if r["arm"] == arm and r["comparable"]]
        if not rs:
            continue
        entry = {"arm": arm, "runs": len(rs)}
        for k in ("throughput_tps", "latency_mean_ms", "latency_p95_ms", "latency_p99_ms", "failed", "transitions_applied"):
            vals = [r[k] for r in rs if r[k] is not None]
            entry[f"{k}_mean"] = statistics.fmean(vals) if vals else None
            entry[f"{k}_min"] = min(vals) if vals else None
            entry[f"{k}_max"] = max(vals) if vals else None
        out.append(entry)
    return out


def figures(out: Path, runs: list[dict], seg_rows: list[dict]) -> list[str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    made = []
    ink, ink2, grid = "#0b0b0b", "#52514e", "#e4e3df"

    def style(ax):
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            ax.spines[s].set_color(grid)
        ax.tick_params(colors=ink2, labelsize=9)
        ax.grid(axis="y", color=grid, linewidth=0.8)
        ax.set_axisbelow(True)

    # Fig 1: mean latency per segment, one line per comparable run, colour = arm
    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=150)
    seen = set()
    for res in runs:
        r = run_row(res)
        if not r["comparable"]:
            continue
        pts = sorted((s["segment"], s["latency_mean_ms"]) for s in seg_rows if s["run_id"] == r["run_id"] and s["latency_mean_ms"] is not None)
        if not pts:
            continue
        ax.plot([p[0] + 1 for p in pts], [p[1] for p in pts], color=COLORS.get(r["arm"], ink2), marker=MARKERS.get(r["arm"], "o"),
                markersize=5, linewidth=2, label=None if r["arm"] in seen else r["arm"])
        seen.add(r["arm"])
    segs = sorted({s["segment"] + 1 for s in seg_rows})
    ax.set_xticks(segs)
    ax.set_xlabel("Decision segment (60 s each, F_82800 replay)", color=ink2)
    ax.set_ylabel("Mean commit latency (ms)", color=ink2)
    ax.set_title("Measured mean latency per segment, by strategy", color=ink, loc="left", fontsize=11)
    style(ax)
    ax.legend(frameon=False, fontsize=9)
    fig.tight_layout()
    fig.savefig(out / "fig1_latency_by_segment.png")
    plt.close(fig)
    made.append("fig1_latency_by_segment.png")

    # Fig 2: run-level p95 and mean latency per arm (each run one marker)
    fig, axes = plt.subplots(1, 2, figsize=(8, 3.6), dpi=150, sharey=False)
    for ax, key, title in ((axes[0], "latency_mean_ms", "Run mean latency (ms)"), (axes[1], "latency_p95_ms", "Run p95 latency (ms)")):
        arms = [a for a in ARM_ORDER if any(run_row(x)["arm"] == a and run_row(x)["comparable"] for x in runs)]
        for i, arm in enumerate(arms):
            vals = [run_row(x)[key] for x in runs if run_row(x)["arm"] == arm and run_row(x)["comparable"]]
            ax.scatter([i] * len(vals), vals, color=COLORS[arm], marker=MARKERS[arm], s=40, zorder=3, edgecolors="#fcfcfb", linewidths=1.5)
        ax.set_xticks(range(len(arms)), arms, rotation=0)
        ax.set_title(title, color=ink, loc="left", fontsize=10)
        style(ax)
    fig.tight_layout()
    fig.savefig(out / "fig2_run_latency_by_arm.png")
    plt.close(fig)
    made.append("fig2_run_latency_by_arm.png")
    return made


def build(results: Path, out: Path) -> dict:
    if out.exists():
        raise FileExistsError(f"refusing to reuse {out}")
    runs = load_runs(results)
    if not runs:
        raise ValueError(f"no run_result.json under {results}")
    out.mkdir(parents=True)
    rows = [run_row(r) for r in runs]
    write_csv(out / "runs.csv", rows)
    seg_rows, dec_rows = [], []
    for res in runs:
        a = res.get("_analysis") or {}
        for s in a.get("rounds", []):
            if not s.get("measured"):
                continue
            under = s.get("committed_under_B") or {}
            seg_rows.append({"run_id": res["run_id"], "arm": res["arm"], "segment": s["segment"], "scheduled": s["scheduled"],
                             "success": s["success"], "failed": s["failed"], "offered_tps": s["offered_tps"],
                             "latency_mean_ms": s["latency_ms"]["mean"], "latency_p95_ms": s["latency_ms"]["p95"],
                             "committed_under_B10": under.get("10", 0), "committed_under_B40": under.get("40", 0),
                             "dispatch_lag_p99_ms": (s.get("dispatch") or {}).get("lag_ms", {}).get("p99")})
        for d in (res.get("control") or {}).get("decisions", []):
            act = d.get("actuation") if isinstance(d.get("actuation"), dict) else {}
            dec_rows.append({"run_id": res["run_id"], "arm": res["arm"], "decision_index": d["decision_index"],
                             "scheduled_utc": d.get("scheduled_utc"), "telemetry_round": d.get("telemetry_round"),
                             "telemetry_age_s": d.get("telemetry_age_s"), "current_B": d["current_B"], "target_B": d["target_B"],
                             "changed": d["changed"], "applied": d.get("applied"), "abstained": d["abstained"], "reason": d["reason"],
                             "actuation_ms": act.get("duration_ms"), "actuation_error": act.get("error") if act else (d.get("actuation") or "")})
    if seg_rows:
        write_csv(out / "segments.csv", seg_rows)
    if dec_rows:
        write_csv(out / "decisions.csv", dec_rows)
    summary = summarize(rows)
    if summary:
        write_csv(out / "arm_summary.csv", summary)
    figs = figures(out, runs, seg_rows) if seg_rows else []
    sums = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.iterdir()) if p.is_file()}
    (out / "SHA256SUMS.txt").write_text("".join(f"{v}  {k}\n" for k, v in sums.items()), encoding="ascii", newline="\n")
    return {"runs": len(rows), "comparable": sum(r["comparable"] for r in rows), "figures": figs, "out": str(out)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Compare closed-loop runs (offline).")
    ap.add_argument("--results", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    print(json.dumps(build(a.results, a.out), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
