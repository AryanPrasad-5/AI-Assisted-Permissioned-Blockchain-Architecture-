"""Reproducibility inventory for Results and Discussion v2: every reported result -> source data, run IDs, configuration, script, sha256.

Read-only on all evidence. Writes into a NEW directory (refuses to overwrite). Reports MISSING for any source it cannot find.

  .venv/Scripts/python.exe -B Fabric_Live_Testbed/paper_v2/build_inventory.py --out D:/PLAN_BLOCK18/results_discussion_v2_2026-11-25/reproducibility
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PLAN = Path("D:/PLAN_BLOCK18")
CL = PLAN / "closed_loop_experiment_2026-11-22"
CLR = CL / "series_evidence_2026-11-22"
CLS = CL / "stage_series"
CLF = CL / "final_results_2026-11-25/series_stats_v2_cl01-cl12"
LPC = PLAN / "live_predictive_control_2026-11-25"
OUT_V2 = PLAN / "results_discussion_v2_2026-11-25"
ASSETS = OUT_V2 / "assets"
FB = REPO / "Flashbots"
P1C = Path("D:/Plan_research/hfbtp_probe/outputs/phase1c")

CL_RUNS = ["cl01", "cl02", "cl03", "cl05", "cl06", "cl07", "cl08", "cl09", "cl10", "cl11", "cl12"]
X0_RUNS = ["cx01", "cx02", "cx03", "cx04"]


def sha(p: Path):
    p = Path(p)
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else "MISSING"


def rel(p: Path) -> str:
    p = Path(p)
    for base in (REPO, PLAN, Path("D:/Plan_research")):
        try:
            return str(p.relative_to(base)).replace("\\", "/") if base != REPO else "REPO/" + str(p.relative_to(base)).replace("\\", "/")
        except ValueError:
            continue
    return str(p)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    a.out.mkdir(parents=True, exist_ok=False)

    # ---- per-run lineage
    runs = []
    for r in CL_RUNS:
        runs.append({"run_id": r, "series": "cl01-cl12", "stage_arm_json": CLS / r / "arm.json", "analysis_json": CLR / r / r / "analysis.json",
                     "run_result_json": CLR / r / r / "run_result.json", "evidence_list": CLR / f"{r}_EVIDENCE_SHA256SUMS.txt" if (CLR / f"{r}_EVIDENCE_SHA256SUMS.txt").exists() else CLR / r / f"{r}_EVIDENCE_SHA256SUMS.txt",
                     "continuation_decision": CLR / "s5a1_decisions" / f"{r}.json", "authorization": CL / "AUTH_SERIES.txt"})
    for r in X0_RUNS:
        runs.append({"run_id": r, "series": "X0", "stage_arm_json": LPC / "stage_x0" / r / "arm.json", "analysis_json": LPC / "evidence" / r / r / "analysis.json",
                     "run_result_json": LPC / "evidence" / r / r / "run_result.json", "evidence_list": LPC / "evidence" / r / f"{r}_EVIDENCE_SHA256SUMS.txt",
                     "continuation_decision": LPC / "evidence" / r / "decisions_v2" / f"{r}.json", "authorization": LPC / "AUTH_X0.txt"})
    cols = ["run_id", "series", "stage_arm_json", "analysis_json", "run_result_json", "evidence_list", "continuation_decision", "authorization"]
    with (a.out / "run_lineage.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["run_id", "series"] + [c + "_path" for c in cols[2:]] + [c + "_sha256" for c in cols[2:]])
        for r in runs:
            w.writerow([r["run_id"], r["series"]] + [rel(r[c]) for c in cols[2:]] + [sha(r[c]) for c in cols[2:]])

    # ---- claim -> source lineage
    stats_script = REPO / "Fabric_Live_Testbed/closed_loop/series_ops_v2/series_stats_v2.py"
    build_assets = REPO / "Fabric_Live_Testbed/paper_v2/build_paper_assets.py"
    x0dec = LPC / "x0_decision_20261125T1015Z/x0_decision.json"
    claims = [
        ("C01", "Run-level arm means, Table 1 / Fig 1 (cl01-cl12)", "run_level_by_scenario.csv; runs_all.csv", [CLF / "run_level_by_scenario.csv", CLF / "runs_all.csv"],
         "cl01,cl02,cl03,cl05-cl12", "stage_series/clNN/arm.json; AUTH_SERIES.txt", [stats_script, build_assets]),
        ("C02", "Differences from Static with Welch CI and permutation floor, Table 2", "differences_vs_static_by_scenario.csv", [CLF / "differences_vs_static_by_scenario.csv"],
         "cl01,cl02,cl03,cl05-cl12", "as C01", [stats_script]),
        ("C03", "R0 single 10->40 switch at decision 5, ledger block 374, adaptation latency 1.62 s", "config_updates.csv", [CLF / "config_updates.csv"], "cl01,cl10",
         "stage_series/cl01/arm.json, cl10 (R0 thresholds 500/200 ms)", [stats_script]),
        ("C04", "Static, R1, Predictive stayed at B=10 in all segments; R0 B trajectory LLLLLHHHHH", "runs_all.csv (B_trajectory); segments_all_runs.csv (B column)",
         [CLF / "runs_all.csv", CLF / "segments_all_runs.csv"], "all cl runs", "as C01", [stats_script, build_assets]),
        ("C05", "Every run: 6,229 measured transactions, 0 failed", "run_level_by_scenario.csv (n, failed)", [CLF / "run_level_by_scenario.csv"], "all cl runs", "as C01", [stats_script]),
        ("C06", "Dispatch-lag flag in round 6 for all 11 runs (p99 109-171 ms)", "runs_all.csv; dispatch_fidelity_by_run_round.csv", [CLF / "runs_all.csv", CLF / "dispatch_fidelity_by_run_round.csv"],
         "all cl runs", "AUTH_S5_AMENDMENT.txt; S5_AMENDMENT_A1_2026-11-22.md", [stats_script]),
        ("C07", "R1 highest round mean latency vs 2,000 ms trigger (never reached)", "segments_all_runs.csv", [CLF / "segments_all_runs.csv"], "cl02,cl03,cl12", "R1 thresholds in stage arm.json", [stats_script]),
        ("C08", "Predictive = stored decisions; 3 abstentions per run, none select B=40", "runs_all.csv; stage decision files", [CLF / "runs_all.csv", CLS / "cl07/arm.json", CLS / "cl09/arm.json", CLS / "cl11/arm.json"],
         "cl07,cl09,cl11", "D-E2-10 stored decision table (policy_provenance in stage arm.json)", [stats_script]),
        ("C09", "X0 rung table (p95/mean per B and rate, client-validity classes), Table 4 / Fig 4", "x0_decision.json rung_table; T4_x0_rung_table.csv", [x0dec, ASSETS / "tables/T4_x0_rung_table.csv", ASSETS / "tables/T5_x0_round_level.csv"],
         "cx01-cx04", "stage_x0/cxNN/arm.json; PROTOCOL_LPC_v1.json (9b0bfa74...)", [REPO / "Fabric_Live_Testbed/closed_loop_v2/x0_decide.py", REPO / "Fabric_Live_Testbed/closed_loop_v2/analysis_v2.py", build_assets]),
        ("C10", "X0 decision STOP_NEGATIVE_RESULT / NO_CROSSOVER_WITHIN_CLIENT_CAPACITY, r_max = 60 tx/s", "x0_decision.json", [x0dec, x0dec.parent / "SHA256SUMS.txt"], "cx01-cx04",
         "PROTOCOL_LPC_v1.json; source_hashes_frozen_v2.txt (131b66f9...)", [REPO / "Fabric_Live_Testbed/closed_loop_v2/x0_decide.py", REPO / "Fabric_Live_Testbed/closed_loop_v2/protocol.py"]),
        ("C11", "Client dispatch lag per X0 round, Fig 6", "T5_x0_round_level.csv (lag_p99_ms, class)", [ASSETS / "tables/T5_x0_round_level.csv"], "cx01-cx04", "classify_round in protocol.py (lag p99 > 100 ms)",
         [REPO / "Fabric_Live_Testbed/closed_loop_v2/protocol.py", build_assets]),
        ("C12", "Batching-relation consistency check, Table 6 / Fig 5 (one constant per B, descriptive)", "T6_x0_batching_relation_check.csv", [ASSETS / "tables/T6_x0_batching_relation_check.csv"],
         "cx01-cx04 (+ cl segments as overlay)", "relation W = min((B-1)/rate, 2 s)/2 + c_B", [build_assets]),
        ("C13", "Offline arrival-rate forecast accuracy, Table 7 / Fig 7 (stored, not recomputed)", "scores_F.csv; final_report.json", [FB / "results/FB-FC-v1/final/scores_F.csv", FB / "results/FB-FC-v1/final/final_report.json"],
         "n/a (offline, test day 2026-10-12)", "Flashbots/experiments/configs/FB-FC-v1.json", [FB / "fbcommon.py", FB / "results/RESULTS_SUMMARY.md"]),
        ("C14", "Stored value-of-information analysis: oracle headroom over static B=10 is 0.0 s on the test day", "voi_F_summary.json; FABRIC_CONNECTION.md",
         [REPO / "experiments/results/flashbots_horizon_benchmark_v2/voi/voi_F_summary.json", FB / "FABRIC_CONNECTION.md"], "n/a (offline)", "batching law p95 = W + c, W = min((B-1)/rate, 2 s)", [FB / "FABRIC_CONNECTION.md"]),
        ("C15", "HFBTP offline model results (selected-model test MAE; HEAVY extrapolation failure)", "PHASE_1C_REPORT.md; test_results.csv", [P1C / "PHASE_1C_REPORT.md", P1C / "test_results.csv"],
         "n/a (offline)", "experiment_config.json (frozen)", [P1C / "experiment_config.json"]),
        ("C16", "HFBTP-to-live-testbed transfer not established", "hfbtp_source_to_e1_construct_alignment_second_audit_2026-11-11.md",
         [REPO / "docs/research/e1/hfbtp_source_to_e1_construct_alignment_second_audit_2026-11-11.md"], "n/a", "n/a", []),
        ("C17", "Post-hoc continuation amendments S5-A1 and S5-A2", "AUTH_S5_AMENDMENT.txt; AUTH_S5_A2.txt; S5_AMENDMENT_A1/A2 records",
         [CL / "AUTH_S5_AMENDMENT.txt", CL / "AUTH_S5_A2.txt", CL / "S5_AMENDMENT_A1_2026-11-22.md", CL / "S5_AMENDMENT_A2_2026-11-25.md"], "cl01-cl12", "n/a", []),
        ("C19", "HFBTP observed (Orderers = 3, B=10/40) vs X0 mean latency, Table 8 / Fig 8 (descriptive; observed columns only)", "HFBTP.csv (observed columns); T8_hfbtp_observed_vs_x0.csv; X0 rung table",
         [REPO / "data/raw/hfbtp/HFBTP.csv", ASSETS / "tables/T8_hfbtp_observed_vs_x0.csv", ASSETS / "tables/T4_x0_rung_table.csv"], "cx01-cx04", "HFBTP.csv sha256 ce295d64... checked at run time", [build_assets]),
        ("C18", "Frozen live-predictive protocol and source freeze (not executed beyond X0)", "PROTOCOL_LPC_v1.json; source_hashes_frozen_v2.txt; stage_x0/SHA256SUMS.txt",
         [LPC / "protocol/PROTOCOL_LPC_v1.json", LPC / "source_hashes_frozen_v2.txt", LPC / "stage_x0/SHA256SUMS.txt", LPC / "PROTOCOL_LPC_v1.md"], "cx01-cx04", "n/a", []),
    ]
    missing = []
    with (a.out / "claim_lineage.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["claim_id", "reported_result", "source_data_description", "source_files", "source_sha256", "run_ids", "configuration", "analysis_code", "analysis_code_sha256", "code_in_git"])
        for cid, text, desc, srcs, runs_s, conf, code in claims:
            for p in list(srcs) + list(code):
                if not Path(p).exists():
                    missing.append((cid, str(p)))
            w.writerow([cid, text, desc, " | ".join(rel(p) for p in srcs), " | ".join(sha(p) for p in srcs), runs_s, conf,
                        " | ".join(rel(p) for p in code), " | ".join(sha(p) for p in code), "UNTRACKED (see GIT_REVIEW_PACKAGE.md)" if "REPO" in " ".join(rel(p) for p in code) and "Flashbots" not in " ".join(rel(p) for p in code) and "phase1c" not in " ".join(rel(p) for p in code) else "see git status"])

    # ---- frozen / signed artifacts
    frozen = [CL / "AUTH_SERIES.txt", CL / "AUTH_S5_A2.txt", CL / "AUTH_S5_AMENDMENT.txt", CL / "stage_series_listing.json", LPC / "AUTH_X0.txt", LPC / "protocol/PROTOCOL_LPC_v1.json",
              LPC / "source_hashes_frozen_v2.txt", LPC / "stage_x0/SHA256SUMS.txt", LPC / "stage_x0/STAGE_MANIFEST.json", stats_script, build_assets, x0dec]
    with (a.out / "frozen_and_signed_artifact_hashes.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["path", "sha256"])
        for p in frozen:
            w.writerow([rel(p), sha(p)])

    # ---- cross-check the hashes the paper quotes
    quoted = {"AUTH_SERIES": ("d9e8d1ae", CL / "AUTH_SERIES.txt"), "AUTH_S5_A2": ("89dc3cfc", CL / "AUTH_S5_A2.txt"),
              "protocol_LPC_v1": ("9b0bfa74", LPC / "protocol/PROTOCOL_LPC_v1.json"), "source_freeze_v2": ("131b66f9", LPC / "source_hashes_frozen_v2.txt"),
              "stage_x0": ("7ec1c029", LPC / "stage_x0/SHA256SUMS.txt"), "series_stats_v2.py": ("5ace64a2", stats_script)}
    res = {k: (v[0], sha(v[1])[:8], sha(v[1]).startswith(v[0])) for k, v in quoted.items()}
    summary = {"missing_sources": missing, "quoted_hash_check": res, "run_lineage_rows": len(runs), "claims": len(claims)}
    (a.out / "inventory_check.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
