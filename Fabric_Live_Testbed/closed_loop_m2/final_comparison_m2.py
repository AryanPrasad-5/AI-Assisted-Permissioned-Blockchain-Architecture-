"""CLI: M2 evidence -> the final Static vs Reactive vs Predictive comparison (csv / md / json). Offline, stdlib; reads files, starts nothing.

  python -m Fabric_Live_Testbed.closed_loop_m2.final_comparison_m2 --stage <stage_m2> --out-root <run output dir> --report-dir <NEW dir> \
      [--label MEASURED|SYNTHETIC]

Every run listed in the stage is looked up under <out-root>/<run_id>/ (analysis.json, run_result.json) and
<out-root>/<run_id>_verification/independent_verification.json. A run that is missing or incomplete is listed as such and never imputed.
The primary analysis uses COMPARABLE runs (VALID or VALID_WITH_FLAGS, no CLIENT_LIMITED or INCOMPLETE round, independent verification not failed);
the sensitivity analysis uses every VALID run. Both tables are written.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

from . import analysis_m2 as A
from . import protocol_m2 as P


def load_records(stage: Path, out_root: Path) -> tuple[list[dict], list[dict], dict]:
    manifest = json.loads((stage / "STAGE_MANIFEST.json").read_text(encoding="utf-8"))
    records, missing, regrets = [], [], {}
    for run in manifest["runs"]:
        rid = run["run_id"]
        arm = json.loads((stage / rid / "arm.json").read_text(encoding="utf-8"))
        an_p, rr_p = out_root / rid / "analysis.json", out_root / rid / "run_result.json"
        if not (an_p.is_file() and rr_p.is_file()):
            missing.append({"run_id": rid, "arm_label": arm["arm_label"], "reason": "no analysis.json / run_result.json (run not executed or stopped early)"})
            continue
        analysis, rr = json.loads(an_p.read_text(encoding="utf-8")), json.loads(rr_p.read_text(encoding="utf-8"))
        iv_p = out_root / f"{rid}_verification" / "independent_verification.json"
        iv = json.loads(iv_p.read_text(encoding="utf-8")).get("independent_pass") if iv_p.is_file() else None
        rec = A.run_record(arm, analysis, rr, independent_pass=iv)
        records.append(rec)
        if arm["arm"] == "predictive_live":
            regrets[rid] = A.model_regret(arm, rec)
    return records, missing, {"manifest": manifest, "model_regret": regrets}


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def build(stage: Path, out_root: Path, report_dir: Path, label: str = "MEASURED") -> dict:
    if report_dir.exists():
        raise FileExistsError(f"refusing to reuse {report_dir}")
    records, missing, extra = load_records(stage, out_root)
    report_dir.mkdir(parents=True)
    primary, sensitivity = A.final_table(records, analysis_set="comparable"), A.final_table(records, analysis_set="all_valid")
    doc = {"label": label, "protocol": P.PROTOCOL_ID, "stage_sha256": hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest(),
           "rule": P.MEANINGFUL_RULE, "primary": primary, "sensitivity_all_valid_runs": sensitivity, "runs_missing": missing,
           "model_regret_predictive_ms": extra["model_regret"], "records": records}
    (report_dir / "final_comparison.json").write_text(json.dumps(doc, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    md = [f"# M2 final comparison ({label})", "", f"Protocol: {P.PROTOCOL_ID}", f"Stage SHA256SUMS: `{doc['stage_sha256']}`", "",
          "## Primary table (comparable runs: no CLIENT_LIMITED / INCOMPLETE round)", "", primary["markdown"], "",
          f"**Verdict ({primary['verdict']['status']}):** {primary['verdict']['text']}", "",
          "## Sensitivity table (every VALID run)", "", sensitivity["markdown"], "",
          f"**Verdict ({sensitivity['verdict']['status']}):** {sensitivity['verdict']['text']}", "", "## Pairwise verdicts (primary set)", ""]
    for key, res in primary["pairwise"].items():
        md.append(f"- `{key}`: " + "; ".join(f"{m}: {v['verdict']} (diff {v.get('difference_of_means', float('nan')):+.4g}, SESOI {v.get('sesoi', float('nan')):.4g})"
                                            for m, v in res.items() if v.get("verdict") != "NOT_ASSESSABLE"))
    md += ["", "## Runs missing", ""] + ([f"- {m['run_id']} ({m['arm_label']}): {m['reason']}" for m in missing] or ["- none"]) + ["", f"Rule: {P.MEANINGFUL_RULE}", ""]
    (report_dir / "final_comparison.md").write_text("\n".join(md), encoding="utf-8")
    write_csv(report_dir / "run_records.csv", records,
              ["run_id", "arm_label", "replicate", "initial_B", "verdict", "comparable", "n_client_limited", "scheduled", "success", "failed", "success_rate",
               "throughput_tps", "offered_tps", "delivered_ratio", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms", "n_decisions", "decisions_changed",
               "decisions_abstained", "config_changes", "ledger_config_blocks"])
    write_csv(report_dir / "segments.csv", [{"run_id": r["run_id"], "arm_label": r["arm_label"], **s} for r in records for s in r["segments"]],
              ["run_id", "arm_label", "segment", "rate_tps", "B", "mean_ms", "p95_ms", "success", "failed", "class", "pending_at_deactivate"])
    write_csv(report_dir / "forecast_accuracy.csv",
              [{"run_id": r["run_id"], **x} for r in records for x in r.get("forecast", {}).get("rows", [])],
              ["run_id", "decision", "forecast_tps", "realized_tps", "naive_persistence_tps", "err_ewma", "err_naive", "abstained"])
    sums = "".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in sorted(report_dir.iterdir()))
    (report_dir / "SHA256SUMS.txt").write_text(sums, encoding="utf-8")
    return doc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build the M2 final comparison table from run evidence (offline).")
    ap.add_argument("--stage", required=True, type=Path)
    ap.add_argument("--out-root", required=True, type=Path)
    ap.add_argument("--report-dir", required=True, type=Path)
    ap.add_argument("--label", default="MEASURED", choices=("MEASURED", "SYNTHETIC"))
    a = ap.parse_args(argv)
    doc = build(a.stage, a.out_root, a.report_dir, a.label)
    print(doc["primary"]["markdown"])
    print(doc["primary"]["verdict"]["text"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
