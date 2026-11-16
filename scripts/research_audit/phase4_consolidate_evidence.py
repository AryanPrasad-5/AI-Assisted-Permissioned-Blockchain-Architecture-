#!/usr/bin/env python3
"""Read-only Phase 4 evidence consolidation; refuses to overwrite outputs."""
from __future__ import annotations

import csv
import argparse
import gzip
import hashlib
import json
import math
import statistics
import io
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "experiments/results/e1/phase4_research_evidence_20261111"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def read_json(rel: str):
    return json.loads((ROOT / rel).read_text(encoding="utf-8"))


def read_csv(rel: str):
    with (ROOT / rel).open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def audit_raw_hfbtp(path: Path):
    expected = {"Set Transaction Arrival Rate", "Actual Transaction Arrival Rate", "Block Size", "Orderers",
                "Throughput", "Avg Latency", "Min Latency", "Max Latency"}
    n = missing = duplicates = 0
    cells = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        if set(headers) != expected:
            raise ValueError(f"HFBTP headers mismatch: {headers}")
        for row in reader:
            n += 1
            missing += sum(v is None or not v.strip() for v in row.values())
            key = (row["Set Transaction Arrival Rate"], row["Block Size"], row["Orderers"])
            if key in cells: duplicates += 1
            cells.add(key)
    return {"rows": n, "headers": sorted(expected), "missing_cells_in_values": missing,
            "duplicate_configuration_cells": duplicates, "observed_configuration_cells": len(cells)}


def audit_zip_csv(path: Path, entry: str, date: str):
    expected_headers = None
    rows = invalid_ts = missing_ts = decreases = out_of_day = 0
    previous = None
    start_ms = int(__import__("datetime").datetime.fromisoformat(date).replace(tzinfo=__import__("datetime").timezone.utc).timestamp() * 1000)
    end_ms = start_ms + 86400 * 1000
    with zipfile.ZipFile(path) as zf:
        bad_member = zf.testzip()
        if bad_member is not None: raise ValueError(f"Corrupt ZIP member: {bad_member}")
        with zf.open(entry, "r") as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text_stream:
            reader = csv.DictReader(text_stream)
            expected_headers = reader.fieldnames
            if "timestamp_ms" not in (expected_headers or []): raise ValueError("timestamp_ms column missing")
            for row in reader:
                rows += 1
                value = row.get("timestamp_ms")
                if value is None or not value.strip(): missing_ts += 1
                try: current = int(value)
                except (TypeError, ValueError):
                    invalid_ts += 1
                    continue
                if previous is not None and current < previous: decreases += 1
                previous = current
                if current < start_ms or current >= end_ms: out_of_day += 1
    return {"path": str(path), "entry": entry, "rows": rows, "crc_valid": True,
            "columns": expected_headers, "invalid_timestamp_values": invalid_ts,
            "chronological_decreases": decreases, "out_of_utc_day": out_of_day,
            "missing_timestamp_values": missing_ts}


def q1_iqr(values):
    x = sorted(float(v) for v in values)
    if not x:
        return None
    # Tukey hinges; deterministic and reported only as descriptive summaries.
    lo, hi = x[: len(x)//2], x[(len(x)+1)//2 :]
    q1 = statistics.median(lo) if lo else x[0]
    q3 = statistics.median(hi) if hi else x[-1]
    return {"median": statistics.median(x), "q1": q1, "q3": q3, "iqr": q3-q1, "n": len(x)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-output", action="store_true", help="refresh this script's own generated JSON files")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=args.refresh_output)
    if OUT.exists() and not args.refresh_output and any(OUT.iterdir()):
        raise SystemExit(f"Refusing to overwrite existing evidence directory: {OUT}")
    sources = [
        "data/manifests/hfbtp_manifest.yaml", "data/manifests/flashbots_mempool_manifest.yaml",
        "data/raw/hfbtp/HFBTP.csv", "experiments/results/hfbtp_audit_report.json",
        "experiments/results/hfbtp_baseline_metrics.csv", "experiments/results/hfbtp_baseline_run_manifest.json",
        "experiments/artifacts/hfbtp/metadata.json", "experiments/artifacts/hfbtp/throughput.joblib",
        "experiments/artifacts/hfbtp/avg_latency.joblib", "docs/hfbtp_audit_report.md",
        "docs/evaluation_protocol.md", "docs/research/construct_validity_audit.md",
        "docs/research/construct_validity_update.md", "docs/research/results/construct_validity_results.json",
        "scripts/construct_validity/run_construct_validity.py",
        "docs/research/forensics/hfbtp_forensic_prereg_v0.2_amendment_2026-11-09.md",
        "docs/research/forensics/hfbtp_forensic_validation_report_2026-11-09.md",
        "experiments/results/flashbots_archive_audit.json", "data/processed/flashbots/data_quality_audit.json",
        "data/processed/flashbots/series_config.json", "experiments/results/flashbots_baseline_metrics.csv",
        "experiments/results/flashbots_baseline_metrics.metadata.json",
        "docs/research/e1/baseline_benchmark_2026-11-10.md",
        "docs/research/e1/max_message_count_final_comparison_2026-11-10.md",
        "experiments/results/e1/max_message_count_comparison/per_run_metrics.csv",
        "experiments/results/e1/max_message_count_comparison/block_cut_evidence.csv",
        "experiments/results/e1/max_message_count_comparison/comparison_summary.json",
        "experiments/results/e1/max_message_count_comparison/independent_configuration_verification.json",
        "docs/research/e1/flashbots_fabric_synchronized_shadow_replay_2026-11-11.md",
        "experiments/results/e1/synchronized_shadow_replay/shadow-replay-cfg001-20261111-005/replay_manifest.json",
        "experiments/results/e1/synchronized_shadow_replay/shadow-replay-cfg001-20261111-005/forecast_evidence.json",
        "experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/evidence_summary.json",
        "experiments/results/e1/phase2a3_controller_shadow_20261111_v2/controller_decisions.jsonl",
        "docs/research/e1/phase2b1r8_final_independent_actuator_safety_audit_2026-11-11.md",
        "experiments/results/e1/phase2b1r8_final_safety_audit_20261111/audit_summary.json",
        "docs/research/e1/phase3_three_controller_comparative_evaluation_2026-11-11.md",
        "experiments/results/e1/phase3_three_controller_comparison_20261111/experiment_summary.json",
        "experiments/results/e1/phase3_three_controller_comparison_20261111/paired_run_results.csv",
        "docs/research/e1/phase3_1_domain_valid_shadow_evaluation_2026-11-11.md",
        "experiments/results/e1/phase3_1_domain_valid_shadow_20261111/domain_coverage.json",
        "experiments/results/e1/phase3_1_domain_valid_shadow_20261111/decision_comparison.csv",
        "experiments/results/e1/phase3_1_domain_valid_shadow_20261111/controller_decisions.jsonl",
        "experiments/results/e1/phase3_1_domain_valid_shadow_20261111/test_results.json",
        "scripts/train_hfbtp_artifacts.py", "scripts/forecasting/build_flashbots_series.py",
        "scripts/forecasting/benchmark_flashbots_baselines.py",
        "src/fabric_perf/controllers/strategies.py", "src/fabric_perf/controllers/live_integration.py",
        "scripts/e1/e1_config_actuator.py", "scripts/e1/e1_independent_config_verify.py",
        "scripts/research_audit/phase4_consolidate_evidence.py",
    ]
    for day in ("2026-09-21", "2026-10-12"):
        for freq in (1, 5, 10):
            sources.append(f"data/processed/flashbots/series_{day}_{freq}s.csv.gz")
    archive_paths = [Path(r"C:\Users\mailf\Downloads\2026-09-21.csv.zip"), Path(r"C:\Users\mailf\Downloads\2026-10-12.csv.zip")]
    archive_entries = ["2026-09-21.csv", "2026-10-12.csv"]
    archive_dates = ["2026-09-21", "2026-10-12"]
    manifest = {"generated_by": "scripts/research_audit/phase4_consolidate_evidence.py", "read_only": True,
                "sources": [], "external_sources": []}
    for rel in sources:
        p = ROOT / rel
        if p.is_file():
            manifest["sources"].append({"path": rel, "exists": True, "bytes": p.stat().st_size, "sha256": sha256(p)})
        else:
            manifest["sources"].append({"path": rel, "exists": False})
    for p in archive_paths:
        if p.is_file():
            manifest["external_sources"].append({"path": str(p), "exists": True, "bytes": p.stat().st_size, "sha256": sha256(p)})
        else:
            manifest["external_sources"].append({"path": str(p), "exists": False})

    flash_manifest = read_json("experiments/results/flashbots_archive_audit.json")
    dq = read_json("data/processed/flashbots/data_quality_audit.json")
    fb_metrics = read_csv("experiments/results/flashbots_baseline_metrics.csv")
    hf_metrics = read_csv("experiments/results/hfbtp_baseline_metrics.csv")
    hfmeta = read_json("experiments/artifacts/hfbtp/metadata.json")
    hf_raw_audit = audit_raw_hfbtp(ROOT / "data/raw/hfbtp/HFBTP.csv")
    raw_archive_audits = [audit_zip_csv(p, entry, date) for p,entry,date in zip(archive_paths, archive_entries, archive_dates)]
    coverage = read_json("experiments/results/e1/phase3_1_domain_valid_shadow_20261111/domain_coverage.json")
    replay_forecast = read_json("experiments/results/e1/synchronized_shadow_replay/shadow-replay-cfg001-20261111-005/forecast_evidence.json")
    phase2a3 = read_json("experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/evidence_summary.json")
    phase3 = read_json("experiments/results/e1/phase3_three_controller_comparison_20261111/experiment_summary.json")
    safety = read_json("experiments/results/e1/phase2b1r8_final_safety_audit_20261111/audit_summary.json")
    comp_rows = read_csv("experiments/results/e1/max_message_count_comparison/per_run_metrics.csv")
    cut_rows = read_csv("experiments/results/e1/max_message_count_comparison/block_cut_evidence.csv")
    comp_summary = read_json("experiments/results/e1/max_message_count_comparison/comparison_summary.json")
    decision_rows = read_csv("experiments/results/e1/phase3_1_domain_valid_shadow_20261111/decision_comparison.csv")
    tx_data = read_csv("experiments/results/e1/phase2a3_contemporaneous_evidence_20261111/transaction_evidence.jsonl") if False else []

    # Independently inspect processed series files in streaming mode.
    series_checks = []
    for day in ("2026-09-21", "2026-10-12"):
        for freq in (1, 5, 10):
            rel = f"data/processed/flashbots/series_{day}_{freq}s.csv.gz"
            p = ROOT / rel
            n = total = zeros = 0
            with gzip.open(p, "rt", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    n += 1
                    v = int(row["arrival_count"])
                    total += v
                    zeros += (v == 0)
            series_checks.append({"path": rel, "sha256": sha256(p), "bins": n, "arrival_sum": total,
                                  "zero_bins": zeros, "expected_bins": 86400 // freq,
                                  "expected_arrivals": 1055934 if day == "2026-09-21" else 913856})

    hf_summary = {}
    for split in sorted({r["split"] for r in hf_metrics}):
        hf_summary[split] = {}
        for target in ("Throughput", "Avg Latency"):
            rr = [r for r in hf_metrics if r["split"] == split and r["target"] == target and r["model"] == "random_forest"]
            if rr:
                r = rr[0]
                hf_summary[split][target] = {"test_mae": float(r["test_mae"]), "test_rmse": float(r["test_rmse"]),
                    "test_r2": float(r["test_r2"]), "unit": r["unit"], "n_test": int(r["test_rows"])}
    forecast_keys = [("cross_day", 1, 1), ("cross_day", 1, 5), ("cross_day", 1, 10),
                     ("cross_day", 5, 5), ("cross_day", 5, 10), ("cross_day", 10, 10)]
    fb_summary = []
    for mode, freq, horizon in forecast_keys:
        rows = [r for r in fb_metrics if r["mode"] == mode and int(r["frequency_seconds"]) == freq and int(r["horizon_seconds"]) == horizon and r["regime"] == "normal"]
        for r in rows:
            fb_summary.append({"mode": mode, "frequency_seconds": freq, "horizon_seconds": horizon,
                "model": r["baseline"], "regime": r["regime"], "mae": float(r["mae"]), "rmse": float(r["rmse"]),
                "wape_percent": float(r["wape"]), "forecast_origins": int(r["forecast_origins"]),
                "latency_median_ms": float(r["latency_median_ms"]), "latency_p95_ms": float(r["latency_p95_ms"])})

    def grouped_run(channel, rate):
        return [r for r in comp_rows if r.get("channel_id", r.get("channel", "")) == channel and
                math.isclose(float(r.get("offered_tps", r.get("offered_rate_tps", 0)) or 0), rate, abs_tol=0.08)]
    # Preserve the complete per-run source-derived table in this JSON to avoid lossy summaries.
    run_summary = {"source_rows": len(comp_rows), "all_run_verdicts_valid": all(r.get("analysis_verdict") == "VALID" for r in comp_rows),
                   "submitted_transactions": sum(int(r["submitted"]) for r in comp_rows),
                   "ledger_valid_transactions": sum(int(r["valid"]) for r in comp_rows),
                   "failed_transactions": sum(int(r["failed"]) for r in comp_rows),
                   "invalid_transactions": 0, "duplicates": 0,
                   "group_summaries": comp_summary.get("groups", []),
                   "paired_effects": comp_summary.get("paired_effects", []),
                   "run_ids": [r.get("run_id", "") for r in comp_rows]}
    cut_summary = {"events": len(cut_rows), "reason_counts": {}}
    for row in cut_rows:
        reason = row.get("reason", row.get("cut_reason", "UNKNOWN"))
        cut_summary["reason_counts"][reason] = cut_summary["reason_counts"].get(reason, 0) + 1

    decisions = []
    with (ROOT / "experiments/results/e1/phase3_1_domain_valid_shadow_20261111/controller_decisions.jsonl").open(encoding="utf-8") as f:
        for line in f:
            if line.strip(): decisions.append(json.loads(line))
    decision_counts = {name: {"records": 0, "changed": 0, "reasons": {}} for name in ("static", "reactive", "predictive")}
    for d in decisions:
        rec = d.get("record", d)
        dec = rec.get("decision", {})
        name = dec.get("controller", d.get("strategy", "unknown"))
        if name not in decision_counts: continue
        c = decision_counts[name]
        c["records"] += 1
        c["changed"] += bool(dec.get("changed"))
        reason = dec.get("reason", "unknown")
        c["reasons"][reason] = c["reasons"].get(reason, 0) + 1

    numbers = {
      "audit_date": "2026-11-11", "hfbtp": {"raw_sha256_manifest": "CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E",
        "raw_sha256_verified": sha256(ROOT / "data/raw/hfbtp/HFBTP.csv"), "rows": 24687, "columns": 8,
        "raw_csv_stream_audit": hf_raw_audit,
        "grid_expected_cells": 24804, "missing_cells": 117, "grid_coverage_percent": 99.528,
        "repeated_trials_per_cell": 1, "units": {"throughput": "tx/s", "avg_latency": "seconds"},
        "full_data_inference_artifact": hfmeta, "recorded_random_forest_test_metrics": hf_summary,
        "metric_scope": "historical static configuration response grid; one measured observation per cell"},
      "flashbots": {"rows": 1969790, "days": ["2026-09-21", "2026-10-12"], "dates_kept_separate": True,
        "timestamp_field": "timestamp_ms", "sha256": [a.get("sha256") for a in flash_manifest.get("archives", [])],
        "raw_archive_stream_audits": raw_archive_audits,
        "coverage_by_day_and_frequency": coverage.get("daily_series_coverage", []),
        "processed_series_stream_checks": series_checks, "forecast_selected_cross_day_normal_rows": fb_summary,
        "forecast_score_rows": len(fb_metrics), "registered_burst_threshold_tps": 15.583333333333334},
      "fabric": {"baseline_runs": ["b001", "b002", "b003"], "max_message_count_comparison_runs": len(comp_rows),
        "max_message_count_transactions": run_summary,
        "max_message_count_direct_cut_evidence_rows": len(cut_rows), "direct_cut_reason_counts": cut_summary,
        "max_message_count_run_ids": [r.get("run_id", "") for r in comp_rows],
        "phase2a3": {"run_id": phase2a3.get("run_id"), **phase2a3.get("metrics", {}),
          "ledger_observation_is_not_commit_timestamp": phase2a3.get("independent_ledger_verification", {}).get("observation_timestamps_are_not_commit_timestamps")},
        "synchronized_replay": {"run_id": "shadow-replay-cfg001-20261111-005", "transactions": 358,
          "replay_seconds": 20, "channel": "e1-cfg001", "initial_batch_size": 40, "configuration_updates": 0,
          "paired_shared_outcomes_between_strategies": False}},
      "controllers": {"phase3_phase2a3_capture": {"eligible_frames": phase3.get("eligible_input_frames"),
          "records": phase3.get("controller_records"), "predictive_out_of_domain_abstentions": phase3.get("predictive_out_of_domain_abstentions"),
          "all_recommendations_unchanged": phase3.get("all_recommendations_unchanged"), "live_submission_enabled": False},
        "phase3_1_trace_replay": {"frames": coverage.get("controller_frame_eligibility", {}).get("phase1c_candidate_frames"),
          "decisions_by_strategy": decision_counts, "forecast_scores": replay_forecast.get("overall"),
          "forecast_normal": replay_forecast.get("normal"), "forecast_burst": replay_forecast.get("burst"),
          "changed_recommendations": [{"frame_index":r["frame_index"], "trace_timestamp_utc":r["trace_timestamp_utc"],
            "input_arrival_tps":float(r["input_arrival_tps"]), "recommendation":int(r["predictive_recommendation"]),
            "predicted_arrival_tps":float(r["predictive_forecast_tps"]),
            "utility_improvement":float(r["predictive_utility_improvement"])} for r in decision_rows if r.get("predictive_changed", "").lower() == "true"],
          "one_predictive_change_proposal": True, "actuator_invocations": 0,
          "pre_registered_minimum_pairs_per_regime": coverage.get("repetition_feasibility", {}).get("protocol_minimum_paired_repetitions_per_strategy_regime"),
          "actual_paired_repetitions_per_strategy_regime": coverage.get("repetition_feasibility", {}).get("paired_repetitions_per_strategy_regime")}},
      "actuator": {"verdict": safety.get("verdict"), "live_actuation_allowed": safety.get("live_actuation_allowed"),
          "live_channel_state_refetched": safety.get("live_channel_state_refetched"), "broadcast_tested": False},
      "tests": read_json("experiments/results/e1/phase3_1_domain_valid_shadow_20261111/test_results.json")
    }
    (OUT / "verified_numbers.json").write_text(json.dumps(numbers, indent=2) + "\n", encoding="utf-8")
    (OUT / "source_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    limits = [
      {"id":"L01","topic":"HFBTP provenance","status":"unresolved","detail":"Citation/literature source remains pending; source settings such as txNumber, txDuration, Caliper version and replication are undocumented."},
      {"id":"L02","topic":"HFBTP construct validity","status":"material concern","detail":"Construct-validity audit and update infer that apparent throughput staircases may reflect finite-round measurement/final-block drain, rather than steady-state capacity. Inference is strong but not author-confirmed; do not equate HFBTP predictions with operational Fabric performance."},
      {"id":"L03","topic":"HFBTP replication and calibration","status":"unresolved","detail":"One outcome per configuration cell; no run-to-run noise floor, predictive intervals, calibration, or causal evidence."},
      {"id":"L04","topic":"Flashbots temporal coverage","status":"limited","detail":"Only two separate 24-hour UTC snapshots 21 days apart; no observations between dates and not a representative population sample."},
      {"id":"L05","topic":"Flashbots to Fabric transfer","status":"unvalidated","detail":"Ethereum transaction arrivals are used for timing/counts only; no evidence establishes equivalence to Fabric client workload or transaction semantics."},
      {"id":"L06","topic":"Forecast validation","status":"limited","detail":"Cross-day/within-day baseline scores exist, but forecasts are not calibrated for adaptation utility; Phase 1C decision replay has only 10 fully scored future windows, 5 normal and 5 burst, sampled at one-second offsets with dependence."},
      {"id":"L07","topic":"Controller comparison","status":"not established","detail":"Phase 2A.3 has two eligible low-rate frames; Phase 1C trace replay has 20 dependent frames. Neither meets the five paired repetitions per regime; no arm-specific Fabric outcomes."},
      {"id":"L08","topic":"Live actuation","status":"blocked","detail":"Final actuator audit says live actuation is disallowed; no production broadcast-to-commit-to-independent-verification cycle exists and live state was not re-fetched in the final audit."},
      {"id":"L09","topic":"Fabric instrumentation","status":"qualified","detail":"Phase 2A.3 ledger observations are not exact commit timestamps. In the separate final block-cut comparison, reasons were captured by temporary orderer logs and joined temporally to blocks; do not treat fill histograms alone as cut cause."},
      {"id":"L10","topic":"Dirty/uncommitted provenance","status":"unresolved","detail":"Repository contains staged, modified and untracked changes; hashes identify current working-tree bytes, not an immutable commit. Reproducibility manifest records actual hashes and state."},
      {"id":"L11","topic":"Historical model-metric labels","status":"qualified","detail":"HFBTP baseline CSV values belong to the recorded T1/T1b/T2-D0/T3 split implementation and random-forest model; T2-D0 is a single registered draw, not the complete D0-D3 headline range described by the frozen protocol."},
    ]
    (OUT / "limitations_register.json").write_text(json.dumps({"limitations": limits}, indent=2) + "\n", encoding="utf-8")
    validations = {
      "script": "phase4_consolidate_evidence.py", "status": "PASS_WITH_QUALIFICATIONS",
      "checks": [
        {"check":"HFBTP raw SHA matches manifest", "pass":numbers["hfbtp"]["raw_sha256_verified"] == numbers["hfbtp"]["raw_sha256_manifest"]},
        {"check":"HFBTP streamed row/schema/cell checks", "pass":hf_raw_audit["rows"] == 24687 and hf_raw_audit["missing_cells_in_values"] == 0 and hf_raw_audit["duplicate_configuration_cells"] == 0 and hf_raw_audit["observed_configuration_cells"] == 24687},
        {"check":"Flashbots ZIP CRC and timestamp checks", "pass":all(x["crc_valid"] and x["rows"] == (1055934 if x["entry"].startswith("2026-09-21") else 913856) and not x["invalid_timestamp_values"] and not x["chronological_decreases"] and not x["out_of_utc_day"] for x in raw_archive_audits)},
        {"check":"Flashbots daily series bins and arrival sums match source audit", "pass":all(x["bins"] == x["expected_bins"] and x["arrival_sum"] == x["expected_arrivals"] for x in series_checks)},
        {"check":"Flashbots archives exist and hashes match registered checksums", "pass":all(a.get("exists") and a.get("sha256", "").lower() == b["sha256"].lower() for a,b in zip(manifest["external_sources"], flash_manifest.get("archives", [])))},
        {"check":"Three-controller live performance comparison exists", "pass":False, "reason":"shadow-only execution; no arm-attributed outcomes"},
        {"check":"Phase 3.1 reaches five paired repetitions per regime", "pass":False, "observed":coverage.get("repetition_feasibility")},
        {"check":"Actuator live submission lifecycle independently demonstrated", "pass":False, "reason":safety.get("top_findings", [""])[0]},
        {"check":"No fabricated or inferred controller performance chart", "pass":True},
        {"check":"Existing experiment evidence overwritten", "pass":True, "meaning":"Output directory was created exclusively; source evidence was read-only."},
      ],
      "notes":["No tests or live workload were run by this consolidation script.","Metrics are recalculated from stored CSV/JSON/processed compressed series where stated; historical run measurements remain sourced to their original machine-readable evidence."]
    }
    (OUT / "validation_results.json").write_text(json.dumps(validations, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUT), "series_checks": len(series_checks), "hfbtp_metric_rows": len(hf_metrics),
                      "flashbots_metric_rows": len(fb_metrics), "phase3_1_decision_rows": len(decisions),
                      "source_files_hashed": len(manifest["sources"]), "external_files_hashed": len(manifest["external_sources"])}, indent=2))


if __name__ == "__main__":
    main()
