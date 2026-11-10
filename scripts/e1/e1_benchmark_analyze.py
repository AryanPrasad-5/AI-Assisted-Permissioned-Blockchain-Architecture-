"""Analyze one or more reproducible Caliper E1 fixed-count runs.

The analyzer consumes only recorded artifacts: the per-transaction recorder CSV,
submit-event CSV, recorder manifest, fetched ledger table, and optional E1
telemetry summary. It does not contact Fabric and refuses to overwrite output.

Usage:
  python e1_benchmark_analyze.py analyze --run-id b001 --recorder-base <base>
      --ledger-tx <ledger_tx.csv> --metrics-summary <metrics_summary.json>
      --out-dir <new-dir>
  python e1_benchmark_analyze.py aggregate --input <summary.json> --input <summary.json>
      --out-dir <new-dir>
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from e1_reconcile import Expectations, reconcile, verdict  # noqa: E402


def read_csv(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def json_x(path: Path, obj) -> str:
    data = (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with path.open("xb") as fh:
        fh.write(data)
    return hashlib.sha256(data).hexdigest()


def p95_nearest_rank(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    return statistics.quantiles(sorted(values), n=100, method="inclusive")[max(0, int(q * 100) - 1)] \
        if len(values) >= 2 else values[0]


def max_backlog(records: list[dict], submits: list[dict]) -> int:
    events: list[tuple[float, int, int]] = []
    for r in records:
        if r.get("final_ms") not in (None, ""):
            events.append((float(r["final_ms"]), 0, -1))  # finish before same-time submit
    for s in submits:
        events.append((float(s["t_submit_evt_ms"]), 1, int(s["count"])))
    outstanding = peak = 0
    for _, _, delta in sorted(events):
        outstanding = max(0, outstanding + delta)
        peak = max(peak, outstanding)
    return peak


def analyze(args: argparse.Namespace) -> dict:
    base = Path(args.recorder_base)
    tx_path = base.with_name(base.name + ".csv")
    submit_path = base.with_name(base.name + ".submits.csv")
    manifest_path = base.with_name(base.name + ".manifest.json")
    ledger_path = Path(args.ledger_tx)
    records, submits = read_csv(tx_path), read_csv(submit_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    ledger = read_csv(ledger_path)

    ids = {r.get("tx_id") for r in records if r.get("tx_id")}
    endorser = [r for r in ledger if r.get("header_type") == "3"]
    foreign = {r["tx_id"] for r in endorser if r.get("tx_id") not in ids}
    other_ids = set()
    for other_csv in args.other_round_csv:
        other_ids |= {r.get("tx_id") for r in read_csv(Path(other_csv)) if r.get("tx_id")}
    exp = Expectations(n=int(args.expected_n), max_message_count=int(args.max_message_count),
                       preferred_max_bytes=int(args.preferred_max_bytes),
                       event_strategy=args.event_strategy, invoke_timeout_s=float(args.invoke_timeout_s),
                       binding=args.binding, clock_offset_bound_ms=float(args.clock_offset_ms),
                       other_round_tx_ids=foreign | other_ids, expect_all_success=True, expect_block_pattern=False)
    checks = reconcile(records, submits, manifest, ledger, exp)
    check_rows = [c.__dict__ for c in checks]

    measured = []
    for r in records:
        create, final = float(r["create_ms"]), float(r["final_ms"])
        measured.append({"tx_id": r.get("tx_id", ""), "seq_finish": int(r["seq_finish"]),
                         "create_ms": create, "final_ms": final, "latency_ms": final - create,
                         "status": r.get("status", ""), "verified": r.get("verified", ""),
                         "error": r.get("err", "")})
    success = [r for r in measured if r["status"] == "success"]
    latencies = [r["latency_ms"] for r in success]
    submit_times = [float(s["t_submit_evt_ms"]) for s in submits]
    submit_span_s = (max(submit_times) - min(submit_times)) / 1000 if len(submit_times) > 1 else None
    clock = manifest.get("clock", {})
    wall_elapsed_s = float(clock.get("wall_elapsed_ms", 0)) / 1000
    offered_tps = sum(int(s["count"]) for s in submits) / submit_span_s if submit_span_s else None
    delivered_tps = len(success) / wall_elapsed_s if wall_elapsed_s > 0 else None
    valid_ledger = sum(1 for r in endorser if r.get("tx_id") in ids and r.get("validation_code") == "0")
    metrics = {}
    if args.metrics_summary:
        metrics = json.loads(Path(args.metrics_summary).read_text(encoding="utf-8"))

    summary = {
        "tool": "e1_benchmark_analyze 0.1",
        "run_id": args.run_id,
        "inputs": {"recorder_csv": {"path": str(tx_path), "sha256": sha256(tx_path)},
                    "submits_csv": {"path": str(submit_path), "sha256": sha256(submit_path)},
                    "manifest": {"path": str(manifest_path), "sha256": sha256(manifest_path)},
                    "ledger_tx": {"path": str(ledger_path), "sha256": sha256(ledger_path)},
                    "metrics_summary": ({"path": str(args.metrics_summary), "sha256": sha256(Path(args.metrics_summary))}
                                         if args.metrics_summary else None)},
        "configuration": {"n": int(args.expected_n), "rate_tps": float(args.rate_tps),
                           "B": int(args.max_message_count), "tau": args.batch_timeout,
                           "P": int(args.preferred_max_bytes), "binding": args.binding,
                           "event_strategy": args.event_strategy, "invoke_timeout_s": float(args.invoke_timeout_s)},
        "counts": {"submitted": sum(int(s["count"]) for s in submits), "finished": len(records),
                   "success": len(success), "failed": len(records) - len(success),
                   "valid_workload_ledger_transactions": valid_ledger,
                   "foreign_endorser_transactions_declared": len(foreign),
                   "pending_at_deactivate": manifest.get("counts", {}).get("pending_at_deactivate"),
                   "duplicate_ids": manifest.get("counts", {}).get("duplicate_ids")},
        "metrics": {"latency_ms_mean": statistics.mean(latencies) if latencies else None,
                    "latency_ms_median": statistics.median(latencies) if latencies else None,
                    "latency_ms_p95_nearest_rank": p95_nearest_rank(latencies),
                    "latency_ms_min": min(latencies) if latencies else None,
                    "latency_ms_max": max(latencies) if latencies else None,
                    "offered_tps_from_submit_events": offered_tps,
                    "delivered_tps_from_round_wall_time": delivered_tps,
                    "success_rate": len(success) / len(records) if records else None,
                    "submit_span_s": submit_span_s, "round_wall_elapsed_s": wall_elapsed_s,
                    "max_inflight_backlog": max_backlog(records, submits)},
        "telemetry": {"status": metrics.get("status"), "traffic": metrics.get("traffic", {}),
                      "derived": metrics.get("derived", {}), "anomalies": metrics.get("anomalies", {})},
        "reconciliation": {"verdict": verdict(checks), "checks": check_rows},
        "transaction_measurements": measured,
    }
    summary["analysis_verdict"] = "VALID" if summary["reconciliation"]["verdict"] == "VALID" \
        and summary["telemetry"].get("status") in (None, "COMPLETE", "COMPLETE_WITH_WARNINGS") else "INVALID"

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=False)
    csv_path = out / "transaction_measurements.csv"
    with csv_path.open("x", newline="", encoding="utf-8") as fh:
        fields = ["tx_id", "seq_finish", "create_ms", "final_ms", "latency_ms", "status", "verified", "error"]
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader(); w.writerows(measured)
    summary["outputs"] = {"transaction_measurements.csv": sha256(csv_path)}
    summary["outputs"]["summary.json"] = json_x(out / "summary.json", summary)
    json_x(out / "reconciliation.json", summary["reconciliation"])
    print(json.dumps({"run_id": args.run_id, "analysis_verdict": summary["analysis_verdict"],
                      "success": len(success), "failed": len(records) - len(success),
                      "latency_mean_ms": summary["metrics"]["latency_ms_mean"],
                      "latency_p95_ms": summary["metrics"]["latency_ms_p95_nearest_rank"],
                      "offered_tps": offered_tps, "delivered_tps": delivered_tps}, indent=2))
    return summary


def aggregate(args: argparse.Namespace) -> None:
    docs = [json.loads(Path(p).read_text(encoding="utf-8")) for p in args.input]
    fields = ["latency_ms_mean", "latency_ms_p95_nearest_rank", "offered_tps_from_submit_events",
              "delivered_tps_from_round_wall_time", "success_rate", "max_inflight_backlog"]
    per_run = [{"run_id": d["run_id"], **{f: d["metrics"].get(f) for f in fields},
                "analysis_verdict": d.get("analysis_verdict")} for d in docs]
    stats = {}
    for f in fields:
        vals = [float(r[f]) for r in per_run if r[f] is not None]
        qs = statistics.quantiles(vals, n=4, method="inclusive") if len(vals) >= 2 else []
        stats[f] = {"median": statistics.median(vals) if vals else None,
                    "q1": qs[0] if qs else (vals[0] if vals else None),
                    "q3": qs[2] if qs else (vals[0] if vals else None),
                    "iqr": (qs[2] - qs[0]) if qs else 0 if vals else None}
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=False)
    result = {"tool": "e1_benchmark_analyze 0.1 aggregate", "runs": per_run,
              "repeatability": stats, "all_runs_valid": all(r["analysis_verdict"] == "VALID" for r in per_run)}
    json_x(out / "repeatability_summary.json", result)
    with (out / "per_run_metrics.csv").open("x", newline="", encoding="utf-8") as fh:
        fields_out = ["run_id"] + fields + ["analysis_verdict"]
        w = csv.DictWriter(fh, fieldnames=fields_out); w.writeheader(); w.writerows(per_run)
    print(json.dumps(result, indent=2))


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="command", required=True)
    a = sub.add_parser("analyze")
    a.add_argument("--run-id", required=True); a.add_argument("--recorder-base", required=True)
    a.add_argument("--ledger-tx", required=True); a.add_argument("--metrics-summary")
    a.add_argument("--out-dir", required=True); a.add_argument("--expected-n", type=int, default=20)
    a.add_argument("--max-message-count", type=int, default=45); a.add_argument("--preferred-max-bytes", type=int, default=10485760)
    a.add_argument("--batch-timeout", default="2s"); a.add_argument("--rate-tps", type=float, default=5.0)
    a.add_argument("--event-strategy", default="msp_any"); a.add_argument("--invoke-timeout-s", type=float, default=60)
    a.add_argument("--binding", default="fabric:2.5"); a.add_argument("--clock-offset-ms", type=float, default=1.0)
    a.add_argument("--other-round-csv", action="append", default=[])
    g = sub.add_parser("aggregate"); g.add_argument("--input", action="append", required=True); g.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    if args.command == "analyze":
        analyze(args); return 0
    aggregate(args); return 0


if __name__ == "__main__":
    raise SystemExit(main())
