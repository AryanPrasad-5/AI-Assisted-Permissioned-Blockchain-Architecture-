"""Produce auditable machine-readable evidence for a completed bounded replay."""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path


BURST_THRESHOLD_TPS = 15.583333333333334  # existing Sep-21 development split; not recomputed from the test segment
HORIZON_SECONDS = 10


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def p95(values: list[float]) -> float | None:
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(0.95 * len(values)) - 1)]


def forecast_metrics(run: Path) -> dict:
    snapshots = [json.loads(line) for line in (run / "controller_snapshots.jsonl").read_text(encoding="utf-8").splitlines() if line]
    predictive = json.loads((run / "controller_shadow_runs" / "predictive.json").read_text(encoding="utf-8"))
    rows = [s["trace_bin"] for s in snapshots]
    points = []
    for index, record in enumerate(predictive):
        predicted = record["decision"].get("predicted_arrival_rate_tps")
        if predicted is None or index + 1 >= len(rows):
            continue
        future = [float(row["arrival_count"]) for row in rows[index + 1:index + 1 + HORIZON_SECONDS]]
        if len(future) != HORIZON_SECONDS:
            continue
        actual = sum(future) / len(future)
        regime = "burst" if actual > BURST_THRESHOLD_TPS else "normal"
        points.append({"origin_index": index, "origin_timestamp_utc": snapshots[index]["timestamp_utc"],
                       "horizon_seconds": HORIZON_SECONDS, "predicted_tps": float(predicted),
                       "actual_future_mean_tps": actual, "absolute_error_tps": abs(float(predicted) - actual),
                       "regime": regime})

    def metrics(items: list[dict]) -> dict:
        if not items:
            return {"forecast_origins": 0, "mae_tps": None, "rmse_tps": None, "wape_percent": None}
        errors = [p["predicted_tps"] - p["actual_future_mean_tps"] for p in items]
        actual = [p["actual_future_mean_tps"] for p in items]
        return {"forecast_origins": len(items), "mae_tps": sum(abs(v) for v in errors) / len(errors),
                "rmse_tps": math.sqrt(sum(v * v for v in errors) / len(errors)),
                "wape_percent": 100 * sum(abs(v) for v in errors) / sum(abs(v) for v in actual) if sum(abs(v) for v in actual) else None}

    forecast_latencies = []
    hfbtp_latencies = []
    decision_latencies = []
    for record in predictive:
        decision_latencies.append(float(record["decision"].get("decision_overhead_ms", 0)))
        forecast_latencies.append(float(record["decision"].get("metadata", {}).get("forecast", {}).get("inference_duration_ms", 0)))
        hfbtp_latencies.append(float(record["decision"].get("metadata", {}).get("performance_predictor", {}).get("inference_duration_ms", 0)))
    return {"source": "predictive controller shadow records; actual future bins are read only after each origin",
            "threshold": {"value_tps": BURST_THRESHOLD_TPS, "source": "existing Sep-21 development split", "test_segment_not_used": True},
            "horizon_seconds": HORIZON_SECONDS, "points": points,
            "overall": metrics(points), "normal": metrics([p for p in points if p["regime"] == "normal"]),
            "burst": metrics([p for p in points if p["regime"] == "burst"]),
            "availability": {"decision_records": len(predictive), "forecast_available": sum(1 for r in predictive if r["decision"].get("predicted_arrival_rate_tps") is not None),
                              "forecast_availability_fraction": sum(1 for r in predictive if r["decision"].get("predicted_arrival_rate_tps") is not None) / len(predictive) if predictive else 0},
            "latency_ms": {"forecast_median": statistics.median(forecast_latencies) if forecast_latencies else None,
                           "forecast_p95": p95(forecast_latencies), "hfbtp_median": statistics.median(hfbtp_latencies) if hfbtp_latencies else None,
                           "hfbtp_p95": p95(hfbtp_latencies), "decision_median": statistics.median(decision_latencies) if decision_latencies else None,
                           "decision_p95": p95(decision_latencies)}}


def block_evidence(run: Path) -> dict:
    blocks = read_csv(run / "ledger" / "ledger" / "blocks.csv")
    tx = read_csv(run / "ledger" / "ledger" / "ledger_tx.csv")
    metrics_summary = json.loads((run / "metrics_summary" / "metrics_summary.json").read_text(encoding="utf-8"))
    by_block: dict[str, list[dict]] = {}
    for row in tx:
        by_block.setdefault(row["block"], []).append(row)
    evidence = []
    previous_ms = None
    for row in blocks:
        timestamps = [int(t["ch_ts_ms"]) for t in by_block.get(row["number"], []) if str(t.get("ch_ts_ms", "")).isdigit()]
        block_ms = max(timestamps) if timestamps else None
        delta = None if previous_ms is None or block_ms is None else (block_ms - previous_ms) / 1000
        evidence.append({"block_number": int(row["number"]), "transaction_count": int(row["tx_count"]),
                         "inferred_cut_cause": row["inferred_cut_cause"], "block_timestamp_utc":
                         datetime.fromtimestamp(block_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds") if block_ms is not None else None,
                         "block_timestamp_source": "maximum channel-header timestamp in verified ledger block",
                         "timestamp_precision": "milliseconds", "inter_block_timestamp_delta_s": delta})
        if block_ms is not None:
            previous_ms = block_ms
    derived = metrics_summary.get("derived", {})
    cutters = metrics_summary.get("traffic", {}).get("block_cutters", {})
    fill_count = sum(float(v.get("fill_duration_count_delta", 0)) for v in derived.values())
    fill_sum = sum(float(v.get("fill_duration_sum_delta_s", 0)) for v in derived.values())
    return {"source": "verified ledger block reader plus channel-header timestamps", "blocks": evidence,
            "transaction_count_distribution": {str(k): sum(1 for e in evidence if e["transaction_count"] == k)
                                                for k in sorted({e["transaction_count"] for e in evidence})},
            "cause_status": "inferred_only; no direct MESSAGE_COUNT/TIMEOUT cut event was captured",
            "prometheus_cut_count": sum(float(v) for v in cutters.values()),
            "prometheus_fill_mean_s": fill_sum / fill_count if fill_count else None,
            "prometheus_fill_duration_count": fill_count,
            "prometheus_metrics_source": str(run / "metrics_summary" / "metrics_summary.json")}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-dir", required=True, type=Path)
    args = ap.parse_args()
    run = args.run_dir
    record = json.loads((run / "replay_record.json").read_text(encoding="utf-8"))
    analysis = record["analysis"]
    blocks = block_evidence(run)
    forecasts = forecast_metrics(run)
    dispatch = read_csv(run / "replay_dispatch.csv")
    metrics_summary = json.loads((run / "metrics_summary" / "metrics_summary.json").read_text(encoding="utf-8"))
    telemetry = {"channel": record["channel"], "configuration_unchanged": record["configuration_unchanged"],
                 "caliper_submitted": analysis["counts"]["submitted"], "caliper_finished": analysis["counts"]["finished"],
                 "valid_ledger_transactions": analysis["counts"]["valid_workload_ledger_transactions"],
                 "dispatch_rows": len(dispatch), "unique_dispatch_sequences": len({int(r["seq"]) for r in dispatch}),
                 "metrics_scrape_status": metrics_summary.get("status"), "metrics_traffic": metrics_summary.get("traffic"),
                 "missing_or_limited": ["Prometheus does not expose client latency/backlog; Caliper recorder supplies those metrics",
                                        "Prometheus block fill is not client transaction latency",
                                        "block count/cause comes from ledger/inference; direct cut reason is absent",
                                        "historical trace timestamps and live capture timestamps are preserved separately"]}
    out = {"tool": "analyze_synchronized_shadow_replay 0.1", "run_id": record["run_id"], "channel": record["channel"],
           "replay": {"transactions": record["trace"]["transactions"], "trace_bins": len(record["trace"]["bin_rows"]),
                       "configuration_unchanged": record["configuration_unchanged"], "live_configuration_submission": False},
           "validity": {"analysis_verdict": analysis["analysis_verdict"], "success_rate": analysis["metrics"]["success_rate"],
                        "duplicate_ids": analysis["counts"]["duplicate_ids"], "pending_at_deactivate": analysis["counts"]["pending_at_deactivate"]},
           "forecast_evidence": forecasts, "block_evidence": blocks, "telemetry_alignment": telemetry}
    (run / "forecast_evidence.json").write_text(json.dumps(forecasts, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (run / "block_evidence.json").write_text(json.dumps(blocks, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (run / "telemetry_alignment.json").write_text(json.dumps(telemetry, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (run / "synchronized_shadow_replay_evidence.json").write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"run_id": record["run_id"], "analysis_verdict": analysis["analysis_verdict"],
                      "valid_transactions": analysis["counts"]["valid_workload_ledger_transactions"],
                      "blocks": len(blocks["blocks"]), "forecast_origins": forecasts["overall"]["forecast_origins"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
