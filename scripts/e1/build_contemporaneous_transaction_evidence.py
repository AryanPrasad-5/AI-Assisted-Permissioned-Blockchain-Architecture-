"""Build a leakage-safe evidence join from one bounded Caliper/Fabric run.

Inputs are immutable recorder, submit-event, block-fetch observation and
verified-ledger files. The output is a new audit artifact plus eligible
telemetry frames; source files are never rewritten.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iso_ms(value: float) -> str:
    return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def nearest_rank(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def state_for(rec: dict[str, str] | None, ledger: dict[str, str] | None) -> str:
    success = bool(rec and rec.get("status") == "success")
    if ledger is None:
        return "PENDING_OR_UNRESOLVED" if rec and rec.get("final_ms") else "FAILED_OR_NOT_SUBMITTED"
    valid = int(ledger["validation_code"]) == 0
    if valid and success:
        return "COMMITTED_VALID"
    if not valid:
        return "COMMITTED_INVALID"
    return "CALIPER_LEDGER_STATUS_CONTRADICTION"


def make_frame(*, run_id: str, channel: str, observation: datetime, config: dict,
               submits: list[dict[str, str]], records: list[dict[str, str]], ledger: list[dict[str, str]],
               block_observations: dict[int, datetime], previous_observation: datetime | None,
               source_files: dict[str, str]) -> dict:
    visible_submits = [row for row in submits if datetime.fromtimestamp(float(row["t_submit_evt_ms"]) / 1000, timezone.utc) <= observation]
    visible_records = [row for row in records if row.get("final_ms") and datetime.fromtimestamp(float(row["final_ms"]) / 1000, timezone.utc) <= observation]
    visible_ledger = [row for row in ledger if block_observations.get(int(row["block"]), observation) <= observation]
    submit_events = [(datetime.fromtimestamp(float(row["t_submit_evt_ms"]) / 1000, timezone.utc), int(row["count"])) for row in visible_submits]
    recent = [(ts, count) for ts, count in submit_events if ts >= observation - timedelta(seconds=5)]
    submit_times = [ts for ts, _ in recent]
    submit_span = (max(submit_times) - min(submit_times)).total_seconds() if len(submit_times) > 1 else 0.0
    offered = sum(count for _, count in recent) / submit_span if submit_span > 0 else None
    history_bins: dict[datetime, int] = defaultdict(int)
    for ts, count in recent:
        bucket = ts.replace(microsecond=0)
        history_bins[bucket] += count
    history_timestamps = sorted(history_bins)
    history_values = [float(history_bins[ts]) for ts in history_timestamps]
    latencies = [float(row["final_ms"]) - float(row["create_ms"]) for row in visible_records
                 if row.get("status") == "success" and row.get("create_ms") and row.get("final_ms")]
    mean_latency = sum(latencies) / len(latencies) if latencies else None
    p95_latency = nearest_rank(latencies, 0.95)
    previous_ledger = [row for row in visible_ledger if previous_observation is None or block_observations.get(int(row["block"]), observation) > previous_observation]
    interval_s = None if previous_observation is None else (observation - previous_observation).total_seconds()
    valid_new = sum(int(row["validation_code"]) == 0 for row in previous_ledger)
    delivered = valid_new / interval_s if interval_s and interval_s > 0 else None
    failed = sum(row.get("status") != "success" for row in visible_records)
    submitted = sum(int(row["count"]) for row in visible_submits)
    pending = max(0, submitted - len(visible_records))
    source_times = {
        "observation": observation.isoformat(),
        "caliper_submission": max((iso_ms(row["t_submit_evt_ms"]) for row in visible_submits), default=observation.isoformat()),
        "caliper_completion": max((iso_ms(row["final_ms"]) for row in visible_records if row.get("final_ms")), default=observation.isoformat()),
        "ledger_observation": max((block_observations[int(row["block"])].isoformat() for row in visible_ledger), default=observation.isoformat()),
    }
    freshness = {name: max(0.0, (observation - parse_iso(value)).total_seconds()) for name, value in source_times.items()}
    critical_missing = [name for name, value in {
        "observed_arrival_rate_tps": offered,
        "observed_throughput_tps": delivered,
        "observed_latency_ms": mean_latency,
        "queue_depth": float(pending),
    }.items() if value is None]
    state = {
        "timestamp_utc": observation.isoformat(), "current_config": config,
        "observed_arrival_rate_tps": offered, "observed_throughput_tps": delivered,
        "observed_latency_ms": mean_latency, "observed_latency_p95_ms": p95_latency,
        "queue_depth": float(pending), "failed_transactions": failed,
        "arrival_history_tps": history_values, "arrival_history_timestamps_utc": [ts.isoformat() for ts in history_timestamps],
        "telemetry_age_s": 0.0,
    }
    return {
        "run_id": run_id, "decision_index": 0, "observation_timestamp_utc": observation.isoformat(),
        "observation_monotonic_ns": 0, "state": state, "source_timestamps_utc": source_times,
        "freshness_seconds": freshness, "critical_missing": critical_missing,
        "inconsistencies": [], "provenance": {"channel_id": channel, "source_files": source_files,
            "measurement_window_s": 5.0, "visible_submitted": submitted, "visible_completed": len(visible_records),
            "visible_ledger_rows": len(visible_ledger), "ledger_observation_is_not_commit_timestamp": True,
            "ledger_interval_s": interval_s},
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--channel", required=True)
    ap.add_argument("--recorder-csv", type=Path, required=True)
    ap.add_argument("--submits-csv", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--ledger-tx", type=Path, required=True)
    ap.add_argument("--ledger-observations", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args(argv)
    if args.out_dir.exists():
        raise SystemExit(f"refusing to overwrite {args.out_dir}")
    out = args.out_dir
    out.mkdir(parents=True)
    recorder = read_csv(args.recorder_csv)
    submits = read_csv(args.submits_csv)
    ledger = [row for row in read_csv(args.ledger_tx) if row.get("channel_id") == args.channel and row.get("header_type") == "3"]
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    observations = [json.loads(line) for line in args.ledger_observations.read_text(encoding="utf-8").splitlines() if line.strip()]
    block_observations = {int(row["block_number"]): parse_iso(row["observed_timestamp_utc"]) for row in observations if row.get("status") == "ok"}
    by_tx = {row["tx_id"]: row for row in ledger}
    recorder_ids = {row.get("tx_id") for row in recorder if row.get("tx_id")}
    joined = [row for row in recorder if row.get("tx_id") in by_tx]
    contradictions = [row["tx_id"] for row in joined if state_for(row, by_tx[row["tx_id"]]) == "CALIPER_LEDGER_STATUS_CONTRADICTION"]
    latencies = [float(row["final_ms"]) - float(row["create_ms"]) for row in recorder if row.get("status") == "success"]
    submit_times = [float(row["t_submit_evt_ms"]) for row in submits]
    submit_span = (max(submit_times) - min(submit_times)) / 1000 if len(submit_times) > 1 else None
    round_wall = float(manifest["clock"]["wall_elapsed_ms"]) / 1000
    metrics = {
        "submitted": sum(int(row["count"]) for row in submits), "valid": sum(int(row["validation_code"]) == 0 for row in ledger),
        "invalid": sum(int(row["validation_code"]) != 0 for row in ledger), "failed": sum(row.get("status") != "success" for row in recorder),
        "pending_unresolved": int(manifest["counts"].get("pending_at_deactivate", 0)),
        "offered_tps": sum(int(row["count"]) for row in submits) / submit_span if submit_span else None,
        "delivered_valid_tps": sum(int(row["validation_code"]) == 0 for row in ledger) / round_wall if round_wall else None,
        "mean_client_latency_ms": sum(latencies) / len(latencies) if latencies else None,
        "p95_client_latency_ms": nearest_rank(latencies, 0.95), "completion_ratio": len(joined) / len(recorder) if recorder else None,
        "ledger_verification_coverage": len(joined) / len(recorder) if recorder else None,
    }
    block_obs = {int(row["block_number"]): row["observed_timestamp_utc"] for row in observations if row.get("status") == "ok"}
    tx_evidence = []
    for row in recorder:
        tx_id = row.get("tx_id", "")
        led = by_tx.get(tx_id)
        block = int(led["block"]) if led else None
        tx_evidence.append({"run_id": args.run_id, "channel": args.channel, "tx_id": tx_id,
            "submission_event_timestamp_utc": iso_ms(row["create_ms"]) if row.get("create_ms") else None,
            "caliper_completion_timestamp_utc": iso_ms(row["final_ms"]) if row.get("final_ms") else None,
            "submission_ack_timestamp_utc": None, "caliper_status": row.get("status"),
            "fabric_transaction_id": tx_id or None, "state": state_for(row, led),
            "block_number": block, "validation_code": int(led["validation_code"]) if led else None,
            "channel_header_timestamp_utc": iso_ms(led["ch_ts_ms"]) if led and led.get("ch_ts_ms") else None,
            "ledger_observation_timestamp_utc": block_obs.get(block) if block is not None else None,
            "ledger_observation_semantics": "peer block fetch observation; not exact commit timestamp",
            "error": row.get("err") or None})
    config = {"max_message_count": 40, "batch_timeout_ms": 2000, "batch_timeout": "2s", "preferred_max_bytes": 10485760,
              "absolute_max_bytes": 103809024, "orderers": 3}
    source_files = {key: {"path": str(path), "sha256": sha256(path)} for key, path in {
        "recorder_csv": args.recorder_csv, "submits_csv": args.submits_csv, "manifest": args.manifest, "ledger_tx": args.ledger_tx,
        "ledger_observations": args.ledger_observations}.items()}
    frame_rows = []
    ordered_obs = sorted(block_observations.items(), key=lambda row: row[1])
    previous = None
    for index, (_, observation) in enumerate(ordered_obs):
        frame = make_frame(run_id=args.run_id, channel=args.channel, observation=observation, config=config,
                           submits=submits, records=recorder, ledger=ledger, block_observations=block_observations,
                           previous_observation=previous, source_files={k: v["path"] for k, v in source_files.items()})
        frame["decision_index"] = index
        frame_rows.append(frame)
        previous = observation
    (out / "controller_frames_all.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in frame_rows), encoding="utf-8")
    eligible = [row for row in frame_rows if not row["critical_missing"] and not row["inconsistencies"]]
    for index, row in enumerate(eligible):
        row["decision_index"] = index
    (out / "controller_frames.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in eligible), encoding="utf-8")
    evidence = {"tool": "build_contemporaneous_transaction_evidence 1.0", "run_id": args.run_id, "channel": args.channel,
        "configuration": config, "source_files": source_files, "counts": {"recorder_rows": len(recorder), "submit_events": len(submits),
            "ledger_rows": len(ledger), "joined_rows": len(joined), "contradictions": len(contradictions), "eligible_shadow_frames": len(eligible),
            "total_shadow_frames": len(frame_rows)}, "metrics": metrics,
        "independent_ledger_verification": {"block_observation_rows": len(observations), "blocks_ok": sum(row.get("status") == "ok" for row in observations),
            "verification_artifact": str(args.ledger_tx), "ledger_validation_codes": sorted({int(row["validation_code"]) for row in ledger}),
            "observation_timestamps_are_not_commit_timestamps": True},
        "discrepancies": {"status_validation_contradictions": contradictions},
        "timestamp_contract": {"caliper_wall_clock": "Node Date.now epoch milliseconds UTC", "caliper_monotonic": "process.hrtime relative nanoseconds",
            "ledger_observation": "host UTC wall clock with nanosecond text precision", "join_rule": "transaction ID plus block observation timestamp",
            "future_data_rule": "frames only expose rows whose source timestamp or ledger observation is <= frame observation"},
        "artifacts": {"transaction_evidence": str(out / "transaction_evidence.jsonl"), "controller_frames": str(out / "controller_frames.jsonl"),
                       "all_frames": str(out / "controller_frames_all.jsonl")}}
    (out / "transaction_evidence.jsonl").write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in tx_evidence), encoding="utf-8")
    (out / "evidence_summary.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"run_id": args.run_id, "recorder_rows": len(recorder), "ledger_rows": len(ledger),
                      "ledger_coverage": metrics["ledger_verification_coverage"], "eligible_shadow_frames": len(eligible), "out_dir": str(out)}, indent=2))
    return 0 if not contradictions and len(joined) == len(recorder) else 2


if __name__ == "__main__":
    raise SystemExit(main())
