"""Analyze the immutable output of e1_final_comparison.py.

The analyzer writes only to a new results directory and computes per-run
metrics, direct log-reason/block joins, paired effects and deterministic
bootstrap intervals using the Python standard library.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import statistics
from datetime import datetime
from pathlib import Path

COUNT_RE = re.compile(r"Batch size met, cutting batch")
TIMER_RE = re.compile(r"Batch timer expired, creating block")
CREATED_RE = re.compile(r"Created block \[(\d+)\].*channel=([^ ]+) node=(\d+)")
EVENT_TS_RE = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+Z)")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def quantile(values: list[float], q: float) -> float:
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(sorted(values), n=100, method="inclusive")[max(0, min(98, int(q * 100) - 1))]


def bootstrap_mean(values: list[float], seed: int = 20261110, n: int = 20000) -> dict:
    rng = random.Random(seed)
    means = []
    for _ in range(n):
        sample = [values[rng.randrange(len(values))] for _ in values]
        means.append(statistics.mean(sample))
    means.sort()
    return {"mean": statistics.mean(values), "ci95_low": quantile(means, 0.025),
            "ci95_high": quantile(means, 0.975), "resamples": n, "seed": seed}


def event_ms(line: str) -> float | None:
    match = EVENT_TS_RE.search(line)
    if not match:
        return None
    return datetime.fromisoformat(match.group(1).replace("Z", "+00:00")).timestamp() * 1000


def direct_log_evidence(log_dir: Path, channel: str, blocks: list[dict],
                        measured_window: tuple[float, float] | None) -> dict:
    events = []
    for path in sorted(log_dir.glob("*.stderr.log")):
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for idx, line in enumerate(lines, start=1):
            reason = "MESSAGE_COUNT" if COUNT_RE.search(line) else "TIMEOUT" if TIMER_RE.search(line) else None
            if not reason:
                continue
            block = None
            for later in lines[idx:idx + 40]:
                if COUNT_RE.search(later) or TIMER_RE.search(later):
                    break
                match = CREATED_RE.search(later)
                if match and match.group(2) == channel:
                    block = int(match.group(1))
                    break
            ts_ms = event_ms(line)
            in_measured = measured_window is None or (ts_ms is not None and measured_window[0] <= ts_ms <= measured_window[1])
            events.append({"source": str(path), "line": idx, "reason": reason,
                           "block": block, "event_ms": ts_ms, "in_measured_window": in_measured,
                           "raw": line.replace("\x1b", "")})
    block_by_no = {int(row["number"]): row for row in blocks}
    joins = []
    for event in events:
        row = block_by_no.get(event["block"])
        joins.append({**event, "tx_count": int(row["tx_count"]) if row else None,
                      "inferred_cut_cause": row.get("inferred_cut_cause") if row else None,
                      "join_confirmed": bool(row and event["block"] is not None)})
    measured = [e for e in events if e["in_measured_window"]]
    return {"message_count_events": sum(e["reason"] == "MESSAGE_COUNT" for e in measured),
            "timeout_events": sum(e["reason"] == "TIMEOUT" for e in measured),
            "all_message_count_events": sum(e["reason"] == "MESSAGE_COUNT" for e in events),
            "all_timeout_events": sum(e["reason"] == "TIMEOUT" for e in events),
            "events": joins, "all_joins_confirmed": all(e["join_confirmed"] for e in joins)}


def resource_peaks(path: Path) -> dict:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {c: {"cpu_percent_max": v.get("cpu_percent_max"), "mem_used_bytes_max": v.get("mem_used_bytes_max"),
                "coverage": v.get("coverage")} for c, v in doc.get("per_container", {}).items()}


def run_row(item: dict, run_root: Path, obs_root: Path, caliper_workspace: Path, ordinal: int) -> tuple[dict, list[dict]]:
    run_id, channel, B = item["run_id"], item["channel"], int(item["B"])
    regime = "timeout" if float(item["rate_tps"]) == 10 else "message_count"
    run = run_root / run_id
    obs = obs_root / run_id
    analysis = item["analysis"]
    metrics = analysis.get("metrics", {})
    telemetry = item.get("metrics", {})
    derived = [v for v in telemetry.get("derived", {}).values() if isinstance(v, dict)]
    fill_count = sum(float(v.get("fill_duration_count_delta", 0) or 0) for v in derived)
    fill_sum = sum(float(v.get("fill_duration_sum_delta_s", 0) or 0) for v in derived)
    blocks_path = run / "ledger" / "ledger" / "blocks.csv"
    blocks = read_csv(blocks_path) if blocks_path.exists() else []
    measured_manifest = caliper_workspace / "runs" / run_id / "recorder" / f"{run_id}_w0_r1_m-{run_id}.manifest.json"
    measured_window = None
    try:
        clock = json.loads(measured_manifest.read_text(encoding="utf-8")).get("clock", {})
        measured_window = (float(clock["wall0_ms"]), float(clock["wall1_ms"]))
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        pass
    evidence = direct_log_evidence(obs / "logs", channel, blocks, measured_window)
    before = item.get("record", {}).get("before", {}).get("config", {}).get("active_config")
    after = item.get("record", {}).get("after", {}).get("config", {}).get("active_config")
    row = {"ordinal": ordinal, "run_id": run_id, "channel": channel, "B": B, "regime": regime,
           "rate_tps": float(item["rate_tps"]), "duration_s": item.get("duration_s"), "warmup_s": item.get("warmup_s"),
           "offered_tps": metrics.get("offered_tps_from_submit_events"),
           "delivered_tps": metrics.get("delivered_tps_from_round_wall_time"),
           "latency_mean_ms": metrics.get("latency_ms_mean"),
           "latency_p95_ms": metrics.get("latency_ms_p95_nearest_rank"),
           "max_backlog": metrics.get("max_inflight_backlog"),
           "submitted": analysis.get("counts", {}).get("submitted"),
           "valid": analysis.get("counts", {}).get("valid_workload_ledger_transactions"),
           "failed": analysis.get("counts", {}).get("failed"),
           "success_rate": metrics.get("success_rate"),
           "analysis_verdict": analysis.get("analysis_verdict"),
           "config_unchanged": before == after,
           "direct_message_count_events": evidence["message_count_events"],
           "direct_timeout_events": evidence["timeout_events"],
           "all_direct_message_count_events": evidence["all_message_count_events"],
           "all_direct_timeout_events": evidence["all_timeout_events"],
           "direct_events_joined": evidence["all_joins_confirmed"],
           "block_count": len(blocks),
           "block_tx_counts": ";".join(str(int(b["tx_count"])) for b in blocks),
           "block_inferred_causes": ";".join(b["inferred_cut_cause"] for b in blocks),
           "metrics_fill_duration_mean_s": fill_sum / fill_count if fill_count else None,
           "leader_changes": sum(float(v.get("leader_changes_delta", 0)) for v in derived),
           "resource_manifest": str(obs / "resources" / "resource_manifest.json"),
           "raw_orderer_log_hashes": ";".join(f"{p.name}:{sha256(p)}" for p in sorted((obs / "logs").glob("*.stderr.log")))}
    return row, [{"run_id": run_id, "channel": channel, "B": B, "regime": regime,
                  "block_number": e["block"], "reason": e["reason"], "tx_count": e["tx_count"],
                  "event_ms": e["event_ms"], "in_measured_window": e["in_measured_window"],
                  "inferred_cut_cause": e["inferred_cut_cause"], "join_confirmed": e["join_confirmed"],
                  "source": e["source"], "line": e["line"]} for e in evidence["events"]]


def summary(rows: list[dict], regime: str, B: int) -> dict:
    data = [r for r in rows if r["regime"] == regime and r["B"] == B]
    out = {"regime": regime, "B": B, "runs": len(data)}
    for field in ("offered_tps", "delivered_tps", "latency_mean_ms", "latency_p95_ms", "max_backlog",
                  "success_rate", "metrics_fill_duration_mean_s"):
        vals = [float(r[field]) for r in data if r[field] is not None]
        out[field] = {"median": statistics.median(vals) if vals else None,
                      "q1": quantile(vals, 0.25) if vals else None,
                      "q3": quantile(vals, 0.75) if vals else None,
                      "iqr": quantile(vals, 0.75) - quantile(vals, 0.25) if vals else None}
    out["total_direct_message_count_events"] = sum(r["direct_message_count_events"] for r in data)
    out["total_direct_timeout_events"] = sum(r["direct_timeout_events"] for r in data)
    out["all_valid"] = all(r["analysis_verdict"] == "VALID" and r["config_unchanged"] for r in data)
    return out


def paired(rows: list[dict], regime: str, field: str) -> dict:
    a = sorted((r for r in rows if r["regime"] == regime and r["B"] == 45), key=lambda r: r["run_id"])
    b = sorted((r for r in rows if r["regime"] == regime and r["B"] == 40), key=lambda r: r["run_id"])
    diffs = [float(y[field]) - float(x[field]) for x, y in zip(a, b)]
    ci = bootstrap_mean(diffs)
    sd = statistics.stdev(diffs) if len(diffs) >= 2 else None
    ci.update({"regime": regime, "metric": field, "pairs": len(diffs),
               "B45_values": [float(x[field]) for x in a], "B40_values": [float(y[field]) for y in b],
               "differences_B40_minus_B45": diffs,
               "percent_difference_vs_B45_mean": 100 * ci["mean"] / statistics.mean(float(x[field]) for x in a),
               "paired_standardized_effect": ci["mean"] / sd if sd else None})
    return ci


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", required=True, type=Path)
    ap.add_argument("--run-root", required=True, type=Path)
    ap.add_argument("--observation-root", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--caliper-workspace", type=Path, default=Path("/home/mailf18/e1/caliper-ws-benchmark"))
    args = ap.parse_args()
    if args.out_dir.exists():
        raise SystemExit(f"refusing to overwrite {args.out_dir}")
    doc = json.loads(args.summary.read_text(encoding="utf-8"))
    rows, events = [], []
    for i, item in enumerate(doc["runs"], start=1):
        row, ev = run_row(item, args.run_root, args.observation_root, args.caliper_workspace, i)
        rows.append(row); events.extend(ev)
    args.out_dir.mkdir(parents=True)
    fields = list(rows[0]) if rows else []
    with (args.out_dir / "per_run_metrics.csv").open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields); w.writeheader(); w.writerows(rows)
    efields = list(events[0]) if events else ["run_id", "channel", "B", "regime", "block_number", "reason", "tx_count",
                                               "inferred_cut_cause", "join_confirmed", "source", "line"]
    with (args.out_dir / "block_cut_evidence.csv").open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=efields); w.writeheader(); w.writerows(events)
    groups = [summary(rows, regime, B) for regime in ("timeout", "message_count") for B in (45, 40)]
    effects = [paired(rows, regime, field) for regime in ("timeout", "message_count")
               for field in ("delivered_tps", "latency_mean_ms", "latency_p95_ms", "max_backlog")]
    resource = {r["run_id"]: resource_peaks(Path(r["resource_manifest"])) for r in rows}
    result = {"tool": "e1_final_comparison_analyze 0.1", "source_summary": str(args.summary),
              "groups": groups, "paired_effects": effects, "direct_block_cut_evidence": events,
              "resource_peaks": resource,
              "integrity": {"all_runs_valid": all(r["analysis_verdict"] == "VALID" for r in rows),
                            "all_configs_unchanged": all(r["config_unchanged"] for r in rows),
                            "all_direct_joins_confirmed": all(r["direct_events_joined"] for r in rows),
                            "run_count": len(rows)}}
    (args.out_dir / "comparison_summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"out_dir": str(args.out_dir), "runs": len(rows), "events": len(events),
                      "integrity": result["integrity"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
