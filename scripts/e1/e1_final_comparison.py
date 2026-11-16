"""Execute the pre-rendered, paired E1 MaxMessageCount comparison.

This wrapper is deliberately narrow: it runs only against the two existing
verified channels, records metrics/resources/logs around each Caliper run,
refuses existing output directories, enforces the registered safety limits,
and restores the original orderer logging specification in a finally block.
It never creates channels or submits configuration updates.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ORDERERS = ("orderer.example.com", "orderer2.example.com", "orderer3.example.com")
OPS = (9443, 9446, 9447)
PEERS = ("peer0.org1.example.com", "peer0.org2.example.com")
CALIPER = "caliper-benchmark-001"
TARGETS = (
    "orderer1:orderer=http://localhost:9443/metrics",
    "orderer2:orderer=http://localhost:9446/metrics",
    "orderer3:orderer=http://localhost:9447/metrics",
    "peer0org1:peer=http://localhost:9444/metrics",
    "peer0org2:peer=http://localhost:9445/metrics",
)
DEBUG_SPEC = "info:orderer.common.blockcutter,orderer.consensus.etcdraft=debug"
INFO_SPEC = "info"


def utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def run(argv: list[str], *, timeout: float = 120.0, stdout=None) -> subprocess.CompletedProcess:
    return subprocess.run(argv, stdout=stdout, stderr=subprocess.STDOUT, timeout=timeout, check=False)


def logspec(port: int, spec: str | None = None) -> tuple[int, str]:
    url = f"http://127.0.0.1:{port}/logspec"
    if spec is None:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status, response.read().decode("utf-8", "replace")
    req = urllib.request.Request(url, data=json.dumps({"spec": spec}).encode(),
                                 headers={"Content-Type": "application/json"}, method="PUT")
    with urllib.request.urlopen(req, timeout=5) as response:
        response.read()
        return response.status, ""


def health(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def set_all(spec: str) -> dict:
    out = {"spec": spec, "ports": {}}
    for port in OPS:
        status, body = logspec(port, spec)
        if status != 204:
            raise RuntimeError(f"PUT /logspec on {port} returned HTTP {status}")
        got_status, got = logspec(port)
        out["ports"][str(port)] = {"put_http": status, "get_http": got_status, "get_body": got,
                                    "health": health(port)}
        if got_status != 200 or not out["ports"][str(port)]["health"]:
            raise RuntimeError(f"orderer {port} failed post-logspec health/spec check")
    return out


def metric_args(phase: str, out_dir: Path, *, duration: float | None = None,
                stop_file: Path | None = None) -> list[str]:
    argv = [sys.executable, "-B", str(HERE / "e1_metrics_scraper.py"), "scrape", "--phase", phase,
            "--out-dir", str(out_dir), "--interval", "1", "--execute"]
    if duration is not None:
        argv += ["--duration", str(duration)]
    if stop_file is not None:
        argv += ["--stop-file", str(stop_file)]
    for target in TARGETS:
        argv += ["--target", target]
    return argv


def resource_args(out_dir: Path, *, duration: float | None = None, stop_file: Path | None = None) -> list[str]:
    argv = [sys.executable, "-B", str(HERE / "e1_resource_sampler.py"), "sample", "--source", "api",
            "--out-dir", str(out_dir), "--interval", "1", "--execute"]
    if duration is not None:
        argv += ["--duration", str(duration)]
    if stop_file is not None:
        argv += ["--stop-file", str(stop_file)]
    for container in (*ORDERERS, *PEERS, CALIPER):
        argv += ["--container", container]
    return argv


def stop_process(proc: subprocess.Popen, stop_file: Path, timeout: float = 20) -> dict:
    stop_file.write_text("stop\n", encoding="utf-8")
    try:
        code = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        code = proc.wait(timeout=10)
    return {"exit_code": code}


def parse_health_metrics(port: int, channels: set[str]) -> dict:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
            text = response.read().decode("utf-8", "replace")
    except (OSError, urllib.error.URLError) as exc:
        return {"error": str(exc)}
    result = {}
    for channel in channels:
        q = re.escape(channel)
        active = re.search(rf'^consensus_etcdraft_active_nodes\{{channel="{q}"\}}\s+(\d+)', text, re.M)
        cluster = re.search(rf'^consensus_etcdraft_cluster_size\{{channel="{q}"\}}\s+(\d+)', text, re.M)
        result[channel] = {"active_nodes": int(active.group(1)) if active else None,
                           "cluster_size": int(cluster.group(1)) if cluster else None}
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="append", required=True, type=Path,
                    help="plans in the intended balanced execution order")
    ap.add_argument("--run-root", required=True, type=Path)
    ap.add_argument("--observation-root", required=True, type=Path)
    ap.add_argument("--caliper-container", default=CALIPER)
    ap.add_argument("--caliper-timeout-s", type=float, default=180.0)
    ap.add_argument("--sampler-timeout-s", type=float, default=90.0)
    ap.add_argument("--summary", required=True, type=Path)
    args = ap.parse_args(argv)
    if args.summary.exists():
        raise SystemExit(f"refusing to overwrite {args.summary}")
    args.run_root.mkdir(parents=True, exist_ok=True)
    args.observation_root.mkdir(parents=True, exist_ok=True)
    results = {"tool": "e1_final_comparison 0.1", "started_utc": utc(), "runs": [], "halted": None}
    try:
        results["logging_enable"] = set_all(DEBUG_SPEC)
        for plan in args.plan:
            raw = json.loads(plan.read_text(encoding="utf-8"))
            channel, run_id = raw["channel"], raw["run_id"]
            B = int(raw["cell"]["B"])
            run_dir = args.run_root / run_id
            obs = args.observation_root / run_id
            if run_dir.exists() or obs.exists():
                raise RuntimeError(f"refusing existing output for {run_id}")
            run_dir.parent.mkdir(parents=True, exist_ok=True)
            obs.mkdir(parents=True)
            (obs / "plan.json").write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            before = obs / "metrics" / "before"
            before_rec = run(metric_args("before", before), timeout=30)
            if before_rec.returncode != 0:
                raise RuntimeError(f"metrics before failed for {run_id}")

            start_epoch = time.time()
            start_utc = utc()
            stop_m = obs / "stop_metrics"
            stop_r = obs / "stop_resources"
            metrics_dir = obs / "metrics" / "during"
            resources_dir = obs / "resources"
            metrics_p = subprocess.Popen(metric_args("during", metrics_dir, stop_file=stop_m),
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            resource_p = subprocess.Popen(resource_args(resources_dir, stop_file=stop_r),
                                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            runner_log = obs / "runner.log"
            runner_argv = [sys.executable, "-B", str(HERE / "e1_existing_channel_pilot.py"),
                           "--plan", str(plan), "--run-dir", str(run_dir), "--execute", "--allow-duration",
                           "--caliper-container", args.caliper_container,
                           "--caliper-timeout-s", str(args.caliper_timeout_s)]
            with runner_log.open("x", encoding="utf-8") as fh:
                runner = run(runner_argv, timeout=args.caliper_timeout_s + 60, stdout=fh)
            end_epoch = time.time()
            end_utc = utc()
            metrics_stop = stop_process(metrics_p, stop_m, args.sampler_timeout_s)
            resources_stop = stop_process(resource_p, stop_r, args.sampler_timeout_s)

            after = obs / "metrics" / "after"
            after_rec = run(metric_args("after", after), timeout=30)
            summary_dir = obs / "metrics" / "summary"
            summary_rec = run([sys.executable, "-B", str(HERE / "e1_metrics_scraper.py"), "summarize",
                               "--before", str(before), "--during", str(metrics_dir), "--after", str(after),
                               "--channel", channel, "--out-dir", str(summary_dir)], timeout=30)

            log_dir = obs / "logs"
            since = datetime.fromtimestamp(max(0, start_epoch - 5), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            until = datetime.fromtimestamp(end_epoch + 5, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            log_rec = run([sys.executable, "-B", str(HERE / "e1_log_collector.py"), "capture",
                           *sum((["--container", c] for c in ORDERERS), []), "--since", since,
                           "--until", until, "--out-dir", str(log_dir), "--execute"], timeout=60)

            record_path = run_dir / "pilot_run_record.json"
            record = json.loads(record_path.read_text(encoding="utf-8")) if record_path.exists() else {}
            analysis_path = run_dir / "analysis" / "summary.json"
            analysis = json.loads(analysis_path.read_text(encoding="utf-8")) if analysis_path.exists() else {}
            metric_path = summary_dir / "metrics_summary.json"
            metrics = json.loads(metric_path.read_text(encoding="utf-8")) if metric_path.exists() else {}
            backlog = analysis.get("metrics", {}).get("max_inflight_backlog")
            failures = analysis.get("counts", {}).get("failed", 0)
            submitted = analysis.get("counts", {}).get("submitted", 0)
            safety = {"backlog_limit": 2 * B, "backlog_ok": backlog is not None and backlog <= 2 * B,
                      "failure_rate": failures / submitted if submitted else 1.0,
                      "failure_rate_ok": bool(submitted) and failures / submitted <= 0.01,
                      "runner_complete": record.get("status") == "COMPLETE",
                      "analysis_valid": analysis.get("analysis_verdict") == "VALID",
                      "metrics_complete": metrics.get("status") in ("COMPLETE", "COMPLETE_WITH_WARNINGS"),
                      "health_ok": all(health(p) for p in OPS),
                      "quorum": parse_health_metrics(OPS[0], {channel}).get(channel, {})}
            safety["quorum_ok"] = safety["quorum"].get("active_nodes") == 3 and safety["quorum"].get("cluster_size") == 3
            item = {"run_id": run_id, "channel": channel, "B": B, "rate_tps": raw["cell"]["rate_tps"],
                    "duration_s": raw["cell"].get("duration_s"), "warmup_s": 5,
                    "start_utc": start_utc, "end_utc": end_utc, "runner_exit_code": runner.returncode,
                    "record": record, "analysis": analysis, "metrics": metrics,
                    "observation": {"path": str(obs), "log_capture": {"since": since, "until": until,
                                                                            "exit_code": log_rec.returncode},
                                    "metrics_before_exit": before_rec.returncode,
                                    "metrics_during": metrics_stop, "resources": resources_stop,
                                    "metrics_after_exit": after_rec.returncode,
                                    "metrics_summary_exit": summary_rec.returncode},
                    "safety": safety}
            results["runs"].append(item)
            if not all(safety[k] for k in ("backlog_ok", "failure_rate_ok", "runner_complete", "analysis_valid",
                                           "metrics_complete", "health_ok", "quorum_ok")):
                results["halted"] = {"run_id": run_id, "reason": "safety stop", "safety": safety}
                break
        results["completed_utc"] = utc()
    finally:
        results["logging_restore"] = set_all(INFO_SPEC)
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"summary": str(args.summary), "runs": len(results["runs"]), "halted": results["halted"]}, indent=2))
    return 0 if not results["halted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
