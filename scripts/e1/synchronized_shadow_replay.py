"""Run one bounded Flashbots-bin replay on an existing E1 channel.

This script is deliberately an existing-channel runner. It never creates a
channel, changes channel configuration, invokes the configuration actuator, or
touches chaincode definitions. The replay payload is synthetic ``CreateAsset``
traffic; only the audited Flashbots arrival counts and UTC bin order are used.

Task A0b (2026-11-20): the channel is named explicitly (``--channel``) and must be a dedicated validation channel
(``fabric_perf.controllers.channel_selection``): the protected reference channels ``e1-r002`` and ``e1-cfg001`` are
refused and there is no default. The expected ``MaxMessageCount`` is a separate explicit input
(``--expected-max-message-count``), checked against the configuration fetched from the channel before the run; it
used to come from a table keyed by channel name, which allowed only the two protected channels.

The execution path is Linux/WSL because the verified Caliper/Fabric tooling is
inside the Ubuntu environment. ``--prepare`` is read-only with respect to
Fabric. ``--execute`` performs one bounded transaction replay and the read-only
collection around it.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "src"))

import e1_channel_run as cr  # noqa: E402
import e1_config_actuator as ca  # noqa: E402
import e1_existing_channel_pilot as pilot  # noqa: E402
from fabric_perf.controllers import channel_selection as cs  # noqa: E402  (standard library only)


DEFAULT_SERIES = ROOT / "data/processed/flashbots/series_2026-09-21_1s.csv.gz"
DEFAULT_OUT = ROOT / "experiments/results/e1/synchronized_shadow_replay"
DEFAULT_WORKSPACE = "/home/mailf18/e1/caliper-ws-benchmark"
CALIPER_CONTAINER = "caliper-benchmark-001"
CALIPER_WS = "/hyperledger/caliper/workspace"
CAL_ORGS = "/fabric/organizations"
PREFERRED_MAX_BYTES = 10_485_760
ORDERER_ENDPOINTS = (9443, 9446, 9447)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as fh:
        json.dump(value, fh, indent=2, sort_keys=True)
        fh.write("\n")


def run_cmd(label: str, argv: list[str], env: dict[str, str], out_dir: Path,
            timeout_s: float = 120.0) -> dict:
    started = time.time()
    try:
        proc = subprocess.run(argv, env=env, capture_output=True, timeout=timeout_s, check=False)
        out, err, code = proc.stdout, proc.stderr, proc.returncode
    except subprocess.TimeoutExpired as exc:
        out, err, code = exc.stdout or b"", (exc.stderr or b"") + \
            f"\n{label} timed out after {timeout_s}s\n".encode(), None
    out_path, err_path = out_dir / f"{label}.stdout", out_dir / f"{label}.stderr"
    out_path.write_bytes(out)
    err_path.write_bytes(err)
    return {"label": label, "argv": argv, "start_epoch": started, "end_epoch": time.time(),
            "exit_code": code, "stdout": str(out_path), "stderr": str(err_path)}


def parse_series(path: Path, start_bin: int, duration_bins: int) -> tuple[list[dict], list[float]]:
    if duration_bins < 1 or start_bin < 0:
        raise ValueError("start-bin must be non-negative and duration-bins must be positive")
    rows: list[dict] = []
    with gzip.open(path, "rt", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        required = {"timestamp_utc", "arrival_count", "frequency_seconds", "date_utc"}
        if not required <= set(reader.fieldnames or ()):
            raise ValueError(f"series missing columns {sorted(required - set(reader.fieldnames or ())) }")
        for index, row in enumerate(reader):
            if index < start_bin:
                continue
            if index >= start_bin + duration_bins:
                break
            count = int(row["arrival_count"])
            if count < 0:
                raise ValueError("arrival_count cannot be negative")
            timestamp = dt.datetime.fromisoformat(row["timestamp_utc"].replace("Z", "+00:00"))
            rows.append({"source_bin": index, "timestamp_utc": timestamp.isoformat(),
                         "arrival_count": count, "frequency_seconds": int(row["frequency_seconds"]),
                         "date_utc": row["date_utc"]})
    if len(rows) != duration_bins:
        raise ValueError(f"requested {duration_bins} bins, found {len(rows)}")
    if any(row["frequency_seconds"] != 1 for row in rows):
        raise ValueError("Phase 1C runner requires the audited 1-second series")
    schedule: list[float] = []
    for offset, row in enumerate(rows):
        count = row["arrival_count"]
        # The processed trace has bin counts, not per-transaction Ethereum
        # timestamps. Spread each bin deterministically inside its one-second
        # interval; the bin timestamp and count remain unchanged in the manifest.
        schedule.extend(offset * 1000.0 + (j * 1000.0 / count) for j in range(count))
    if not schedule:
        raise ValueError("selected trace segment contains no transactions")
    return rows, schedule


def network_from_file(path: Path) -> cr.Network:
    return cr.Network(json.loads(path.read_text(encoding="utf-8")))


def metrics_targets() -> list[str]:
    return [f"orderer{i}:orderer=http://localhost:{port}/metrics" for i, port in enumerate(ORDERER_ENDPOINTS, 1)]


def render_files(run_id: str, channel: str, schedule: list[float], dispatch_log: str,
                 workspace: Path, repo: Path) -> dict:
    run_dir = workspace / "runs" / run_id
    if run_dir.exists():
        raise ValueError(f"refusing to overwrite existing Caliper run directory {run_dir}")
    (run_dir / "modules").mkdir(parents=True)
    module_sources = {
        "e1_trace_replay.js": repo / "fabric-testbed/caliper/workloads/e1_trace_replay.js",
        "e1_workload_core.js": repo / "fabric-testbed/caliper/workloads/e1_workload_core.js",
        "e1_per_tx_recorder.js": repo / "scripts/e1/caliper/e1_per_tx_recorder.js",
    }
    for name, source in module_sources.items():
        shutil.copyfile(source, run_dir / "modules" / name)
    module = f"{CALIPER_WS}/runs/{run_id}/modules/e1_trace_replay.js"
    recorder = f"{CALIPER_WS}/runs/{run_id}/recorder"
    dispatch_in_container = f"{CALIPER_WS}/runs/{run_id}/replay_dispatch.csv"
    replay_rate_in_container = f"{CALIPER_WS}/runs/{run_id}/replay_rate.txt"
    replay_rate_path = run_dir / "replay_rate.txt"
    # Caliper 0.6 replay-rate consumes one relative millisecond offset per
    # line.  The source schedule is deterministic and may contain fractions;
    # rounding here is explicit and bounded to the controller's millisecond
    # clock, while the original floating-point schedule remains in the
    # manifest and dispatch log.
    replay_rate_path.write_text("\n".join(str(max(0, round(value))) for value in schedule), encoding="utf-8")
    args = {"runId": run_id, "contractId": "e1asset", "invokerIdentity": "User1", "valueBytes": 128,
            "mode": "create", "failEvery": 25, "rmwKey": "unused", "scheduleMs": schedule,
            "dispatchLog": dispatch_in_container, "rateControlled": True}
    benchmark = {"test": {"name": f"e1-{run_id}", "workers": {"number": 1}, "rounds": [{
        "label": f"m-{run_id}", "txNumber": len(schedule),
        # Caliper's replay-rate controller is the sole inter-arrival scheduler.
        # This prevents fixed-rate callbacks from accumulating while the
        # workload module waits for a trace-bin offset.
        "rateControl": {"type": "replay-rate", "opts": {"pathTemplate": replay_rate_in_container,
                                                           "inputFormat": "TEXT", "defaultSleepTime": 100}},
        "workload": {"module": module, "arguments": args}}]},
        "monitors": {"transaction": [{"module": f"{CALIPER_WS}/runs/{run_id}/modules/e1_per_tx_recorder.js",
                                          "options": {"runId": run_id, "outputDir": recorder,
                                                      "binding": "fabric:2.5", "eventSemantics": "gateway_peer_commit_status",
                                                      "caliperVersion": "0.6.0"}}]}}
    org1 = ("peerOrganizations", "org1.example.com")
    ccp = {"name": "e1-org1", "version": "1.0.0", "client": {"organization": "Org1"},
           "organizations": {"Org1": {"mspid": "Org1MSP", "peers": ["peer0.org1.example.com"]}},
           "peers": {"peer0.org1.example.com": {"url": "grpcs://peer0.org1.example.com:7051",
               "tlsCACerts": {"path": f"{CAL_ORGS}/peerOrganizations/org1.example.com/tlsca/tlsca.org1.example.com-cert.pem"},
               "grpcOptions": {"ssl-target-name-override": "peer0.org1.example.com",
                                "hostnameOverride": "peer0.org1.example.com"}}}}
    user = f"{CAL_ORGS}/peerOrganizations/org1.example.com/users/User1@org1.example.com/msp"
    network = {"name": "e1-network", "version": "2.0.0", "caliper": {"blockchain": "fabric"},
               "channels": [{"channelName": channel, "contracts": [{"id": "e1asset"}]}],
               "organizations": [{"mspid": "Org1MSP", "identities": {"certificates": [{"name": "User1",
                   "clientPrivateKey": {"path": f"{user}/keystore/priv_sk"},
                   "clientSignedCert": {"path": f"{user}/signcerts/User1@org1.example.com-cert.pem"}}]},
                   "connectionProfile": {"path": f"{CALIPER_WS}/runs/{run_id}/ccp-org1.json", "discover": True}}]}
    ccp["_source"] = str(repo / "fabric-testbed/network/env-e1-caliper-benchmark.json")
    for name, value in (("benchmark.yaml", benchmark), ("networkconfig.yaml", network), ("ccp-org1.json", ccp)):
        write_json(run_dir / name, value)
    return {"run_dir": str(run_dir), "benchmark": str(run_dir / "benchmark.yaml"),
            "networkconfig": str(run_dir / "networkconfig.yaml"), "recorder_dir": str(run_dir / "recorder"),
            "dispatch_log": str(run_dir / "replay_dispatch.csv"),
            "replay_rate": str(replay_rate_path),
            "modules": {name: sha256(source) for name, source in module_sources.items()}}


def scrape_once(script: Path, phase: str, out_dir: Path, env: dict[str, str]) -> dict:
    argv = [sys.executable, "-B", str(script), "scrape", "--phase", phase, "--out-dir", str(out_dir),
            "--interval", "0.5", "--max-rounds", "1", "--execute"]
    for target in metrics_targets():
        argv += ["--target", target]
    return run_cmd(f"metrics_{phase}", argv, env, out_dir.parent, 30)


def start_metrics(script: Path, run_dir: Path, env: dict[str, str]) -> dict:
    out = run_dir / "metrics_during"
    stop = run_dir / "stop_metrics"
    argv = [sys.executable, "-B", str(script), "scrape", "--phase", "during", "--out-dir", str(out),
            "--interval", "0.5", "--stop-file", str(stop), "--execute"]
    for target in metrics_targets():
        argv += ["--target", target]
    stdout = (run_dir / "metrics_during.stdout").open("xb")
    stderr = (run_dir / "metrics_during.stderr").open("xb")
    proc = subprocess.Popen(argv, env=env, stdout=stdout, stderr=stderr)
    return {"proc": proc, "stop": stop, "argv": argv}


def stop_metrics(handle: dict) -> dict:
    handle["stop"].write_text("stop\n", encoding="utf-8")
    try:
        code = handle["proc"].wait(timeout=30)
    except subprocess.TimeoutExpired:
        handle["proc"].terminate()
        code = handle["proc"].wait(timeout=10)
    return {"exit_code": code, "stop_file": str(handle["stop"])}


def copy_tree(source: Path, target: Path) -> None:
    if not source.exists():
        return
    target.mkdir(parents=True, exist_ok=True)
    for item in source.rglob("*"):
        dest = target / item.relative_to(source)
        if item.is_dir():
            dest.mkdir(exist_ok=True)
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists():
                shutil.copy2(item, dest)


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def epoch_to_utc_ms(value: float) -> str:
    return dt.datetime.fromtimestamp(value / 1000.0, tz=dt.timezone.utc).isoformat(timespec="milliseconds")


def build_timeline(run_dir: Path, trace_rows: list[dict], summary: dict) -> tuple[Path, Path]:
    analysis = run_dir / "analysis"
    tx = read_csv(analysis / "transaction_measurements.csv")
    dispatch = read_csv(run_dir / "replay_dispatch.csv")
    intended = {int(r["seq"]): r for r in dispatch}
    events: list[dict] = []
    for row in dispatch:
        events.append({"timestamp_utc": epoch_to_utc_ms(float(row["dispatch_ms"])), "source": "replay_dispatch",
                       "event_type": "dispatch", "seq": int(row["seq"]), "intended_trace_offset_ms": float(row["intended_offset_ms"]),
                       "dispatch_lag_ms": float(row["lag_ms"]), "provenance": "Caliper workload dispatch log"})
    for row in tx:
        events.append({"timestamp_utc": epoch_to_utc_ms(float(row["final_ms"])), "source": "caliper_recorder",
                       "event_type": "client_completion", "tx_id": row["tx_id"], "seq": int(row["seq_finish"]),
                       "status": row["status"], "latency_ms": float(row["latency_ms"]),
                       "provenance": "e1_per_tx_recorder gateway peer commit status"})
    event_path = run_dir / "synchronized_timeline.jsonl"
    with event_path.open("x", encoding="utf-8", newline="\n") as fh:
        for event in sorted(events, key=lambda r: (r["timestamp_utc"], r.get("event_type", ""), r.get("seq", -1))):
            fh.write(json.dumps(event, sort_keys=True) + "\n")

    base = dt.datetime.fromisoformat(trace_rows[0]["timestamp_utc"])
    dispatch_start = min(float(r["dispatch_ms"]) for r in dispatch)
    # The block size in force is the one fetched from the channel before the run (execute() refuses to start
    # unless it equals that channel's verified baseline). It used to be the constant of e1-cfg001 whatever the
    # channel, which would have labelled an e1-r002 run (B=45) as B=40 (2026-11-17, live-shadow fix 1).
    block_size = summary["before"]["config"]["active_config"]["max_message_count"]
    snapshots: list[dict] = []
    history_counts: list[float] = []
    history_timestamps: list[str] = []
    completion_rows = sorted(tx, key=lambda r: float(r["final_ms"]))
    for index, trace in enumerate(trace_rows):
        ts = dt.datetime.fromisoformat(trace["timestamp_utc"])
        end_ms = dispatch_start + index * 1000.0
        completed = [r for r in completion_rows if float(r["final_ms"]) < end_ms + 1000.0]
        window_completed = [r for r in completed if end_ms <= float(r["final_ms"]) < end_ms + 1000.0]
        submitted = sum(1 for r in dispatch if float(r["dispatch_ms"]) < end_ms + 1000.0)
        mean_latency = (sum(float(r["latency_ms"]) for r in window_completed) / len(window_completed)
                        if window_completed else None)
        history_counts.append(float(trace["arrival_count"]))
        history_timestamps.append(ts.isoformat())
        snapshots.append({"timestamp_utc": ts.isoformat(), "capture_timestamp_utc": epoch_to_utc_ms(end_ms),
                          "current_config": {"max_message_count": block_size, "batch_timeout_ms": 2000,
                                              "preferred_max_bytes": PREFERRED_MAX_BYTES, "absolute_max_bytes": None, "orderers": 3},
                          "observed_arrival_rate_tps": float(trace["arrival_count"]),
                          "observed_throughput_tps": float(len(window_completed)),
                          "observed_latency_ms": mean_latency, "queue_depth": max(0, submitted - len(completed)),
                          "failed_transactions": sum(1 for r in window_completed if r["status"] != "success"),
                          "telemetry_age_s": 0.0, "arrival_history_tps": [v for v in history_counts],
                          "arrival_history_timestamps_utc": [v for v in history_timestamps],
                          "trace_bin": trace, "provenance": {"arrival": str(summary["trace"]["series_path"]),
                          "capture": "replay dispatch wall clock anchored to first dispatch", "future_data_excluded": True}})
    snapshot_path = run_dir / "controller_snapshots.jsonl"
    with snapshot_path.open("x", encoding="utf-8", newline="\n") as fh:
        for row in snapshots:
            fh.write(json.dumps(row, sort_keys=True) + "\n")
    return event_path, snapshot_path


def execute(args: argparse.Namespace, run_dir: Path, prepared: dict, trace_rows: list[dict], schedule: list[float]) -> dict:
    net = network_from_file(args.network)
    env = {**os.environ, **net.peer_env(net.orgs()[0])}
    commands: list[dict] = []
    result = {"tool": "synchronized_shadow_replay 0.1", "run_id": args.run_id, "channel": args.channel,
              "live_configuration_submission": False, "configuration_updates": 0, "commands": commands,
              "trace": {"series_path": str(args.series), "series_sha256": sha256(args.series),
                        "start_bin": args.start_bin, "duration_bins": args.duration_bins,
                        "transactions": len(schedule), "bin_rows": trace_rows}}

    # A live run may finish its Fabric/Caliper work before a local post-
    # processor fails. Resume only that post-processing; never resubmit the
    # workload for an existing analysis artifact.
    existing_summary = run_dir / "analysis" / "summary.json"
    if existing_summary.is_file():
        before_block = json.loads((run_dir / "before_config.json").read_text(encoding="utf-8"))
        after_block = json.loads((run_dir / "after_config.json").read_text(encoding="utf-8"))
        before_cfg = {"active_config": ca.config_from_block(before_block)[0].as_dict()}
        after_cfg = {"active_config": ca.config_from_block(after_block)[0].as_dict()}
        result["before"] = {"config": before_cfg}
        result["after"] = {"config": after_cfg}
        result["configuration_unchanged"] = before_cfg["active_config"] == after_cfg["active_config"]
        result["analysis"] = json.loads(existing_summary.read_text(encoding="utf-8"))
        dispatch_src = Path(prepared["dispatch_log"])
        dispatch_dst = run_dir / "replay_dispatch.csv"
        if not dispatch_dst.exists():
            shutil.copy2(dispatch_src, dispatch_dst)
        overflow_src = Path(f"{prepared['dispatch_log']}.overflow.csv")
        if overflow_src.exists() and not (run_dir / "replay_dispatch.overflow.csv").exists():
            shutil.copy2(overflow_src, run_dir / "replay_dispatch.overflow.csv")
        event_path, snapshot_path = build_timeline(run_dir, trace_rows, result)
        result["synchronized_timeline"] = {"events": str(event_path), "controller_snapshots": str(snapshot_path),
                                            "event_count": sum(1 for _ in event_path.open(encoding="utf-8")),
                                            "snapshot_count": len(trace_rows), "resumed_postprocessing": True}
        write_json(run_dir / "replay_record.json", result)
        return result

    before_height, rec = pilot.getinfo(net, args.channel, env, run_dir, "before_getinfo")
    commands.append(rec)
    before_cfg, cfg = pilot.fetch_config(net, args.channel, env, run_dir, "before_config")
    commands.extend(cfg.values())
    active = before_cfg["active_config"]
    expected_b = args.expected_max_message_count
    if (active["max_message_count"], active["batch_timeout_ms"], active["preferred_max_bytes"], active["orderers"]) != \
            (expected_b, 2000, PREFERRED_MAX_BYTES, 3):
        raise RuntimeError(f"target channel configuration is not the verified baseline: {active}")
    result["before"] = {"height": int(before_height["height"]), "config": before_cfg}

    metrics_script = HERE / "e1_metrics_scraper.py"
    before_metrics = scrape_once(metrics_script, "before", run_dir / "metrics_before", env)
    commands.append(before_metrics)
    metrics_handle = start_metrics(metrics_script, run_dir, env)
    caliper_cmd = ["docker", "exec", "-w", CALIPER_WS, CALIPER_CONTAINER, "caliper", "launch", "manager",
                   "--caliper-workspace", CALIPER_WS, "--caliper-benchconfig", f"runs/{args.run_id}/benchmark.yaml",
                   "--caliper-networkconfig", f"runs/{args.run_id}/networkconfig.yaml", "--caliper-flow-only-test"]
    caliper = run_cmd("caliper", caliper_cmd, env, run_dir, args.caliper_timeout_s)
    commands.append(caliper)
    commands.append(stop_metrics(metrics_handle))
    after_metrics = scrape_once(metrics_script, "after", run_dir / "metrics_after", env)
    commands.append(after_metrics)
    if caliper["exit_code"] != 0:
        raise RuntimeError(f"Caliper failed; see {caliper['stderr']}")

    after_height, rec = pilot.getinfo(net, args.channel, env, run_dir, "after_getinfo")
    commands.append(rec)
    after_cfg, cfg = pilot.fetch_config(net, args.channel, env, run_dir, "after_config")
    commands.extend(cfg.values())
    result["after"] = {"height": int(after_height["height"]), "config": after_cfg}
    result["configuration_unchanged"] = after_cfg["active_config"] == active
    if not result["configuration_unchanged"]:
        raise RuntimeError("target channel configuration changed during replay")

    ledger_dir = run_dir / "ledger"
    first = int(before_height["height"])
    fetch_cmd = [sys.executable, "-B", str(HERE / "e1_block_fetch.py"), "fetch", "--network", str(args.network),
                 "--channel", args.channel, "--first", str(first), "--max-message-count", str(expected_b),
                 "--preferred-max-bytes", str(PREFERRED_MAX_BYTES), "--out-dir", str(ledger_dir), "--execute"]
    fetch = run_cmd("block_fetch", fetch_cmd, env, run_dir, 300)
    commands.append(fetch)
    if fetch["exit_code"] != 0:
        raise RuntimeError(f"block fetch failed; see {fetch['stderr']}")
    recorder_base = Path(prepared["recorder_dir"]) / f"{args.run_id}_w0_r0_m-{args.run_id}"
    ledger_tx = ledger_dir / "ledger" / "ledger_tx.csv"
    metrics_summary_dir = run_dir / "metrics_summary"
    summarize_cmd = [sys.executable, "-B", str(metrics_script), "summarize", "--before", str(run_dir / "metrics_before"),
                     "--during", str(run_dir / "metrics_during"), "--after", str(run_dir / "metrics_after"),
                     "--channel", args.channel, "--fresh-channel", "--expect-traffic", "--out-dir", str(metrics_summary_dir)]
    summary_run = run_cmd("metrics_summary", summarize_cmd, env, run_dir, 60)
    commands.append(summary_run)
    analysis_dir = run_dir / "analysis"
    avg_rate = sum(row["arrival_count"] for row in trace_rows) / len(trace_rows)
    analysis_cmd = [sys.executable, "-B", str(HERE / "e1_benchmark_analyze.py"), "analyze", "--run-id", args.run_id,
                    "--recorder-base", str(recorder_base), "--ledger-tx", str(ledger_tx), "--out-dir", str(analysis_dir),
                    "--expected-n", str(len(schedule)), "--max-message-count", str(expected_b),
                    "--preferred-max-bytes", str(PREFERRED_MAX_BYTES), "--batch-timeout", "2s", "--rate-tps", str(avg_rate),
                    "--metrics-summary", str(metrics_summary_dir / "metrics_summary.json")]
    analysis = run_cmd("analysis", analysis_cmd, env, run_dir, 120)
    commands.append(analysis)
    if analysis["exit_code"] != 0:
        raise RuntimeError(f"analysis failed; see {analysis['stderr']}")
    summary = json.loads((analysis_dir / "summary.json").read_text(encoding="utf-8"))
    result["analysis"] = summary
    dispatch_src = Path(prepared["dispatch_log"])
    dispatch_dst = run_dir / "replay_dispatch.csv"
    if not dispatch_dst.exists():
        shutil.copy2(dispatch_src, dispatch_dst)
    overflow_src = Path(f"{prepared['dispatch_log']}.overflow.csv")
    if overflow_src.exists() and not (run_dir / "replay_dispatch.overflow.csv").exists():
        shutil.copy2(overflow_src, run_dir / "replay_dispatch.overflow.csv")
    event_path, snapshot_path = build_timeline(run_dir, trace_rows, result)
    result["synchronized_timeline"] = {"events": str(event_path), "controller_snapshots": str(snapshot_path),
                                       "event_count": sum(1 for _ in event_path.open(encoding="utf-8")),
                                       "snapshot_count": len(trace_rows)}
    write_json(run_dir / "replay_record.json", result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--series", type=Path, default=DEFAULT_SERIES)
    parser.add_argument("--network", type=Path, default=ROOT / "fabric-testbed/network/network-e1-recovery.json")
    parser.add_argument("--run-id", required=True)
    # Required, no default: a run never falls back to a protected reference channel (owner decision D1, 2026-11-17),
    # and (task A0b) the two protected channels are refused outright. The expected block size is a separate input.
    parser.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL",
                        help="the dedicated validation channel")
    parser.add_argument("--expected-max-message-count", required=True, type=int, metavar="B",
                        help="the channel's BatchSize.MaxMessageCount; execute refuses to start if the fetched configuration differs")
    parser.add_argument("--start-bin", type=int, default=0)
    parser.add_argument("--duration-bins", type=int, default=30)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    parser.add_argument("--caliper-timeout-s", type=float, default=180.0)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.expected_max_message_count < 1:
        parser.error("--expected-max-message-count must be a positive integer")
    if not sys.platform.startswith("linux") and args.execute:
        parser.error("--execute must run inside Ubuntu WSL")
    if args.series.suffix != ".gz" or not args.series.is_file():
        parser.error(f"missing gzip Flashbots series {args.series}")
    if args.network.suffix != ".json" or not args.network.is_file():
        parser.error(f"missing network description {args.network}")
    run_dir = args.out_root / args.run_id
    rows, schedule = parse_series(args.series, args.start_bin, args.duration_bins)
    existing_manifest = run_dir / "replay_manifest.json"
    if run_dir.exists() and (not args.execute or not existing_manifest.is_file()):
        parser.error(f"refusing to overwrite {run_dir}")
    if args.execute and existing_manifest.is_file():
        manifest = json.loads(existing_manifest.read_text(encoding="utf-8"))
        if manifest.get("mode") != "prepared":
            parser.error(f"existing manifest is not a prepare-only artifact: {existing_manifest}")
        prepared = manifest["prepared_caliper"]
    else:
        workspace = Path(args.workspace)
        prepared = render_files(args.run_id, args.channel, schedule,
                                f"{CALIPER_WS}/runs/{args.run_id}/replay_dispatch.csv", workspace, ROOT)
        run_dir.mkdir(parents=True)
        manifest = {"tool": "synchronized_shadow_replay 0.1", "mode": "prepared",
                    "run_id": args.run_id, "channel": args.channel, "live_configuration_submission": False,
                    "channel_selection": cs.selection_record(args.channel, role=cs.ROLE_EXPERIMENT, option="--channel"),
                    "expected_max_message_count": args.expected_max_message_count,
                    "configuration_updates": 0, "series": {"path": str(args.series), "sha256": sha256(args.series),
                    "start_bin": args.start_bin, "duration_bins": args.duration_bins, "rows": rows,
                    "transactions": len(schedule), "schedule_derivation": "uniform deterministic offsets within each 1-second bin"},
                    "prepared_caliper": prepared, "network": str(args.network),
                    "safety": {"existing_channel_only": True, "configuration_update_disabled": True,
                                "chaincode_update_disabled": True, "full_day_replay_disabled": True}}
        write_json(existing_manifest, manifest)
    if not args.execute:
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0
    result = execute(args, run_dir, prepared, rows, schedule)
    print(json.dumps({"run_id": args.run_id, "channel": args.channel,
                      "transactions": len(schedule), "analysis_verdict": result["analysis"].get("analysis_verdict"),
                      "configuration_unchanged": result["configuration_unchanged"],
                      "live_configuration_submission": False}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
