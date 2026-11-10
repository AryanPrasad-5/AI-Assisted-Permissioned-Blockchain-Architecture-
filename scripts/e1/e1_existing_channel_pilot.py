                                                                      

                                                                          
                                                                         
                                                                            
                                                                      
   

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import e1_benchmark_analyze as ba              
import e1_config_actuator as ca              
import e1_run_orchestrator as ro              


INFO_RE = re.compile(r"Blockchain info:\s*(\{.*\})")
ALLOWED_CHANNELS = {"e1-r002", "e1-cfg001"}


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as fh:
        json.dump(value, fh, indent=2, sort_keys=True)
        fh.write("\n")


def run_cmd(label: str, argv: list[str], env: dict[str, str], out_dir: Path, timeout_s: float) -> dict:
    started = time.time()
    try:
        proc = subprocess.run(argv, env=env, capture_output=True, timeout=timeout_s, check=False)
        code = proc.returncode
        out, err = proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as exc:
        code = None
        out = exc.stdout or b""
        err = (exc.stderr or b"") + f"\n{label} timed out after {timeout_s}s\n".encode()
    (out_dir / f"{label}.stdout").write_bytes(out)
    (out_dir / f"{label}.stderr").write_bytes(err)
    return {"label": label, "argv": argv, "start_epoch": started, "end_epoch": time.time(),
            "exit_code": code, "stdout": f"{label}.stdout", "stderr": f"{label}.stderr"}


def getinfo(net, channel: str, env: dict[str, str], out_dir: Path, label: str) -> tuple[dict, dict]:
    rec = run_cmd(label, [net.tool("peer"), "channel", "getinfo", "-c", channel], env, out_dir, 30)
    text = ((out_dir / rec["stdout"]).read_bytes() + b"\n" + (out_dir / rec["stderr"]).read_bytes()).decode("utf-8", "replace")
    match = INFO_RE.search(text)
    if rec["exit_code"] != 0 or not match:
        raise RuntimeError(f"{label} did not return usable channel height")
    return json.loads(match.group(1)), rec


def fetch_config(net, channel: str, env: dict[str, str], out_dir: Path, label: str) -> tuple[dict, dict]:
    pb = out_dir / f"{label}.pb"
    decoded = out_dir / f"{label}.json"
    fetch = run_cmd(f"{label}_fetch", [net.tool("peer"), "channel", "fetch", "config", str(pb), "-c", channel], env, out_dir, 30)
    if fetch["exit_code"] != 0:
        raise RuntimeError(f"{label} config fetch failed")
    decode = run_cmd(f"{label}_decode", [net.tool("configtxlator"), "proto_decode", "--input", str(pb), "--type", "common.Block", "--output", str(decoded)], env, out_dir, 30)
    if decode["exit_code"] != 0:
        raise RuntimeError(f"{label} config decode failed")
    block = json.loads(decoded.read_text(encoding="utf-8"))
    active, policy = ca.config_from_block(block)
    return {"active_config": active.as_dict(), "policy": policy,
            "block_number": ca.block_number(block), "block_timestamp_utc": ca.block_timestamp_utc(block),
            "block_data_hash": ca.block_data_hash(block), "block_sha256": ca.sha256(pb),
            "block_json": str(decoded), "block_pb": str(pb)}, {"fetch": fetch, "decode": decode}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--caliper-timeout-s", type=float, default=120.0)
    ap.add_argument("--caliper-container", default="caliper-benchmark-001")
    ap.add_argument("--allow-duration", action="store_true", help="allow an explicitly rendered txDuration plan")
    args = ap.parse_args(argv)
    plan = ro.Plan(json.loads(args.plan.read_text(encoding="utf-8")))
    if plan.request.channel not in ALLOWED_CHANNELS:
        raise ValueError(f"pilot only permits existing verified channels: {sorted(ALLOWED_CHANNELS)}")
    mode = plan.raw["cell"].get("mode", "txNumber")
    if mode == "txNumber" and plan.raw["cell"]["n"] != 60:
        raise ValueError("pilot arms must use exactly n=60")
    if mode == "txDuration" and not args.allow_duration:
        raise ValueError("duration plans require --allow-duration; fixed-count pilot behavior is unchanged")
    if args.run_dir.exists():
        raise ValueError(f"refusing to overwrite {args.run_dir}")
    if not args.execute:
        print(json.dumps({"mode": "dry-run", "channel": plan.request.channel, "run_id": plan.raw["run_id"],
                          "n": plan.raw["cell"]["n"], "mode": mode,
                          "duration_s": plan.raw["cell"].get("duration_s"),
                          "rate_tps": plan.raw["cell"].get("rate_tps"),
                          "caliper_timeout_s": args.caliper_timeout_s,
                          "note": "no channel setup or workload executed"}, indent=2))
        return 0
    if not sys.platform.startswith("linux"):
        raise RuntimeError("--execute must run inside Ubuntu WSL")

    run_dir = args.run_dir
    run_dir.mkdir(parents=True)
    (run_dir / "run_plan.json").write_text(json.dumps(plan.raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    net = plan.network
    env = {**os.environ, **net.peer_env(net.orgs()[0])}
    commands: list[dict] = []
    result = {"tool": "e1_existing_channel_pilot 0.1", "run_id": plan.raw["run_id"],
              "channel": plan.request.channel, "cell": plan.raw["cell"], "bounded": {"n": plan.raw["cell"]["n"],
              "mode": mode, "duration_s": plan.raw["cell"].get("duration_s"),
              "caliper_timeout_s": args.caliper_timeout_s, "allowed_channels": sorted(ALLOWED_CHANNELS)},
              "commands": commands, "channel_setup": "SKIPPED_READ_ONLY_EXISTING_CHANNEL"}
    try:
        before_height, rec = getinfo(net, plan.request.channel, env, run_dir, "before_getinfo")
        commands.append(rec)
        before_cfg, cfg_commands = fetch_config(net, plan.request.channel, env, run_dir, "before_config")
        commands.extend(cfg_commands.values())
        result["before"] = {"height": int(before_height["height"]), "config": before_cfg}
        actual = before_cfg["active_config"]
        if actual["max_message_count"] != plan.request.max_message_count or actual["batch_timeout_ms"] != plan.request.tau_ms \
                or actual["preferred_max_bytes"] != plan.request.preferred_max_bytes or actual["orderers"] != len(net.raw["orderers"]):
            raise RuntimeError(f"active configuration does not match pilot cell: {actual}")
        caliper_argv = list(plan.raw["caliper"]["argv"])
        if len(caliper_argv) >= 5 and caliper_argv[:4] == ["docker", "exec", "-w", "/hyperledger/caliper/workspace"]:
            caliper_argv[4] = args.caliper_container
        caliper = run_cmd("caliper", caliper_argv, env, run_dir, args.caliper_timeout_s)
        commands.append(caliper)
        if caliper["exit_code"] != 0:
            raise RuntimeError(f"Caliper failed or timed out; see {run_dir / caliper['stderr']}")
        after_height, rec = getinfo(net, plan.request.channel, env, run_dir, "after_getinfo")
        commands.append(rec)
        after_cfg, cfg_commands = fetch_config(net, plan.request.channel, env, run_dir, "after_config")
        commands.extend(cfg_commands.values())
        result["after"] = {"height": int(after_height["height"]), "config": after_cfg}
        if after_cfg["active_config"] != actual:
            raise RuntimeError("channel configuration changed during pilot arm")
        first = int(before_height["height"])
        fetch_dir = run_dir / "ledger"
        fetch_cmd = [sys.executable, "-B", str(HERE / "e1_block_fetch.py"), "fetch", "--network", str(args.plan.parent / "network-e1-recovery.json"),
                     "--channel", plan.request.channel, "--first", str(first), "--max-message-count", str(plan.request.max_message_count),
                     "--preferred-max-bytes", str(plan.request.preferred_max_bytes), "--out-dir", str(fetch_dir), "--execute"]
                                                                                
                                                                            
        network_arg = plan.raw["network"] if isinstance(plan.raw["network"], str) else None
        if network_arg:
            fetch_cmd[fetch_cmd.index(str(args.plan.parent / "network-e1-recovery.json"))] = network_arg
        rec = run_cmd("block_fetch", fetch_cmd, env, run_dir, 300)
        commands.append(rec)
        if rec["exit_code"] != 0:
            raise RuntimeError("block fetch failed")
                                                                      
                                                                   
        ledger_dir = run_dir / "ledger" / "ledger"
        if not (ledger_dir / "ledger_tx.csv").is_file() or not (ledger_dir / "blocks.csv").is_file():
            raise RuntimeError("block fetch completed without ledger tables")
        base = plan.recorder_base("measured_round", "measured_label")
        if base is None:
            raise RuntimeError("measured recorder base is missing")
        if mode == "txDuration":
            submits_path = base.with_name(base.name + ".submits.csv")
            with submits_path.open("r", newline="", encoding="utf-8") as fh:
                expected_n = sum(int(row["count"]) for row in csv.DictReader(fh))
            if expected_n < 1:
                raise RuntimeError("duration run produced no submitted transactions")
        else:
            expected_n = int(plan.raw["cell"]["n"])
        analysis_dir = run_dir / "analysis"
        analysis_cmd = [sys.executable, "-B", str(HERE / "e1_benchmark_analyze.py"), "analyze", "--run-id", plan.raw["run_id"],
                        "--recorder-base", str(base), "--ledger-tx", str(ledger_dir / "ledger_tx.csv"), "--out-dir", str(analysis_dir),
                        "--expected-n", str(expected_n), "--max-message-count", str(plan.request.max_message_count), "--preferred-max-bytes", str(plan.request.preferred_max_bytes),
                        "--batch-timeout", "2s", "--rate-tps", str(plan.raw["cell"]["rate_tps"])]
        warmup_base = plan.recorder_base("warmup_round", "warmup_label")
        if warmup_base is not None:
            warmup_csv = warmup_base.with_name(warmup_base.name + ".csv")
            if warmup_csv.exists():
                analysis_cmd += ["--other-round-csv", str(warmup_csv)]
        rec = run_cmd("analysis", analysis_cmd, env, run_dir, 60)
        commands.append(rec)
        if rec["exit_code"] != 0:
            raise RuntimeError("benchmark analysis failed")
        summary = json.loads((analysis_dir / "summary.json").read_text(encoding="utf-8"))
        result["analysis"] = {"summary": str(analysis_dir / "summary.json"), "metrics": summary["metrics"],
                               "counts": summary["counts"], "verdict": summary["analysis_verdict"]}
        result["status"] = "COMPLETE"
    except Exception as exc:
        result["status"] = "BLOCKED_OR_FAILED"
        result["error"] = f"{type(exc).__name__}: {exc}"
    result["commands"] = commands
    write_json(run_dir / "pilot_run_record.json", result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "COMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
