"""Run one bounded Caliper round while observing new Fabric blocks.

This wrapper is deliberately narrower than the full E1 orchestrator: it does
not create a channel, inspect or update configuration, install chaincode, or
run a controller. It reuses the pinned Caliper recorder and the existing
read-only block verifier while polling only blocks appended after the initial
channel height.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

import e1_block_fetch as bf  # noqa: E402
import e1_channel_run as cr  # noqa: E402
from fabric_perf.controllers import channel_selection as cs  # noqa: E402  (standard library only)

# The channel the stored template run (q22c001, held in WSL) carries. copy_run_template rewrites this literal to
# the selected channel, then verify_template_rewritten proves the rewrite happened (task A0b, 2026-11-20).
TEMPLATE_CHANNEL_PLACEHOLDER = "e1-cfg001"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def run_cmd(argv: list[str], env: dict[str, str] | None = None) -> tuple[int, bytes, bytes]:
    proc = subprocess.run(argv, env=env, capture_output=True, timeout=120, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def get_height(net: cr.Network, channel: str) -> tuple[int | None, str]:
    code, out, err = run_cmd(bf.getinfo_argv(net, channel), env={**__import__("os").environ, **net.peer_env(net.orgs()[0])})
    text = (out + b"\n" + err).decode("utf-8", "replace")
    info = bf.parse_getinfo(text) if code == 0 else None
    return (int(info["height"]) if info else None), text


def verify_template_rewritten(target: Path, channel: str) -> None:
    """Refuse a generated Caliper run that does not name exactly the selected channel.

    The rewrite in copy_run_template replaces one literal. A template that lacks it would silently keep its own
    channel, which could be a protected reference channel. So the generated network configuration must name the
    selected channel, and no file of the run may still name a protected reference channel.
    """
    network = target / "networkconfig.yaml"
    if channel not in network.read_text(encoding="utf-8"):
        raise cs.ChannelSelectionError(
            cs.TEMPLATE_NOT_REWRITTEN,
            f"the generated {network} does not name the selected channel {channel!r}: the template run did not carry "
            f"the placeholder channel {TEMPLATE_CHANNEL_PLACEHOLDER!r}, so nothing was rewritten. Nothing was run.",
            value=channel)
    for path in sorted(target.rglob("*")):
        if not path.is_file() or path.suffix not in {".json", ".yaml", ".js"}:
            continue
        text = path.read_text(encoding="utf-8")
        for name in sorted(cs.PROTECTED_REFERENCE_CHANNELS):
            if name in text:
                raise cs.ChannelSelectionError(
                    cs.PROTECTED_REMAINS,
                    f"{path} still names the protected reference channel {name!r} after the rewrite. Nothing was run.",
                    value=name)


def copy_run_template(template: Path, target: Path, run_id: str, channel: str, container_workspace: str) -> None:
    channel = cs.select_experiment_channel(channel, purpose="a contemporaneous capture run", option="--channel")
    if target.exists():
        raise FileExistsError(f"refusing to overwrite Caliper run directory: {target}")
    shutil.copytree(template, target)
    for path in target.rglob("*"):
        if not path.is_file() or path.suffix not in {".json", ".yaml", ".js"}:
            continue
        text = path.read_text(encoding="utf-8")
        text = text.replace("q22c001", run_id).replace(TEMPLATE_CHANNEL_PLACEHOLDER, channel)
        path.write_text(text, encoding="utf-8")
    network = target / "networkconfig.yaml"
    network.write_text(network.read_text(encoding="utf-8").replace("q22c001", run_id), encoding="utf-8")
    benchmark = json.loads((target / "benchmark.yaml").read_text(encoding="utf-8"))
    for round_def in benchmark["test"]["rounds"]:
        round_def["txDuration"] = 5
        round_def["rateControl"]["opts"]["tps"] = 4.0
    benchmark["test"]["rounds"] = [benchmark["test"]["rounds"][-1]]
    benchmark["test"]["rounds"][0]["label"] = f"m-{run_id}"
    workload = benchmark["test"]["rounds"][0]["workload"]
    workload["arguments"]["runId"] = run_id
    workload["arguments"]["settleSeconds"] = 0
    module_path = f"{container_workspace}/runs/{run_id}/modules/e1_create_asset.js"
    benchmark["test"]["rounds"][0]["workload"]["module"] = module_path
    monitor = benchmark["monitors"]["transaction"][0]
    monitor["options"]["runId"] = run_id
    monitor["options"]["outputDir"] = f"{container_workspace}/runs/{run_id}/recorder"
    monitor["module"] = f"{container_workspace}/runs/{run_id}/modules/e1_per_tx_recorder.js"
    (target / "benchmark.yaml").write_text(json.dumps(benchmark, indent=2) + "\n", encoding="utf-8")
    verify_template_rewritten(target, channel)


def prometheus_sampler(stop: threading.Event, out: Path, endpoints: dict[str, str]) -> None:
    with out.open("x", encoding="utf-8", newline="\n") as fh:
        while not stop.is_set():
            observed = utc_now()
            rows = {}
            for name, url in endpoints.items():
                try:
                    req = urllib.request.Request(url, headers={"Accept": "text/plain;version=0.0.4"})
                    with urllib.request.urlopen(req, timeout=2) as response:
                        body = response.read()
                    rows[name] = {"url": url, "status": int(response.status), "sha256": hashlib.sha256(body).hexdigest()}
                except Exception as exc:
                    rows[name] = {"url": url, "error": f"{type(exc).__name__}: {exc}"}
            fh.write(json.dumps({"observation_timestamp_utc": observed, "endpoints": rows}, sort_keys=True) + "\n")
            fh.flush()
            stop.wait(0.25)


def live_block_observer(stop: threading.Event, net: cr.Network, channel: str, first_block: int,
                        out_dir: Path, observations: Path) -> None:
    blocks_dir = out_dir / "blocks"
    blocks_dir.mkdir(parents=True, exist_ok=True)
    seen: set[int] = set()
    env = {**__import__("os").environ, **net.peer_env(net.orgs()[0])}
    with observations.open("x", encoding="utf-8", newline="\n") as fh:
        while not stop.is_set():
            height, raw_info = get_height(net, channel)
            if height is not None:
                for number in range(first_block, height):
                    if number in seen:
                        continue
                    path = blocks_dir / f"{channel}_{number:06d}_live.block"
                    started = utc_now()
                    code, stdout, stderr = run_cmd(
                        bf.fetch_argv(net, channel, number, path.resolve().as_posix()), env=env)
                    ended = utc_now()
                    if code == 0 and path.is_file() and path.stat().st_size > 0:
                        seen.add(number)
                        row = {"channel": channel, "block_number": number, "status": "ok",
                               "observed_timestamp_utc": ended, "fetch_start_utc": started,
                               "fetch_end_utc": ended, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                               "source": "peer channel fetch during Caliper workload"}
                        fh.write(json.dumps(row, sort_keys=True) + "\n")
                        fh.flush()
                    else:
                        fh.write(json.dumps({"channel": channel, "block_number": number, "status": "FAILED",
                                             "observed_timestamp_utc": ended, "stderr": stderr.decode("utf-8", "replace")[-500:],
                                             "source": "peer channel fetch during Caliper workload"}, sort_keys=True) + "\n")
                        fh.flush()
            stop.wait(0.25)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", default="phase2a3-c001")
    # Required, no default: a capture never falls back to a protected reference channel (owner decision D1,
    # 2026-11-17). The same goes for the block size, which is written into run_metadata.json as a claim about
    # the channel and must never be an assumed constant (live-shadow fix 1).
    ap.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL",
                    help="the dedicated validation channel; the protected reference channels are refused")
    ap.add_argument("--network", type=Path, default=Path("fabric-testbed/network/network-e1-recovery.json"))
    ap.add_argument("--template-run", type=Path, default=Path("/home/mailf18/e1/caliper-ws-benchmark/runs/q22c001"))
    ap.add_argument("--caliper-run", type=Path, default=Path("/home/mailf18/e1/caliper-ws-benchmark/runs/phase2a3-c001"))
    ap.add_argument("--container", default="caliper-benchmark-001")
    ap.add_argument("--container-workspace", default="/hyperledger/caliper/workspace")
    ap.add_argument("--out-dir", type=Path, default=Path("/home/mailf18/e1/final-runs/phase2a3-c001-run"))
    ap.add_argument("--max-message-count", type=int, required=True,
                    help="the channel's BatchSize.MaxMessageCount, recorded as an operator assertion; verify it "
                         "against the fetched channel configuration")
    ap.add_argument("--preferred-max-bytes", type=int, default=10_485_760)
    ap.add_argument("--prom-endpoint", action="append", default=[
        "orderer1=http://localhost:9443/metrics", "orderer2=http://localhost:9446/metrics",
        "orderer3=http://localhost:9447/metrics",
    ])
    args = ap.parse_args(argv)
    if args.out_dir.exists() or args.caliper_run.exists():
        raise SystemExit("refusing to overwrite an existing capture directory")
    network_path = args.network if args.network.is_absolute() else Path.cwd() / args.network
    net = cr.Network(json.loads(network_path.read_text(encoding="utf-8")))
    args.out_dir.mkdir(parents=True)
    (args.out_dir / "run_metadata.json").write_text(json.dumps({
        "run_id": args.run_id, "channel": args.channel, "mode": "bounded_caliper_with_live_ledger_observer",
        "configuration_mutation": False, "max_message_count": args.max_message_count,
        "preferred_max_bytes": args.preferred_max_bytes, "started_utc": utc_now(),
    }, indent=2) + "\n", encoding="utf-8")
    copy_run_template(args.template_run, args.caliper_run, args.run_id, args.channel, args.container_workspace)
    initial_height, info_text = get_height(net, args.channel)
    if initial_height is None:
        raise RuntimeError(f"could not read initial channel height: {info_text[-500:]}")
    (args.out_dir / "initial_getinfo.txt").write_text(info_text, encoding="utf-8")
    ledger_dir = args.out_dir / "ledger_live"
    ledger_dir.mkdir()
    stop = threading.Event()
    endpoints = {}
    for item in args.prom_endpoint:
        name, sep, url = item.partition("=")
        if sep:
            endpoints[name] = url
    prom_thread = threading.Thread(target=prometheus_sampler, args=(stop, args.out_dir / "prometheus_samples.jsonl", endpoints), daemon=True)
    ledger_thread = threading.Thread(target=live_block_observer, args=(stop, net, args.channel, initial_height, ledger_dir,
                                                                        args.out_dir / "ledger_observations.jsonl"), daemon=True)
    prom_thread.start()
    ledger_thread.start()
    container_workspace = args.container_workspace
    command = ["docker", "exec", "-w", container_workspace, args.container, "caliper", "launch", "manager",
               "--caliper-workspace", container_workspace, "--caliper-benchconfig", f"runs/{args.run_id}/benchmark.yaml",
               "--caliper-networkconfig", f"runs/{args.run_id}/networkconfig.yaml", "--caliper-flow-only-test"]
    started = utc_now()
    proc = subprocess.run(command, capture_output=True, timeout=180, check=False)
    ended = utc_now()
    stop.set()
    prom_thread.join(timeout=5)
    ledger_thread.join(timeout=5)
    (args.out_dir / "caliper.stdout").write_bytes(proc.stdout)
    (args.out_dir / "caliper.stderr").write_bytes(proc.stderr)
    (args.out_dir / "execution.json").write_text(json.dumps({"command": command, "started_utc": started,
        "ended_utc": ended, "exit_code": proc.returncode, "initial_height": initial_height,
        "caliper_run": str(args.caliper_run), "ledger_observation": "read-only peer polling"}, indent=2) + "\n", encoding="utf-8")
    if proc.returncode != 0:
        return proc.returncode or 1
    print(json.dumps({"run_id": args.run_id, "caliper_exit_code": proc.returncode, "initial_height": initial_height,
                      "caliper_run": str(args.caliper_run), "out_dir": str(args.out_dir)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
