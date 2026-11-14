"""Replay renderer: turns a ReplayWindow into a Caliper run workspace. Offline: it only writes files. It never runs Caliper.

Derived from scripts/e1/e1_la2_render.py (imported, not edited). What differs from the P1b renderer:
  * two rounds, both driven by Caliper's replay-rate controller from absolute millisecond offsets, never fixed-rate:
      round 0  label wu-<run_id>  pre-roll, 60 s of the real trace before the window, UNMEASURED
      round 1  label m-<run_id>   measured replay, exactly the window [0, 600 s)
    The labels reuse P1b's warm-up/measured slots, so the P1b recorder naming and analysis tool keep working unchanged.
  * txNumber = number of scheduled arrivals in each round (one worker, so exact: Caliper divides txNumber per worker with floor,
    and floor(N/1) = N). This is what makes "scheduled == dispatched" a testable identity. The 600 s is a property of the
    schedule, enforced here, not of a Caliper timer.
  * the workload module is e1_trace_replay.js, which logs intended and actual dispatch time per transaction.
  * expect_block_pattern is False: the registered block-pattern expectation was defined for constant-rate arrivals.
LIVE_UNTESTED: Caliper's parsing of the schedule files, its replay-rate behaviour and the workload's timing on real Fabric were not
exercised. Facts about replay-rate (absolute offsets; sleeps of 5 ms or less skipped; open loop) are carried from earlier
notes and were not re-read from Caliper source in this phase.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import shutil
import sys
from pathlib import Path

import numpy as np

from . import PACKAGE_VERSION, REPO_ROOT, ensure_paths
from .offsets import (MEASURED_ROUND_INDEX, PRE_ROUND_INDEX, ReplayWindow, ScheduleError, build_tx_map, rounds, schedule_bytes,
                      sha256_hex, write_tx_map)

ensure_paths()

import e1_la2_render as lr  # noqa: E402  (existing renderer: reused)
import e1_run_orchestrator as ro  # noqa: E402  (existing plan validator: reused)
import e1_telemetry_common as tc  # noqa: E402

TOOL_VERSION = f"p4_replay.render {PACKAGE_VERSION}"
REPLAY_RATE_OPTS = {"inputFormat": "TEXT", "defaultSleepTime": 100}     # as used by the 11-11 shadow replay (run 005)
MODULES = {"e1_per_tx_recorder.js": "scripts/e1/caliper/e1_per_tx_recorder.js",
           "e1_workload_core.js": "fabric-testbed/caliper/workloads/e1_workload_core.js",
           "e1_trace_replay.js": "fabric-testbed/caliper/workloads/e1_trace_replay.js"}
ROUND_LABEL = {PRE_ROUND_INDEX: "wu-{run}", MEASURED_ROUND_INDEX: "m-{run}"}


def benchmark_doc(env: dict, run_id: str, channel: str, window: ReplayWindow) -> tuple[dict, dict]:
    """(benchmark.yaml document, the rest of lr.caliper_files: networkconfig and connection profile)."""
    run_c = posixpath.join(lr.CAL_WS, "runs", run_id)
    module = posixpath.join(run_c, "modules", "e1_trace_replay.js")
    base_args = {"runId": run_id, "contractId": env["chaincode"]["name"], "invokerIdentity": "User1", "valueBytes": 128,
                 "mode": "create", "failEvery": 25, "rmwKey": "unused", "rateControlled": True, "settleSeconds": 0}
    built = lr.caliper_files(env, run_id, channel, 1, 1.0, 0, "create", None)    # reused for monitors, network config, profile
    docs = []
    for rnd in rounds(window):
        schedule_ms = [int(v) for v in rnd["offsets"]]
        docs.append({
            "label": ROUND_LABEL[rnd["round"]].format(run=run_id), "txNumber": len(schedule_ms),
            "rateControl": {"type": "replay-rate", "opts": {**REPLAY_RATE_OPTS,
                                                            "pathTemplate": posixpath.join(run_c, f"schedule_{rnd['name']}.txt")}},
            "workload": {"module": module, "arguments": {**base_args, "scheduleMs": schedule_ms,
                                                         "dispatchLog": posixpath.join(run_c, f"dispatch_{rnd['name']}.csv")}}})
    bench = {"test": {"name": f"e1-{run_id}", "workers": {"number": 1}, "rounds": docs}, "monitors": built["benchmark.yaml"]["monitors"]}
    return bench, {"networkconfig.yaml": built["networkconfig.yaml"], "ccp-org1.json": built["ccp-org1.json"]}


def replay_plan(env: dict, network_path: str, run_id: str, channel: str, B: int, tau: str, P: int, window: ReplayWindow,
                network_doc: dict | None = None) -> dict:
    """The run plan. The stored plan keeps `network_path` (a WSL path for the live run). `network_doc`, when given, is the same network
    description loaded locally, so the orchestrator's own validator can run offline without that path being readable here."""
    n = int(window.measured_ms.size)
    cell = {"B": B, "tau": tau, "P": P, "n": n, "mode": "txNumber", "rate_tps": round(n / window.length_s, 6)}
    plan = lr.run_plan(env, network_path, run_id, channel, cell, window.pre_roll_s, False)
    ro.Plan({**plan, "network": network_doc} if network_doc is not None else plan)    # raises PlanError on a malformed plan
    return plan


def verify_rendered(out: Path, window: ReplayWindow, run_id: str) -> dict:
    """Re-read what was written and prove it equals the window: schedule files, round arguments, counts, keys, tx map."""
    bench = json.loads((out / "benchmark.yaml").read_text(encoding="utf-8"))
    checks: dict = {}
    for rnd, doc in zip(rounds(window), bench["test"]["rounds"]):
        body = (out / f"schedule_{rnd['name']}.txt").read_bytes()
        parsed = np.array([int(x) for x in body.decode("ascii").split("\n")], dtype=np.int64)
        args = doc["workload"]["arguments"]
        ok = (b"\r" not in body and not body.endswith(b"\n") and np.array_equal(parsed, rnd["offsets"])
              and args["scheduleMs"] == [int(v) for v in rnd["offsets"]] and doc["txNumber"] == len(rnd["offsets"])
              and doc["rateControl"]["type"] == "replay-rate" and args["rateControlled"] is True
              and doc["label"] == ROUND_LABEL[rnd["round"]].format(run=run_id))
        checks[rnd["name"]] = {"file_equals_window_offsets": bool(np.array_equal(parsed, rnd["offsets"])), "lf_only_no_trailing_newline": bool(b"\r" not in body and not body.endswith(b"\n")),
                               "args_scheduleMs_equals_file": args["scheduleMs"] == [int(v) for v in parsed], "txNumber_equals_count": doc["txNumber"] == len(rnd["offsets"]),
                               "ok": bool(ok)}
        if not ok:
            raise ScheduleError(f"rendered round {rnd['name']} does not match the window: {checks[rnd['name']]}")
    return checks


def render_replay_run(env: dict, network_path: str, run_id: str, channel: str, B: int, tau: str, P: int, window: ReplayWindow,
                      out_plan: Path, *, ws_root: Path | None = None, repo_root: Path | None = None, identifiers: dict | None = None,
                      day_start_ms: int | None = None, lineage: dict | None = None, argv: list[str] | None = None,
                      network_doc: dict | None = None) -> dict:
    if not lr.RUN_ID.match(run_id):
        raise ValueError(f"run id {run_id!r} must match {lr.RUN_ID.pattern}")
    import re
    if not re.match(r"^e1-[A-Za-z0-9_.-]{1,40}$", channel):
        raise ValueError(f"channel {channel!r} must be an E1 channel name")
    plan = replay_plan(env, network_path, run_id, channel, B, tau, P, window, network_doc)
    bench, netfiles = benchmark_doc(env, run_id, channel, window)
    repo = repo_root or REPO_ROOT
    out = (ws_root or Path(env["caliper_workspace"])) / "runs" / run_id
    out.mkdir(parents=True, exist_ok=False)
    (out / "modules").mkdir()
    files: dict = {"benchmark.yaml": tc.write_json_x(out / "benchmark.yaml", bench)}
    for name, doc in netfiles.items():
        files[name] = tc.write_json_x(out / name, doc)
    schedules: dict = {}
    for rnd in rounds(window):
        body = schedule_bytes(rnd["offsets"])
        schedules[rnd["name"]] = {"file": f"schedule_{rnd['name']}.txt", "sha256": tc.write_bytes_x(out / f"schedule_{rnd['name']}.txt", body),
                                  "n_tx": int(rnd["offsets"].size), "first_offset_ms": int(rnd["offsets"][0]), "last_offset_ms": int(rnd["offsets"][-1]),
                                  "round_index": rnd["round"], "measured": rnd["measured"]}
    modules: dict = {}
    for name, rel in MODULES.items():
        shutil.copyfile(repo / rel, out / "modules" / name)
        a, b = tc.sha256_file(repo / rel), tc.sha256_file(out / "modules" / name)
        if a != b:
            raise RuntimeError(f"copy of {rel} differs from the repo")
        modules[name] = {"source": rel, "sha256": a}
    tx_rows = build_tx_map(run_id, window, day_start_ms=day_start_ms, identifiers=identifiers)
    files["tx_map.csv"] = write_tx_map(out / "tx_map.csv", tx_rows)
    checks = verify_rendered(out, window, run_id)
    record = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "status": "RENDERED_OFFLINE_NOT_EXECUTED",
              "run_id": run_id, "channel": channel, "cell": plan["cell"],
              "window": {"id": window.window_id, "day": window.day, "start_s": window.start_s, "length_s": window.length_s,
                         "pre_roll_s": window.pre_roll_s, "schedule": "A"},
              "rounds": {"pre_roll": {"round_index": PRE_ROUND_INDEX, "label": ROUND_LABEL[PRE_ROUND_INDEX].format(run=run_id), "measured": False},
                         "measured": {"round_index": MEASURED_ROUND_INDEX, "label": ROUND_LABEL[MEASURED_ROUND_INDEX].format(run=run_id), "measured": True}},
              "schedules": schedules, "files": files, "modules": modules, "verified": checks,
              "identifiers": "LOADED_FROM_ARCHIVE" if identifiers is not None else "NOT_LOADED (source_hash column empty)",
              "lineage_to_p1_p5": lineage, "expect_block_pattern": False,
              "planned_dispatch_outputs": [f"dispatch_{n}.csv" for n in ("pre_roll", "measured")] + [f"dispatch_{n}.csv.overflow.csv" for n in ("pre_roll", "measured")],
              "notes": ["LIVE_UNTESTED: Caliper parse of the schedule files and replay-rate timing were never exercised",
                        "settleSeconds is 0: e1_trace_replay.js has no settle step (only e1_create_asset.js implements one); the gap between the pre-roll round and the measured round is whatever Caliper's own round turnover takes, and is not recorded in the schedule"]}
    record["plan_sha256"] = tc.write_json_x(out_plan, plan)
    tc.write_json_x(out / "render_record.json", record)
    return {"plan": plan, "record": record, "dir": out}


def main(argv: list[str] | None = None) -> int:
    from .offsets import load_registered_window
    ap = argparse.ArgumentParser(description="Render one replay run workspace (offline; starts nothing).")
    ap.add_argument("--env", required=True, type=Path)
    ap.add_argument("--network", required=True, help="the network.json path as the LIVE run will see it (a WSL path)")
    ap.add_argument("--network-file", type=Path, help="the same network.json, readable here, for offline plan validation")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--channel", required=True)
    ap.add_argument("--B", required=True, type=int)
    ap.add_argument("--tau", default="2s")
    ap.add_argument("--P", type=int, default=10_485_760)
    ap.add_argument("--window", required=True, help="registered window id, e.g. F_82800")
    ap.add_argument("--processed-dir", type=Path, default=REPO_ROOT / "data" / "processed" / "flashbots_ms")
    ap.add_argument("--ws-root", type=Path, help="workspace root (default: the env's caliper_workspace)")
    ap.add_argument("--out-plan", required=True, type=Path)
    args = ap.parse_args(argv)
    try:
        env = lr.load_env(args.env)
        if args.out_plan.exists():
            print(f"refusing to overwrite {args.out_plan}")
            return 2
        window, lineage = load_registered_window(args.processed_dir, args.window)
        net_doc = json.loads(args.network_file.read_text(encoding="utf-8")) if args.network_file else None
        r = render_replay_run(env, args.network, args.run_id, args.channel, args.B, args.tau, args.P, window, args.out_plan,
                              ws_root=args.ws_root, lineage=lineage, argv=sys.argv if argv is None else argv, network_doc=net_doc)
        print(f"run files -> {r['dir']}; plan -> {args.out_plan} (rendered only; nothing executed)")
        return 0
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as err:
        print(f"INVALID: {err!r}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
