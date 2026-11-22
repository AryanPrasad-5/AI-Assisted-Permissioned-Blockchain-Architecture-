"""Stage the closed-loop runs OFFLINE (Windows venv; numpy via p4_replay). Writes files only; starts nothing.

For every run: <stage>/<run_id>/workspace/ (copied by the WSL runner into <caliper_ws>/runs/<run_id>/) and <run_id>/arm.json.
<stage>/SHA256SUMS.txt covers every staged file; the owner's authorization pins its SHA-256.

  python -m Fabric_Live_Testbed.closed_loop.stage --stage-root D:/PLAN_BLOCK18/<new dir> --seed <int> \
      --arms static,reactive:R1,predictive --replicates 1 --first-seq 1
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import posixpath
import random
import shutil
import sys
from pathlib import Path

from . import PACKAGE_VERSION, REPO_ROOT, SEGMENT_S, ensure_paths
from .segments import build_rounds, schedule_text

ensure_paths()

import e1_la2_render as lr  # noqa: E402
from p4_replay.offsets import load_registered_window  # noqa: E402

MODULES = {"e1_per_tx_recorder.js": "scripts/e1/caliper/e1_per_tx_recorder.js",
           "e1_workload_core.js": "fabric-testbed/caliper/workloads/e1_workload_core.js",
           "e1_trace_replay.js": "fabric-testbed/caliper/workloads/e1_trace_replay.js"}
REPLAY_RATE_OPTS = {"inputFormat": "TEXT", "defaultSleepTime": 100}
CALIPER_CONTAINER = "caliper-benchmark-001"
MENU_B = (10, 40)
INITIAL_B = 10
DEFAULT_REGISTRATION = REPO_ROOT / "Integration/system_integration/final_live_experiment_2026-11-24/outputs/reactive_threshold_registration_PROPOSED.json"
DEFAULT_PRED_DIR = Path(r"D:\PLAN_BLOCK18\d_e2_10_offline_decision_audit_2026-11-15")
PRED_TABLE_REL = "outputs/decision_table_F_82800_window.csv"


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def write_x(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as fh:
        fh.write(data)
    return sha256_bytes(data)


def json_bytes(doc) -> bytes:
    return (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode("utf-8")


def caliper_argv(run_id: str) -> list[str]:
    return ["docker", "exec", "-w", lr.CAL_WS, CALIPER_CONTAINER, "caliper", "launch", "manager",
            "--caliper-workspace", lr.CAL_WS, "--caliper-benchconfig", f"runs/{run_id}/benchmark.yaml",
            "--caliper-networkconfig", f"runs/{run_id}/networkconfig.yaml", "--caliper-flow-only-test",
            "--caliper-report-path", f"runs/{run_id}/report.html"]


def benchmark_doc(env: dict, run_id: str, channel: str, rounds) -> tuple[dict, dict]:
    run_c = posixpath.join(lr.CAL_WS, "runs", run_id)
    base_args = {"runId": run_id, "contractId": env["chaincode"]["name"], "invokerIdentity": "User1", "valueBytes": 128,
                 "mode": "create", "failEvery": 25, "rmwKey": "unused", "rateControlled": True, "settleSeconds": 0}
    built = lr.caliper_files(env, run_id, channel, 1, 1.0, 0, "create", None)
    docs = []
    for r in rounds:
        docs.append({"label": r.label(run_id), "txNumber": len(r.offsets_ms),
                     "rateControl": {"type": "replay-rate", "opts": {**REPLAY_RATE_OPTS,
                                                                     "pathTemplate": posixpath.join(run_c, f"schedule_{r.name}.txt")}},
                     "workload": {"module": posixpath.join(run_c, "modules", "e1_trace_replay.js"),
                                  "arguments": {**base_args, "scheduleMs": list(r.offsets_ms),
                                                "dispatchLog": posixpath.join(run_c, f"dispatch_{r.name}.csv")}}})
    bench = {"test": {"name": f"e1-{run_id}", "workers": {"number": 1}, "rounds": docs}, "monitors": built["benchmark.yaml"]["monitors"]}
    return bench, {"networkconfig.yaml": built["networkconfig.yaml"], "ccp-org1.json": built["ccp-org1.json"]}


def load_predictive_rows(pred_dir: Path) -> tuple[list[dict], dict]:
    table = pred_dir / PRED_TABLE_REL
    sums = {}
    for line in (pred_dir / "SHA256SUMS.txt").read_text(encoding="utf-8").splitlines():
        if line.strip():
            digest, name = line.split(maxsplit=1)
            sums[name.lstrip("*")] = digest.lower()
    raw = table.read_bytes()
    if sha256_bytes(raw) != sums.get(PRED_TABLE_REL):
        raise ValueError(f"{table}: SHA-256 differs from its SHA256SUMS.txt")
    rows = []
    for r in csv.DictReader(raw.decode("utf-8").splitlines()):
        rows.append({"decision_index": int(r["decision_index"]), "decision_ts_utc": r["origin_ts_utc"],
                     "predictive_B_stored": int(r["predictive_B"]), "current_B_before": int(r["current_B_before"]),
                     "predictive_abstained": r["controller_reason"].endswith("fail_safe"),
                     "predictive_cause_stored": r["decision_cause"], "predictive_forecast_tps_stored": r["forecast_tps"],
                     "controller_reason_stored": r["controller_reason"]})
    rows.sort(key=lambda x: x["decision_index"])
    return rows, {"path": str(table), "sha256": sums[PRED_TABLE_REL], "sums_file": str(pred_dir / "SHA256SUMS.txt")}


def reactive_params(registration: dict, variant: str) -> dict:
    if variant == "R1":
        src = registration
    elif variant == "R0":
        src = registration["alternative_kept_for_disclosure"]["R0_repo_fixture"]
    else:
        raise ValueError("reactive variant must be R1 or R0")
    return {"variant": variant, "congested_B": int(registration["congested_B"]), "latency_high_ms": float(src["latency_high_ms"]),
            "latency_low_ms": float(src["latency_low_ms"]), "consecutive_samples": int(src["consecutive_samples"]),
            "cooldown_samples": int(src["cooldown_samples"]), "max_telemetry_age_s": float(registration["max_telemetry_age_s"]),
            "registration_status": registration["status"]}


def parse_arms(text: str) -> list[tuple[str, str | None]]:
    out = []
    for item in text.split(","):
        arm, _, variant = item.partition(":")
        if arm not in ("static", "reactive", "predictive", "scripted"):
            raise ValueError(f"unknown arm {arm}")
        if arm == "reactive" and variant not in ("R1", "R0"):
            raise ValueError("reactive arm needs :R1 or :R0")
        if arm == "scripted" and not variant:
            raise ValueError("scripted arm needs :B,B,... (one B per decision, / separated), e.g. scripted:10/40/40/10")
        out.append((arm, variant or None))
    return out


def stage(stage_root: Path, *, seed: int, arms: list[tuple[str, str | None]], replicates: int, first_seq: int,
          window_id: str = "F_82800", segments: int | None = None, env_path: Path | None = None,
          registration_path: Path = DEFAULT_REGISTRATION, pred_dir: Path = DEFAULT_PRED_DIR, run_prefix: str = "cl") -> dict:
    if stage_root.exists():
        raise FileExistsError(f"refusing to reuse {stage_root}")
    env = lr.load_env(env_path or REPO_ROOT / "fabric-testbed/network/env-e1-caliper-benchmark.json")
    window, lineage = load_registered_window(REPO_ROOT / "data/processed/flashbots_ms", window_id)
    all_rounds = build_rounds(window.measured_ms.tolist(), window.pre_ms_rel.tolist(), window.length_s, window.pre_roll_s, SEGMENT_S)
    n_seg = len(all_rounds) - 1 if segments is None else int(segments)
    if not 1 <= n_seg <= len(all_rounds) - 1:
        raise ValueError(f"segments must be 1..{len(all_rounds) - 1}")
    rounds = all_rounds[: n_seg + 1]
    t0 = dt.datetime.fromisoformat(window.day).replace(tzinfo=dt.timezone.utc) + dt.timedelta(seconds=window.start_s)
    decision_times = [(t0 + dt.timedelta(seconds=SEGMENT_S * d)).isoformat() for d in range(n_seg)]
    registration = json.loads(registration_path.read_text(encoding="utf-8"))
    pred_rows, pred_src = load_predictive_rows(pred_dir) if any(a == "predictive" for a, _ in arms) else ([], None)
    if pred_rows and window_id != "F_82800":
        raise ValueError("stored predictive decisions exist only for F_82800")

    cells = [(a, v, rep) for rep in range(1, replicates + 1) for a, v in arms]
    order = list(range(len(cells)))
    random.Random(seed).shuffle(order)
    stage_root.mkdir(parents=True)
    sums: dict[str, str] = {}
    runs = []
    src_index = {0: window.pre_src_index.tolist()}
    for r in rounds[1:]:
        lo, hi = r.window_start_ms, r.window_start_ms + r.length_ms
        src_index[r.index] = [int(s) for s, off in zip(window.measured_src_index.tolist(), window.measured_ms.tolist()) if lo <= off < hi]
    for pos, ci in enumerate(order):
        arm, variant, rep = cells[ci]
        run_id = f"{run_prefix}{first_seq + pos:02d}"
        channel = f"e1-{run_id}"
        rd = stage_root / run_id
        ws = rd / "workspace"
        bench, net = benchmark_doc(env, run_id, channel, rounds)
        sums[f"{run_id}/workspace/benchmark.yaml"] = write_x(ws / "benchmark.yaml", json_bytes(bench))
        for name, doc in net.items():
            sums[f"{run_id}/workspace/{name}"] = write_x(ws / name, json_bytes(doc))
        for r in rounds:
            sums[f"{run_id}/workspace/schedule_{r.name}.txt"] = write_x(ws / f"schedule_{r.name}.txt", schedule_text(r.offsets_ms))
        for name, rel in MODULES.items():
            sums[f"{run_id}/workspace/modules/{name}"] = write_x(ws / "modules" / name, (REPO_ROOT / rel).read_bytes())
        lines = ["run_id,round_index,round_label,seq,create_asset_key,source_day,source_row_index,window_offset_ms,round_offset_ms"]
        for r in rounds:
            for seq, (off, src) in enumerate(zip(r.offsets_ms, src_index[r.index])):
                lines.append(f"{run_id},{r.index},{r.label(run_id)},{seq},{run_id}_{r.index}_{seq:07d},{window.day},{src},"
                             f"{r.window_start_ms + off},{off}")
        sums[f"{run_id}/workspace/tx_map.csv"] = write_x(ws / "tx_map.csv", ("\n".join(lines) + "\n").encode("ascii"))
        arm_doc = {"tool": PACKAGE_VERSION, "run_id": run_id, "channel": channel, "replicate": rep, "order_position": pos + 1,
                   "arm": arm, "arm_label": arm if not variant else f"{arm}-{variant}" if arm == "reactive" else f"{arm}-ACTUATION_CHECK",
                   "menu_B": list(MENU_B), "initial_B": INITIAL_B, "batch_timeout": "2s", "preferred_max_bytes": 10_485_760,
                   "window": {"id": window_id, "day": window.day, "start_s": window.start_s, "segments": n_seg, "segment_s": SEGMENT_S,
                              "pre_roll_s": window.pre_roll_s, "lineage": lineage},
                   "rounds": [{"index": r.index, "name": r.name, "label": r.label(run_id), "measured": r.measured, "segment": r.segment,
                               "window_start_ms": r.window_start_ms, "length_ms": r.length_ms, "n_tx": len(r.offsets_ms)} for r in rounds],
                   "decision_times_utc": decision_times,
                   "decision_rule": "decision d is taken when round d (d=0 pre-roll) has finished and applies to round d+1 (segment d)",
                   "caliper_argv": caliper_argv(run_id), "caliper_hard_timeout_s": 1200}
        if arm == "reactive":
            arm_doc["reactive"] = reactive_params(registration, variant)
        elif arm == "predictive":
            arm_doc["predictive"] = {"rows": pred_rows[:n_seg], "source": pred_src}
        elif arm == "scripted":
            seq_b = [int(b) for b in variant.split("/")]
            if len(seq_b) != n_seg or any(b not in MENU_B for b in seq_b):
                raise ValueError(f"scripted schedule needs {n_seg} menu values")
            arm_doc["scripted"] = {"schedule_B": seq_b, "purpose": "ACTUATION_CHECK: proves the live switch path; not a strategy, "
                                                                     "excluded from every strategy comparison"}
        sums[f"{run_id}/arm.json"] = write_x(rd / "arm.json", json_bytes(arm_doc))
        runs.append({"run_id": run_id, "channel": channel, "arm_label": arm_doc["arm_label"], "replicate": rep, "order_position": pos + 1})
    manifest = {"tool": PACKAGE_VERSION, "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                "seed": seed, "window_id": window_id, "segments": n_seg, "runs": runs,
                "inputs": {"registration": {"path": str(registration_path), "sha256": sha256_bytes(registration_path.read_bytes())},
                           "predictive": pred_src, "env": str(env_path or "fabric-testbed/network/env-e1-caliper-benchmark.json"),
                           "modules": {n: sha256_bytes((REPO_ROOT / r).read_bytes()) for n, r in MODULES.items()}},
                "n_tx": {"pre_roll": len(rounds[0].offsets_ms), "measured": sum(len(r.offsets_ms) for r in rounds[1:])}}
    sums["STAGE_MANIFEST.json"] = write_x(stage_root / "STAGE_MANIFEST.json", json_bytes(manifest))
    body = "".join(f"{sums[k]}  {k}\n" for k in sorted(sums)).encode("ascii")
    write_x(stage_root / "SHA256SUMS.txt", body)
    manifest["stage_sha256"] = sha256_bytes(body)
    return manifest


def verify_stage(stage_root: Path) -> str:
    from .stage_verify import verify_stage_tree
    return verify_stage_tree(stage_root)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Stage closed-loop runs (offline; starts nothing).")
    ap.add_argument("--stage-root", required=True, type=Path)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--arms", default="static,reactive:R1,predictive")
    ap.add_argument("--replicates", type=int, default=1)
    ap.add_argument("--first-seq", type=int, default=1)
    ap.add_argument("--segments", type=int)
    ap.add_argument("--run-prefix", default="cl")
    ap.add_argument("--verify", action="store_true", help="only verify an existing stage")
    a = ap.parse_args(argv)
    if a.verify:
        print(f"stage OK; SHA256SUMS.txt sha256 = {verify_stage(a.stage_root)}")
        return 0
    if a.seed is None:
        ap.error("--seed is required to stage")
    m = stage(a.stage_root, seed=a.seed, arms=parse_arms(a.arms), replicates=a.replicates, first_seq=a.first_seq,
              segments=a.segments, run_prefix=a.run_prefix)
    print(json.dumps({"stage_sha256": m["stage_sha256"], "runs": m["runs"], "n_tx": m["n_tx"]}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
