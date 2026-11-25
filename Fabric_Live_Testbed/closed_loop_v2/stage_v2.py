"""Stage the v2 runs OFFLINE (Windows venv). Writes files only; starts nothing.

  X0 stage  (calibration): runs cx01.. : one fresh channel per run at a fixed B (10 or 40), a 20 tx/s unmeasured pre-roll round and the
            nine constant-rate rungs in a seeded random order, evenly spaced arrivals. arm = static, no decisions.
  M stage   (evaluation): runs cm01.. : the scaled historical window (k * per-minute rates), arms static10, static40, reactive
            (thresholds from X0), predictive_live (causal EWMA + local X0 predictor), shared schedule and seeded run order.

Every arm.json keeps the schema the UNCHANGED closed_loop runner and analyzer read (run_id, channel, arm, arm_label, menu_B, initial_B,
batch_timeout, rounds, decision_times_utc, caliper_argv, ...), plus v2 fields. <stage>/SHA256SUMS.txt covers every file; the owner's
authorization pins its SHA-256 (closed_loop.authorization.ExperimentAuthorization, unchanged).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
import sys
from pathlib import Path

from . import PACKAGE_VERSION, REPO_ROOT, SEGMENT_S, ensure_paths
from . import protocol as P
from . import scaling as S

ensure_paths()

import e1_la2_render as lr  # noqa: E402
from Fabric_Live_Testbed.closed_loop import stage as s1  # noqa: E402
from Fabric_Live_Testbed.closed_loop.segments import schedule_text  # noqa: E402

ENV_PATH = REPO_ROOT / "fabric-testbed/network/env-e1-caliper-benchmark.json"


def _write_workspace(stage_root: Path, sums: dict, env: dict, run_id: str, channel: str, rounds, tx_source: str) -> None:
    ws = stage_root / run_id / "workspace"
    bench, net = s1.benchmark_doc(env, run_id, channel, rounds)
    sums[f"{run_id}/workspace/benchmark.yaml"] = s1.write_x(ws / "benchmark.yaml", s1.json_bytes(bench))
    for name, doc in net.items():
        sums[f"{run_id}/workspace/{name}"] = s1.write_x(ws / name, s1.json_bytes(doc))
    for r in rounds:
        sums[f"{run_id}/workspace/schedule_{r.name}.txt"] = s1.write_x(ws / f"schedule_{r.name}.txt", schedule_text(r.offsets_ms))
    for name, rel in s1.MODULES.items():
        sums[f"{run_id}/workspace/modules/{name}"] = s1.write_x(ws / "modules" / name, (REPO_ROOT / rel).read_bytes())
    lines = ["run_id,round_index,round_label,seq,create_asset_key,source_day,source_row_index,window_offset_ms,round_offset_ms"]
    for r in rounds:
        for seq, off in enumerate(r.offsets_ms):
            lines.append(f"{run_id},{r.index},{r.label(run_id)},{seq},{run_id}_{r.index}_{seq:07d},{tx_source},-1,"
                         f"{r.window_start_ms + off},{off}")
    sums[f"{run_id}/workspace/tx_map.csv"] = s1.write_x(ws / "tx_map.csv", ("\n".join(lines) + "\n").encode("ascii"))


def _arm_doc(run_id: str, channel: str, pos: int, rep: int, arm: str, label: str, initial_b: int, rounds, window: dict,
             hard_timeout: int, decisions: bool) -> dict:
    t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    n_dec = (len(rounds) - 1) if decisions else 0
    return {"tool": PACKAGE_VERSION, "protocol": P.PROTOCOL_ID, "run_id": run_id, "channel": channel, "replicate": rep,
            "order_position": pos + 1, "arm": arm, "arm_label": label, "menu_B": list(P.MENU_B), "initial_B": initial_b,
            "batch_timeout": P.BATCH_TIMEOUT, "preferred_max_bytes": 10_485_760, "window": window,
            "rounds": [{"index": r.index, "name": r.name, "label": r.label(run_id), "measured": r.measured, "segment": r.segment,
                        "window_start_ms": r.window_start_ms, "length_ms": r.length_ms, "n_tx": len(r.offsets_ms)} for r in rounds],
            "decision_times_utc": [(t0 + dt.timedelta(seconds=SEGMENT_S * d)).isoformat() for d in range(n_dec)],
            "decision_rule": "decision d is taken when round d (d=0 pre-roll) has finished and applies to round d+1 (segment d)"
                             if decisions else "no decisions (static configuration, X0 calibration)",
            "caliper_argv": s1.caliper_argv(run_id), "caliper_hard_timeout_s": hard_timeout}


def _finish(stage_root: Path, sums: dict, manifest: dict) -> dict:
    sums["STAGE_MANIFEST.json"] = s1.write_x(stage_root / "STAGE_MANIFEST.json", s1.json_bytes(manifest))
    body = "".join(f"{sums[k]}  {k}\n" for k in sorted(sums)).encode("ascii")
    s1.write_x(stage_root / "SHA256SUMS.txt", body)
    manifest["stage_sha256"] = s1.sha256_bytes(body)
    return manifest


def x0_run_specs(seed: int = P.X0_ORDER_SEED) -> list[dict]:
    """Deterministic: 2 replicates x B in (10, 40), run order and each run's rung order from one seeded generator."""
    rng = random.Random(seed)
    cells = [(b, rep) for rep in range(1, P.X0_REPLICATES_PER_B + 1) for b in P.MENU_B]
    rng.shuffle(cells)
    specs = []
    for pos, (b, rep) in enumerate(cells):
        rungs = list(P.X0_RUNG_RATES_TPS)
        rng.shuffle(rungs)
        specs.append({"pos": pos, "B": b, "rep": rep, "rung_order": rungs})
    return specs


def stage_x0(stage_root: Path, *, first_seq: int = 1, protocol_sha256: str | None = None, source_freeze_sha256: str | None = None,
             env_path: Path = ENV_PATH) -> dict:
    if stage_root.exists():
        raise FileExistsError(f"refusing to reuse {stage_root}")
    env = lr.load_env(env_path)
    stage_root.mkdir(parents=True)
    sums: dict[str, str] = {}
    runs = []
    for spec in x0_run_specs():
        run_id = f"cx{first_seq + spec['pos']:02d}"
        channel = f"e1-{run_id}"
        rounds = S.rounds_from_rates(P.X0_PREROLL_RATE_TPS, [float(r) for r in spec["rung_order"]])
        _write_workspace(stage_root, sums, env, run_id, channel, rounds, "synthetic-constant-rate")
        arm = _arm_doc(run_id, channel, spec["pos"], spec["rep"], "static", f"static-B{spec['B']}-x0", spec["B"], rounds,
                       {"id": "X0", "kind": "constant-rate crossover scan", "rung_rates_tps": spec["rung_order"],
                        "preroll_rate_tps": P.X0_PREROLL_RATE_TPS, "segment_s": SEGMENT_S}, P.X0_HARD_TIMEOUT_S, decisions=False)
        arm["x0"] = {"B": spec["B"], "rung_order_tps": spec["rung_order"], "rate_of_round": {str(i + 1): float(r) for i, r in enumerate(spec["rung_order"])}}
        sums[f"{run_id}/arm.json"] = s1.write_x(stage_root / run_id / "arm.json", s1.json_bytes(arm))
        runs.append({"run_id": run_id, "channel": channel, "arm_label": arm["arm_label"], "B": spec["B"], "replicate": spec["rep"],
                     "rung_order_tps": spec["rung_order"], "n_tx": sum(len(r.offsets_ms) for r in rounds)})
    manifest = {"tool": PACKAGE_VERSION, "kind": "X0", "protocol": P.PROTOCOL_ID, "protocol_sha256": protocol_sha256,
                "source_freeze_sha256": source_freeze_sha256,
                "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "seed": P.X0_ORDER_SEED, "runs": runs,
                "rungs_tps": list(P.X0_RUNG_RATES_TPS)}
    return _finish(stage_root, sums, manifest)


def m_run_specs(seed: int = P.M_ORDER_SEED, replicates: int = P.M_REPLICATES_TARGET) -> list[dict]:
    """Block-randomized: replicate 1 of every arm (shuffled), then replicate 2, then replicate 3. Any prefix of 4k runs holds exactly k
    runs of every arm, so stopping early (2 replicates = the first 8 runs) never leaves the arms unbalanced."""
    rng = random.Random(seed)
    specs: list[dict] = []
    for rep in range(1, replicates + 1):
        arms = list(P.M_ARMS)
        rng.shuffle(arms)
        base = len(specs)
        specs += [{"pos": base + i, "arm": a, "rep": rep} for i, a in enumerate(arms)]
    return specs


def stage_m(stage_root: Path, *, window: dict, k: float, reactive: dict, surface_points: list[dict], x0_provenance: dict,
            replicates: int = P.M_REPLICATES_TARGET, first_seq: int = 1, protocol_sha256: str | None = None,
            source_freeze_sha256: str | None = None, env_path: Path = ENV_PATH) -> dict:
    """window: one entry of window_selection.rank_windows (frozen ranking). k, reactive and surface_points come from X0 ONLY."""
    if stage_root.exists():
        raise FileExistsError(f"refusing to reuse {stage_root}")
    if not (P.K_MIN <= k <= P.K_MAX):
        raise ValueError(f"k={k} is outside the pre-registered range [{P.K_MIN}, {P.K_MAX}]")
    env = lr.load_env(env_path)
    seg_rates = S.scaled_rates(window["segment_rates_tps"], k)
    pre_rate = k * window["preroll_rate_tps"]
    rounds = S.rounds_from_rates(pre_rate, seg_rates)
    warm = S.warmup_counts(S.scaled_rates(window["warmup_rates_tps"], k))
    hist = S.history_by_round(rounds)
    sched_n = {r.index: len(r.offsets_ms) for r in rounds}
    stage_root.mkdir(parents=True)
    sums: dict[str, str] = {}
    runs = []
    win = {"id": f"M_{window['start_s']}", "day": P.WINDOW["day"], "start_s": window["start_s"], "segments": len(seg_rates),
           "segment_s": SEGMENT_S, "k": k, "historical_segment_rates_tps": window["segment_rates_tps"],
           "scaled_segment_rates_tps": seg_rates, "preroll_rate_tps": pre_rate, "scaling": "piecewise-constant per-minute rate, evenly spaced"}
    for spec in m_run_specs(replicates=replicates):
        run_id = f"cm{first_seq + spec['pos']:02d}"
        channel = f"e1-{run_id}"
        _write_workspace(stage_root, sums, env, run_id, channel, rounds, f"scaled-{P.WINDOW['day']}-w{window['start_s']}-k{k}")
        a = spec["arm"]
        arm_name = {"static10": "static", "static40": "static", "reactive": "reactive", "predictive_live": "predictive_live"}[a]
        initial = 40 if a == "static40" else P.INITIAL_B
        doc = _arm_doc(run_id, channel, spec["pos"], spec["rep"], arm_name, a, initial, rounds, win, 1200, decisions=True)
        if a == "reactive":
            doc["reactive"] = {**reactive, "variant": "X0-calibrated", "registration_status": "LPC-v1 rule, thresholds fixed from X0 before M"}
        elif a == "predictive_live":
            doc["predictive_live"] = {"warmup_counts": warm, "round_counts": {str(k_): v for k_, v in hist.items()},
                                      "scheduled_n": {str(k_): v for k_, v in sched_n.items()}, "surface_points": surface_points,
                                      "forecast": P.FORECAST, "controller": P.PREDICTIVE, "x0": x0_provenance}
        sums[f"{run_id}/arm.json"] = s1.write_x(stage_root / run_id / "arm.json", s1.json_bytes(doc))
        runs.append({"run_id": run_id, "channel": channel, "arm_label": a, "replicate": spec["rep"], "order_position": spec["pos"] + 1})
    manifest = {"tool": PACKAGE_VERSION, "kind": "M", "protocol": P.PROTOCOL_ID, "protocol_sha256": protocol_sha256,
                "source_freeze_sha256": source_freeze_sha256,
                "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "seed": P.M_ORDER_SEED, "runs": runs,
                "window": win, "reactive": reactive, "x0_provenance": x0_provenance,
                "n_tx": {"pre_roll": sched_n[0], "measured": sum(v for i, v in sched_n.items() if i)}}
    return _finish(stage_root, sums, manifest)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Stage v2 X0 (calibration) runs offline; starts nothing.")
    ap.add_argument("--x0", action="store_true", required=True)
    ap.add_argument("--stage-root", required=True, type=Path)
    ap.add_argument("--first-seq", type=int, default=1)
    ap.add_argument("--protocol-sha256")
    ap.add_argument("--source-freeze-sha256")
    a = ap.parse_args(argv)
    m = stage_x0(a.stage_root, first_seq=a.first_seq, protocol_sha256=a.protocol_sha256, source_freeze_sha256=a.source_freeze_sha256)
    print(json.dumps({"stage_sha256": m["stage_sha256"], "runs": m["runs"]}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
