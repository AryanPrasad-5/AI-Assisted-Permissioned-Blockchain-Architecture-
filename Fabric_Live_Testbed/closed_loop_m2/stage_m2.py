"""Stage the M2 runs OFFLINE (Windows venv; numpy only for the window ranking). Writes files only; starts nothing.

  comparison stage  cc01..cc12: 4 arms (static_b2, reactive, predictive_live, static_b10) x 3 replicates, block-randomized, every arm sees the
                    identical scaled-window schedule on its own fresh channel e1-ccNN. Primary arms start at B=2; static_b10 starts at B=10.
  actuation stage   ca01: one scripted ACTUATION_CHECK run (2->5->10->5->2) that proves the live switch path for the new targets 2 and 5.
                    Not a strategy; excluded from every comparison.

arm.json keeps the schema the UNCHANGED closed_loop runner/analyzer read, plus an "m2" block. <stage>/SHA256SUMS.txt covers every file; the
owner's authorization pins its SHA-256.

  python -m Fabric_Live_Testbed.closed_loop_m2.stage_m2 --kind m2  --x1-decision <json> --stage-root <new dir> [--protocol-sha256 .. --source-freeze-sha256 ..]
  python -m Fabric_Live_Testbed.closed_loop_m2.stage_m2 --kind act --stage-root <new dir> [...]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import sys
from pathlib import Path

from . import PACKAGE_VERSION, REPO_ROOT, SEGMENT_S, ensure_paths
from . import protocol_m2 as P

ensure_paths()

from Fabric_Live_Testbed.closed_loop_v2 import scaling as S  # noqa: E402
from Fabric_Live_Testbed.closed_loop_v2 import stage_v2 as V2  # noqa: E402

s1 = V2.s1


def run_specs(arms=P.ARMS, replicates: int = P.REPLICATES_TARGET, seed: int = P.ORDER_SEED) -> list[dict]:
    """Deterministic. Replicate 1 of every arm (shuffled), then replicate 2, then replicate 3, so any prefix of len(arms)*k runs holds exactly k
    runs of every arm. The same arm is never run twice in a row across a block boundary."""
    rng = random.Random(seed)
    specs: list[dict] = []
    last = None
    for rep in range(1, replicates + 1):
        while True:
            order = list(arms)
            rng.shuffle(order)
            if len(order) < 2 or order[0] != last:
                break
        last = order[-1]
        for a in order:
            specs.append({"pos": len(specs), "arm": a, "rep": rep})
    return specs


def _finish_arm(doc: dict, label: str) -> dict:
    doc["tool"], doc["protocol"] = PACKAGE_VERSION, P.PROTOCOL_ID
    doc["menu_B"], doc["batch_timeout"] = list(P.MENU_B), P.BATCH_TIMEOUT
    return doc


def stage_m2(stage_root: Path, *, window: dict, k: float, reactive: dict, surface_points: list[dict], provenance: dict,
             arms=P.ARMS, replicates: int = P.REPLICATES_TARGET, first_seq: int = 1, protocol_sha256: str | None = None,
             source_freeze_sha256: str | None = None, env_path: Path = V2.ENV_PATH) -> dict:
    """window = one entry of window_selection.rank_windows; k, reactive and surface_points from design_from_x1 (X1 only)."""
    if stage_root.exists():
        raise FileExistsError(f"refusing to reuse {stage_root}")
    unknown = [a for a in arms if a not in P.ARMS]
    if unknown or not set(P.PRIMARY_ARMS) <= set(arms):
        raise ValueError(f"arms must contain the three primary arms and only known arms (got {list(arms)})")
    if not (P.K_MIN <= k <= P.K_MAX):
        raise ValueError(f"k={k} is outside the pre-registered range [{P.K_MIN}, {P.K_MAX}]")
    env = V2.lr.load_env(env_path)
    seg_rates = S.scaled_rates(window["segment_rates_tps"], k)
    pre_rate = k * window["preroll_rate_tps"]
    rounds = S.rounds_from_rates(pre_rate, seg_rates)
    warm = S.warmup_counts(S.scaled_rates(window["warmup_rates_tps"], k))
    hist = S.history_by_round(rounds)
    sched_n = {r.index: len(r.offsets_ms) for r in rounds}
    stage_root.mkdir(parents=True)
    sums: dict[str, str] = {}
    runs = []
    win = {"id": f"M2_{window['start_s']}", "day": P.WINDOW["day"], "start_s": window["start_s"], "segments": len(seg_rates), "segment_s": SEGMENT_S,
           "k": k, "historical_segment_rates_tps": window["segment_rates_tps"], "scaled_segment_rates_tps": seg_rates,
           "preroll_rate_tps": pre_rate, "scaling": "piecewise-constant per-minute rate, evenly spaced (closed_loop_v2.scaling)"}
    for spec in run_specs(arms, replicates):
        run_id = f"{P.RUN_PREFIX}{first_seq + spec['pos']:02d}"
        channel = f"e1-{run_id}"
        V2._write_workspace(stage_root, sums, env, run_id, channel, rounds, f"scaled-{P.WINDOW['day']}-w{window['start_s']}-k{k}")
        a = spec["arm"]
        doc = V2._arm_doc(run_id, channel, spec["pos"], spec["rep"], P.ARM_RUNNER_NAME[a], a, P.ARM_INITIAL_B[a], rounds, win,
                          P.HARD_TIMEOUT_S, decisions=True)
        _finish_arm(doc, a)
        doc["m2"] = {"arm": a, "primary": a in P.PRIMARY_ARMS, "replicate": spec["rep"]}
        if a == "reactive":
            doc["reactive"] = {**reactive, "variant": "X1-calibrated", "registration_status": "M2-v1 rule, thresholds fixed from X1 before M2"}
        elif a == "predictive_live":
            doc["predictive_live"] = {"warmup_counts": warm, "round_counts": {str(k_): v for k_, v in hist.items()},
                                      "scheduled_n": {str(k_): v for k_, v in sched_n.items()}, "surface_points": surface_points,
                                      "forecast": P.FORECAST, "controller": P.PREDICTIVE, "surface_provenance": provenance}
        sums[f"{run_id}/arm.json"] = s1.write_x(stage_root / run_id / "arm.json", s1.json_bytes(doc))
        runs.append({"run_id": run_id, "channel": channel, "arm_label": a, "replicate": spec["rep"], "order_position": spec["pos"] + 1,
                     "initial_B": P.ARM_INITIAL_B[a]})
    manifest = {"tool": PACKAGE_VERSION, "kind": "M2", "protocol": P.PROTOCOL_ID, "protocol_sha256": protocol_sha256,
                "source_freeze_sha256": source_freeze_sha256,
                "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "seed": P.ORDER_SEED, "runs": runs,
                "arms": list(arms), "replicates": replicates, "window": win, "reactive": reactive, "provenance": provenance,
                "n_tx": {"pre_roll": sched_n[0], "measured": sum(v for i, v in sched_n.items() if i)}}
    return V2._finish(stage_root, sums, manifest)


def stage_act(stage_root: Path, *, run_id: str = P.ACT_RUN_ID, protocol_sha256: str | None = None, source_freeze_sha256: str | None = None,
              env_path: Path = V2.ENV_PATH) -> dict:
    """One scripted ACTUATION_CHECK run: constant 10 tx/s, 6 segments, schedule 2->5->10->5->2 (+2 holds)."""
    if stage_root.exists():
        raise FileExistsError(f"refusing to reuse {stage_root}")
    env = V2.lr.load_env(env_path)
    n = len(P.ACT_SCHEDULE_B)
    rounds = S.rounds_from_rates(P.ACT_RATE_TPS, [P.ACT_RATE_TPS] * n)
    stage_root.mkdir(parents=True)
    sums: dict[str, str] = {}
    channel = f"e1-{run_id}"
    V2._write_workspace(stage_root, sums, env, run_id, channel, rounds, "synthetic-constant-rate")
    win = {"id": "ACT", "kind": "actuation check at constant rate", "segments": n, "segment_s": SEGMENT_S, "rate_tps": P.ACT_RATE_TPS}
    doc = V2._arm_doc(run_id, channel, 0, 1, "scripted", "scripted-ACTUATION_CHECK", 2, rounds, win, P.HARD_TIMEOUT_S, decisions=True)
    _finish_arm(doc, "scripted")
    doc["scripted"] = {"schedule_B": list(P.ACT_SCHEDULE_B), "purpose": "ACTUATION_CHECK: proves the live switch path for the new targets 2 and 5; "
                                                                        "not a strategy, excluded from every comparison"}
    doc["m2"] = {"arm": "actuation_check", "primary": False}
    sums[f"{run_id}/arm.json"] = s1.write_x(stage_root / run_id / "arm.json", s1.json_bytes(doc))
    manifest = {"tool": PACKAGE_VERSION, "kind": "ACT", "protocol": P.PROTOCOL_ID, "protocol_sha256": protocol_sha256,
                "source_freeze_sha256": source_freeze_sha256, "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                "runs": [{"run_id": run_id, "channel": channel, "arm_label": "scripted-ACTUATION_CHECK", "initial_B": 2}],
                "schedule_B": list(P.ACT_SCHEDULE_B), "n_tx": sum(len(r.offsets_ms) for r in rounds)}
    return V2._finish(stage_root, sums, manifest)


def load_design(x1_decision_path: Path) -> tuple[dict, dict]:
    """Design from the X1 primary decision file and the frozen LPC-v1 ranking (numpy)."""
    import hashlib
    from Fabric_Live_Testbed.closed_loop_v2.window_selection import load_ranking
    raw = Path(x1_decision_path).read_bytes()
    x1 = json.loads(raw)
    ranked, rank_src = load_ranking()
    design = P.design_from_x1(x1, ranked)
    prov = {"x1_decision_path": str(x1_decision_path), "x1_decision_sha256": hashlib.sha256(raw).hexdigest(), "x1_status": x1.get("status"),
            "r_max_tps": design["r_max_tps"], "ranking": rank_src, "window_start_s": design["window"]["start_s"], "k": design["k"],
            "surface": "X1 primary rung table, valid and unsaturated cells, log-rate linear interpolation, no extrapolation"}
    return design, prov


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Stage M2 runs offline; starts nothing.")
    ap.add_argument("--kind", choices=("m2", "act"), required=True)
    ap.add_argument("--stage-root", required=True, type=Path)
    ap.add_argument("--x1-decision", type=Path)
    ap.add_argument("--arms", default=",".join(P.ARMS))
    ap.add_argument("--replicates", type=int, default=P.REPLICATES_TARGET)
    ap.add_argument("--first-seq", type=int, default=1)
    ap.add_argument("--protocol-sha256")
    ap.add_argument("--source-freeze-sha256")
    a = ap.parse_args(argv)
    if a.kind == "act":
        m = stage_act(a.stage_root, protocol_sha256=a.protocol_sha256, source_freeze_sha256=a.source_freeze_sha256)
        print(json.dumps({"stage_sha256": m["stage_sha256"], "runs": m["runs"], "n_tx": m["n_tx"]}, indent=1))
        return 0
    if a.x1_decision is None:
        ap.error("--x1-decision is required for --kind m2")
    design, prov = load_design(a.x1_decision)
    m = stage_m2(a.stage_root, window=design["window"], k=design["k"], reactive=design["reactive"], surface_points=design["surface_points"],
                 provenance=prov, arms=tuple(a.arms.split(",")), replicates=a.replicates, first_seq=a.first_seq,
                 protocol_sha256=a.protocol_sha256, source_freeze_sha256=a.source_freeze_sha256)
    print(json.dumps({"stage_sha256": m["stage_sha256"], "runs": m["runs"], "n_tx": m["n_tx"], "window": m["window"]["id"], "k": m["window"]["k"],
                      "scaled_rates": m["window"]["scaled_segment_rates_tps"], "reactive": design["reactive"]}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
