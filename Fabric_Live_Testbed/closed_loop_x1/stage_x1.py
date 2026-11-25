"""Stage the X1 runs OFFLINE (Windows venv). Writes files only; starts nothing.

X1 stage: 9 static runs cb01..cb09 = 3 configurations (B in {2, 5, 10}) x 3 replicates. Each run is one fresh channel at a fixed B, a 20 tx/s
unmeasured pre-roll round and ten constant-rate rungs in a seeded random order (evenly spaced arrivals, 60 s per rung), no decisions and no
configuration change (max_broadcasts_per_run = 0). It reuses the v2 workspace writer, arm schema and hashing unchanged: arm.json carries the
fields the UNCHANGED closed_loop runner/analyzer read, plus the "x0" block (B, rate_of_round) that closed_loop_v2.analysis_v2.x0_round_rows reads.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import sys
from pathlib import Path

from . import PACKAGE_VERSION, SEGMENT_S, ensure_paths
from . import protocol_x1 as X

ensure_paths()

from Fabric_Live_Testbed.closed_loop_v2 import scaling as S  # noqa: E402
from Fabric_Live_Testbed.closed_loop_v2 import stage_v2 as V2  # noqa: E402

s1 = V2.s1


def run_specs(seed: int = X.ORDER_SEED) -> list[dict]:
    """Deterministic. Block-randomized by replicate (every configuration once per block, so any prefix of 3k runs holds k replicates of
    each), no configuration run twice in a row across blocks, and each run's rung order drawn from the same seeded generator."""
    rng = random.Random(seed)
    specs: list[dict] = []
    last = None
    for rep in range(1, X.REPLICATES_PER_B + 1):
        while True:
            order = list(X.CONFIGS)
            rng.shuffle(order)
            if order[0] != last:
                break
        last = order[-1]
        for b in order:
            rungs = list(X.RUNG_RATES_TPS)
            rng.shuffle(rungs)
            specs.append({"pos": len(specs), "B": b, "rep": rep, "rung_order": rungs})
    return specs


def stage_x1(stage_root: Path, *, first_seq: int = 1, protocol_sha256: str | None = None, source_freeze_sha256: str | None = None,
             env_path: Path = V2.ENV_PATH) -> dict:
    if stage_root.exists():
        raise FileExistsError(f"refusing to reuse {stage_root}")
    env = V2.lr.load_env(env_path)
    stage_root.mkdir(parents=True)
    sums: dict[str, str] = {}
    runs = []
    for spec in run_specs():
        run_id = f"{X.RUN_PREFIX}{first_seq + spec['pos']:02d}"
        channel = f"e1-{run_id}"
        rounds = S.rounds_from_rates(X.PREROLL_RATE_TPS, [float(r) for r in spec["rung_order"]])
        V2._write_workspace(stage_root, sums, env, run_id, channel, rounds, "synthetic-constant-rate")
        arm = V2._arm_doc(run_id, channel, spec["pos"], spec["rep"], "static", f"static-B{spec['B']}-x1", spec["B"], rounds,
                          {"id": "X1", "kind": "constant-rate block-size scan", "rung_rates_tps": spec["rung_order"],
                           "preroll_rate_tps": X.PREROLL_RATE_TPS, "segment_s": SEGMENT_S}, X.HARD_TIMEOUT_S, decisions=False)
        arm["tool"], arm["protocol"], arm["menu_B"], arm["batch_timeout"] = PACKAGE_VERSION, X.PROTOCOL_ID, list(X.CONFIGS), X.BATCH_TIMEOUT
        arm["x0"] = {"B": spec["B"], "rung_order_tps": spec["rung_order"],
                     "rate_of_round": {str(i + 1): float(r) for i, r in enumerate(spec["rung_order"])}}
        arm["x1"] = {"experiment": "X1 (exploratory follow-up to X0)", "B": spec["B"], "replicate": spec["rep"]}
        sums[f"{run_id}/arm.json"] = s1.write_x(stage_root / run_id / "arm.json", s1.json_bytes(arm))
        runs.append({"run_id": run_id, "channel": channel, "arm_label": arm["arm_label"], "B": spec["B"], "replicate": spec["rep"],
                     "rung_order_tps": spec["rung_order"], "n_tx": sum(len(r.offsets_ms) for r in rounds)})
    manifest = {"tool": PACKAGE_VERSION, "kind": "X1", "protocol": X.PROTOCOL_ID, "protocol_sha256": protocol_sha256,
                "source_freeze_sha256": source_freeze_sha256, "lpc_protocol_sha256": X.LPC_PROTOCOL_SHA256,
                "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "seed": X.ORDER_SEED, "runs": runs,
                "configs": list(X.CONFIGS), "rungs_tps": list(X.RUNG_RATES_TPS)}
    return V2._finish(stage_root, sums, manifest)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Stage the X1 runs offline; starts nothing.")
    ap.add_argument("--stage-root", required=True, type=Path)
    ap.add_argument("--first-seq", type=int, default=1)
    ap.add_argument("--protocol-sha256")
    ap.add_argument("--source-freeze-sha256")
    a = ap.parse_args(argv)
    m = stage_x1(a.stage_root, first_seq=a.first_seq, protocol_sha256=a.protocol_sha256, source_freeze_sha256=a.source_freeze_sha256)
    print(json.dumps({"stage_sha256": m["stage_sha256"], "runs": m["runs"]}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
