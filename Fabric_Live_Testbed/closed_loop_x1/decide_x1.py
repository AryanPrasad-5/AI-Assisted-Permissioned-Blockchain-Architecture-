"""X1 evidence -> decision (Windows, offline, read-only on the evidence). The pre-registered chain as ONE deterministic function:

    X1 round rows -> rung table -> rung verdicts -> regimes / ordering reversal -> structurally selected window -> headroom gate
      GO_LIVE_DESIGN         a separately authorized live Static vs Reactive vs Predictive comparison may now be DESIGNED (nothing is run)
      STOP_NO_HEADROOM       stop experimenting; finalize the paper with the completed evidence
      STOP_NOT_ESTABLISHED   X1 could not establish any client-valid rung

  python -m Fabric_Live_Testbed.closed_loop_x1.decide_x1 --stage <stage_x1> --evidence <root with cbNN/cbNN/analysis.json> \
      --lpc-protocol <PROTOCOL_LPC_v1.json> --out <new dir> [--day-arrivals <arrivals_2026-10-12_ms.npy>]
Nothing else is an input. Outputs go to a NEW directory (never overwritten).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

from . import ensure_paths
from . import protocol_x1 as X

ensure_paths()

from Fabric_Live_Testbed.closed_loop_v2 import analysis_v2 as A2  # noqa: E402


def load_rows(stage: Path, evidence: Path) -> list[dict]:
    rows = []
    for arm_path in sorted(Path(stage).glob(f"{X.RUN_PREFIX}[0-9][0-9]/arm.json")):
        arm = json.loads(arm_path.read_text(encoding="utf-8"))
        rid = arm["run_id"]
        ap = Path(evidence) / rid / rid / "analysis.json"
        if not ap.exists():
            continue                              # a run that never produced an analysis contributes no rows (its absence is reported)
        rows += A2.x0_round_rows(arm, json.loads(ap.read_text(encoding="utf-8")))
    return rows


def load_ranking(lpc_protocol: Path) -> tuple[list[dict], str]:
    raw = Path(lpc_protocol).read_bytes()
    sha = hashlib.sha256(raw).hexdigest()
    if sha != X.LPC_PROTOCOL_SHA256:
        raise ValueError(f"LPC-v1 protocol file sha256 {sha} != pinned {X.LPC_PROTOCOL_SHA256}")
    return json.loads(raw)["window_ranking"], sha


def day_minute_rates(path: Path) -> list[float]:
    """Natural per-minute rates of the whole 2026-10-12 day (descriptive workload only; numpy)."""
    import numpy as np
    from Fabric_Live_Testbed.closed_loop_v2.window_selection import minute_rates, per_second_counts
    return [float(v) for v in minute_rates(per_second_counts(np.load(path)))]


def write_outputs(decision: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "x1_decision.json").write_text(json.dumps(decision, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    rows = decision["rung_table"]
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with (out_dir / "rung_table.csv").open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, lineterminator="\n")
        w.writeheader()
        w.writerows({k: (json.dumps(v) if isinstance(v, (list, dict)) else v) for k, v in r.items()} for r in rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, type=Path)
    ap.add_argument("--evidence", required=True, type=Path)
    ap.add_argument("--lpc-protocol", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--day-arrivals", type=Path)
    a = ap.parse_args(argv)
    ranked, lpc_sha = load_ranking(a.lpc_protocol)
    extra = {"whole_day_2026-10-12_k1": day_minute_rates(a.day_arrivals)} if a.day_arrivals else None
    rows = load_rows(a.stage, a.evidence)
    decision = X.decide(rows, ranked, extra)
    decision["inputs"] = {"stage_sha256": hashlib.sha256((a.stage / "SHA256SUMS.txt").read_bytes()).hexdigest(),
                          "lpc_protocol_sha256": lpc_sha, "runs_with_rows": sorted({r["run_id"] for r in rows})}
    write_outputs(decision, a.out)
    print(json.dumps({k: decision.get(k) for k in ("status", "reason", "meaning", "menu")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
