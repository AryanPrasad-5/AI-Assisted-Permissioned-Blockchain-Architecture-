"""X0 -> decision -> M stage (Windows). The pre-registered chain as ONE deterministic function of the X0 measurements:

    X0 round rows -> rung table -> crossover analysis (protocol.analyse_crossover)
      CROSSOVER                                   -> pick the first feasible ranked window and k (protocol.select_k_over_ranked_windows),
                                                     thresholds for the reactive arm (protocol.reactive_params), surface for the predictor
      NO_CROSSOVER_WITHIN_CLIENT_CAPACITY / NOT_ESTABLISHED / NO_FEASIBLE_WINDOW -> STOP_NEGATIVE_RESULT: no M stage is built
Nothing else is an input: not the M series, not any controller output. Outputs go to a NEW directory (never overwritten).

  python -m Fabric_Live_Testbed.closed_loop_v2.x0_decide --x0-stage <stage_x0> --evidence <evidence root with cxNN/cxNN/analysis.json> \
      --out <new dir> [--build-m-stage <new stage dir> --source-freeze-sha256 <hex>]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from . import analysis_v2 as A
from . import protocol as P


def load_x0_rows(stage_x0: Path, evidence: Path) -> list[dict]:
    rows = []
    for arm_path in sorted(Path(stage_x0).glob("cx[0-9][0-9]/arm.json")):
        arm = json.loads(arm_path.read_text(encoding="utf-8"))
        rid = arm["run_id"]
        ap = Path(evidence) / rid / rid / "analysis.json"
        if not ap.exists():
            raise FileNotFoundError(f"X0 run {rid} has no analysis.json at {ap}")
        rows += A.x0_round_rows(arm, json.loads(ap.read_text(encoding="utf-8")))
    return rows


def decide(rows: list[dict], ranked_windows: list[dict]) -> dict:
    rep = A.crossover_report(rows)
    res = rep["crossover"]
    out = {"protocol": P.PROTOCOL_ID, "x0_rows": len(rows), "crossover": res, "rung_table": rep["rung_table"],
           "surface_points": rep["surface_points"]}
    if res["outcome"] != "CROSSOVER":
        out.update({"status": "STOP_NEGATIVE_RESULT", "reason": res["outcome"],
                    "meaning": "no decision headroom was established between B=10 and B=40 within the client-valid range "
                               f"(highest valid rung r_max = {res['r_max']} tx/s): the adaptive comparison is not run"})
        return out
    sel = P.select_k_over_ranked_windows(ranked_windows, res["r_c"], res["r_max"])
    out["window_selection"] = sel
    if sel["status"] != "WINDOW_AND_K_SELECTED":
        out.update({"status": "STOP_NEGATIVE_RESULT", "reason": "NO_FEASIBLE_WINDOW", "meaning": sel["meaning"]})
        return out
    m10 = next(r["mean_ms"] for r in rep["rung_table"] if r["B"] == 10 and r["rate_tps"] == res["r_c"])
    out.update({"status": "GO_M", "window": sel["window"], "k": sel["k"], "reactive": P.reactive_params(m10)})
    return out


def write_outputs(decision: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "x0_decision.json").write_text(json.dumps(decision, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    for name, rows in (("rung_table.csv", decision["rung_table"]), ("surface_points.csv", decision["surface_points"])):
        if rows:
            keys = list(dict.fromkeys(k for r in rows for k in r))
            with (out_dir / name).open("x", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=keys, lineterminator="\n")
                w.writeheader()
                w.writerows({k: (json.dumps(v) if isinstance(v, (list, dict)) else v) for k, v in r.items()} for r in rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--x0-stage", required=True, type=Path)
    ap.add_argument("--evidence", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--build-m-stage", type=Path)
    ap.add_argument("--source-freeze-sha256")
    ap.add_argument("--protocol-sha256")
    a = ap.parse_args(argv)
    from .window_selection import load_ranking
    ranked, src = load_ranking()
    decision = decide(load_x0_rows(a.x0_stage, a.evidence), ranked)
    decision["window_ranking_source"] = src
    x0_sums = (a.x0_stage / "SHA256SUMS.txt")
    import hashlib
    decision["x0_stage_sha256"] = hashlib.sha256(x0_sums.read_bytes()).hexdigest()
    write_outputs(decision, a.out)
    print(json.dumps({k: decision.get(k) for k in ("status", "reason", "k")} | {"r_c": decision["crossover"]["r_c"], "r_max": decision["crossover"]["r_max"],
                                                                           "outcome": decision["crossover"]["outcome"]}, indent=1))
    if decision["status"] == "GO_M" and a.build_m_stage:
        from . import stage_v2 as ST
        m = ST.stage_m(a.build_m_stage, window=decision["window"], k=decision["k"], reactive=decision["reactive"],
                       surface_points=decision["surface_points"], x0_provenance={"x0_stage_sha256": decision["x0_stage_sha256"],
                                                                                 "decision_file": str(a.out / "x0_decision.json")},
                       protocol_sha256=a.protocol_sha256, source_freeze_sha256=a.source_freeze_sha256)
        print("M stage_sha256", m["stage_sha256"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
