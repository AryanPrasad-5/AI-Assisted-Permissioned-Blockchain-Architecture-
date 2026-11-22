"""A0w post-run checks on a finished clock round (read-only on the round directory; writes only a NEW output directory).

1. Re-evaluates the stored raw samples with the unchanged A0e evaluator (``clock_only_readiness.assess_round_directory``) and compares the verdict with the stored one.
2. Recomputes each sample's offset bracket with plain integer arithmetic that does not use the A0e module (remote - wall_after .. remote - wall_before) and intersects them.
3. Re-hashes every file listed in the round's integrity manifest.
4. Runs the A0e evaluator over a DIAGNOSTIC list of tolerances to show how the verdict depends on the tolerance. The list is a characterization aid; it selects nothing and approves nothing.

    python scripts/e1/e1_a0w_post_run_check.py --round-dir <clock_round_01> --out-dir <new empty-or-missing directory>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1].parent
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import clock_only_readiness as cor      # noqa: E402
from fabric_perf.controllers import clock_preflight as cp            # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi          # noqa: E402
from fabric_perf.controllers import wsl_clock_probe as wp            # noqa: E402

SWEEP_MS = (10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--round-dir", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args(argv)
    rd = args.round_dir
    records = [json.loads((rd / "clock" / f"{p}.json").read_text(encoding="utf-8")) for p in ("pre_run", "post_run")]
    verdict_doc = json.loads((rd / "clock" / "clock_verdict.json").read_text(encoding="utf-8"))
    diag = verdict_doc["parameters"]
    plan = {"request": {k: diag[k] for k in ("tolerance_ms", "min_valid_samples", "max_wall_monotonic_disagreement_ms")}}
    reeval = cor.assess_round_directory(rd, plan)

    independent: dict = {"method": "bracket = [remote_wall - ref_wall_after, remote_wall - ref_wall_before] in integer ns; no midpoint assumption; resolution ignored (<= 1 us)", "phases": {}}
    all_lo, all_hi = [], []
    for rec in records:
        lows = [s["remote_wall_ns"] - s["ref_wall_after_ns"] for s in rec["samples"]]
        highs = [s["remote_wall_ns"] - s["ref_wall_before_ns"] for s in rec["samples"]]
        lo, hi = max(lows), min(highs)
        all_lo += lows
        all_hi += highs
        independent["phases"][rec["phase"]] = {"intersection_ms": {"low": lo / 1e6, "high": hi / 1e6, "width": (hi - lo) / 1e6} if lo <= hi else None, "n": len(lows)}
    lo, hi = max(all_lo), min(all_hi)
    independent["both_phases_intersection_ms"] = {"low": lo / 1e6, "high": hi / 1e6, "width": (hi - lo) / 1e6} if lo <= hi else None
    a0e_intervals = {r["phase"]: verdict_doc["phases"][r["phase"]].get("interval_ms") for r in records}
    independent["a0e_intervals_ms"] = a0e_intervals
    independent["agrees_with_a0e_within_1us"] = all(
        a0e_intervals[p] and independent["phases"][p]["intersection_ms"] and abs(a0e_intervals[p]["low"] - independent["phases"][p]["intersection_ms"]["low"]) < 0.002
        and abs(a0e_intervals[p]["high"] - independent["phases"][p]["intersection_ms"]["high"]) < 0.002 for p in a0e_intervals)

    manifest = json.loads((rd / "integrity_manifest.json").read_text(encoding="utf-8"))["files"]
    bad = sorted(rel for rel, digest in manifest.items() if pi.sha256_file(rd / rel) != digest)
    on_disk = {p.relative_to(rd).as_posix() for p in rd.rglob("*") if p.is_file()} - {"integrity_manifest.json"}
    integrity = {"files_listed": len(manifest), "files_on_disk": len(on_disk), "hash_mismatches": bad, "unlisted_files": sorted(on_disk - set(manifest))}

    sweep = []
    for tol in SWEEP_MS:
        result = wp.evaluate(records, tolerance_ms=tol, min_valid_samples=diag["min_valid_samples"], max_wall_monotonic_disagreement_ms=diag["max_wall_monotonic_disagreement_ms"])
        sweep.append({"tolerance_ms": tol, "verdict": result["verdict"], "phase_reasons": {p: r["reason"] for p, r in result["phases"].items()}})
    sensitivity = {"label": "DIAGNOSTIC ONLY: a characterization of how the A0e verdict depends on the tolerance. No tolerance is selected, accepted or recommended by this table.", "evaluator": cp.CLOCK_PREFLIGHT_VERSION, "rows": sweep}

    args.out_dir.mkdir(parents=True, exist_ok=False)
    for name, doc in (("reevaluation_a0e.json", reeval), ("independent_bracket_check.json", independent), ("integrity_recheck.json", integrity), ("tolerance_sensitivity_diagnostic.json", sensitivity)):
        (args.out_dir / name).write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"reeval_verdicts_equal": reeval["verdicts_equal"], "sample_order_valid": reeval["sample_order_valid"], "independent_agrees_with_a0e": independent["agrees_with_a0e_within_1us"],
                      "both_phases_intersection_ms": independent["both_phases_intersection_ms"], "integrity_mismatches": len(bad), "unlisted": len(integrity["unlisted_files"]),
                      "sensitivity": [(r["tolerance_ms"], r["verdict"]) for r in sweep]}, indent=2))
    return 0 if reeval["verdicts_equal"] and not bad and independent["agrees_with_a0e_within_1us"] else 1


if __name__ == "__main__":
    sys.exit(main())
