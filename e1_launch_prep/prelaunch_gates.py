"""Read-only evaluation of the registered pre-flight gates that need no launch artifact, with the SAME functions and thresholds the launch uses (frozen P1b gates, p4_replay gate R4).
Run it inside WSL with the interpreter the launch will use:

    PYTHONPATH=/home/mailf18/p4_replay_pylib python3 -B -m e1_launch_prep.prelaunch_gates --launch-id p4r-dry-01 --out <new json>

It runs gates S3 (60 s idle memory window, 12 samples), S4 (disk), S5 (containers and health), S6 (Raft and chaincode state), S8 (no name collision), S11 (output roots absent) and R4
(no conflicting process). It does NOT run S1 (hours of hashing; run separately), S2 (needs the keep-awake helper), S7 (writes a fingerprint), S9 (needs a 30-minute probe), R1-R3, R5, R6,
S10 (need the owner's artifacts). It writes nothing but the JSON report (create-exclusive), starts no process but the read-only queries the gates themselves make, and never submits a
transaction. A PASS here is a reading at one moment, not a clean pre-flight: the launch repeats every gate.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from p4_replay import driver as dr          # noqa: E402
from p4_replay import gate_r4 as g4         # noqa: E402
from p4_replay import launch_ids as li      # noqa: E402
from p4_replay import live_entry as le      # noqa: E402


def run(launch_id: str) -> dict:
    p1b = dr.p1b_module()
    be = p1b.Backend()
    ctx = le._live_ctx(launch_id, r"D:\PLAN_BLOCK18\evidence_archive\ARCHIVE_20261114T113430Z")       # exactly the context the live entry builds
    thr = p1b.THR
    rows = li.dry_rows()
    net = p1b.load_network(ctx)
    results: list[dict] = []

    def attempt(gid: str, name: str, fn) -> None:
        try:
            res = fn()
            results.append({**res.as_dict(), "evaluated_by": "frozen/registered gate function"})
        except Exception as exc:                                                        # noqa: BLE001 - a gate that cannot run has failed (as in the launch)
            results.append({"id": gid, "name": name, "status": "FAIL", "rule": "gate raised", "detail": repr(exc), "evaluated_by": "gate raised"})
        print(f"{results[-1]['id']:<4}{results[-1]['status']:<5} {results[-1]['name']}: {str(results[-1]['detail'])[:220]}", flush=True)

    attempt("S3", "host memory headroom", lambda: p1b.eval_memory(be.host_memory(thr.mem_window_samples, thr.mem_window_interval_s), be.wsl_mem_available_mb(), thr))
    attempt("S4", "disk", lambda: p1b.eval_disk(be.disk_free_gb("/mnt/c"), thr))
    attempt("S5", "containers and health", lambda: p1b.gate_s5(ctx, be)[0])
    attempt("S6", "Raft and chaincode state", lambda: p1b.gate_s6(ctx, be, net))
    attempt("S8", "no name collision", lambda: p1b.gate_s8(ctx, be, net, list(rows)))
    attempt("S11", "output roots absent", lambda: p1b.gate_s11(ctx, list(rows)))
    attempt("R4", "no conflicting experiment process", lambda: g4.gate_r4(be, os.getpid()))
    return {"launch_id": launch_id, "created_utc": be.now_utc(), "boot_id": be.boot_id(), "python": sys.version.split()[0], "gates": results,
            "all_pass": all(r["status"] == "PASS" for r in results), "evaluated": [r["id"] for r in results],
            "not_evaluated": ["S1", "S2", "S7", "S9", "R1", "R2", "R3", "R5", "R6", "S10"]}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--launch-id", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)
    li.check_launch_id(args.launch_id, li.DRY)
    report = run(args.launch_id)
    with open(args.out, "x", encoding="utf-8", newline="\n") as fh:                      # create-exclusive
        json.dump(report, fh, indent=1)
    print(f"report: {args.out}; all evaluated gates pass: {report['all_pass']}")
    return 0 if report["all_pass"] else 3


if __name__ == "__main__":
    sys.exit(main())
