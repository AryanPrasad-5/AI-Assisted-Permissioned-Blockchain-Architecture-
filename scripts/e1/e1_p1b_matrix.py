#!/usr/bin/env python3
"""Generator for the P1b capacity-characterization run matrix (plan dated 2026-11-13). PROPOSED, not authorised to execute.

Deterministic: the same constants give the same bytes. The matrix is a new dated version; the frozen P1a matrix (28 rows) is untouched.

Scope (owner decision of 2026-11-13: "TIER 1 FIRST"): by default this generator emits TIER 1 ONLY, the ten rungs the frozen P1a ramps skipped
(15, 20, 25, 30, 40 tx/s for B = 10 and B = 40; ql18-ql22 and ql24-ql28 in the old numbering). Tier 2 (60-200 tx/s) is a design that stays unauthorised: it needs
the Tier 1 results and stronger overload monitoring first. It can still be generated with --max-rate-tps 200, only so that the 2026-11-13 plan's 26-row
matrix (SHA-256 e73f0a36...) remains reproducible; the Tier 1 matrix is an exact prefix of it (same rows, same seeded coin).

Design (one estimand: where does each setting stop being healthy as the offered rate climbs?)
  * Two ascending ramps, B = 10 and B = 40, one replicate per rung, everything else as in P1a (tau 2 s, P 10 MiB, 30 s warm-up, 120 s measured).
  * Rung-major order: at every rate the two settings are adjacent in time (B = 10 then 40, or 40 then 10, by a seeded coin that is frozen with
    the matrix), so slow drift (channels accumulate on the orderers) cannot favour one setting.
  * Each ramp continues from the P1a rung-1 run of the same setting (ql23 for B = 10, ql17 for B = 40) through the erratum's derived table:
    the first rung runs only if that run is HEALTHY there. Every later rung runs only if the previous rung of the SAME setting ran, was VALID and
    is HEALTHY under the corrected classification (CLIENT_LIMITED_V2 / SATURATED unchanged).
  * An approval file caps the highest rate that may run (max_rate_tps); for Tier 1 the cap is 40 and the matrix has no row above it.

usage: e1_p1b_matrix.py --out <new file> [--max-rate-tps 40]      (writes the matrix, refuses to overwrite, prints its SHA-256)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import random
import sys
from pathlib import Path

HEADER = "seq,run_id,channel,kind,B,tau,P,rate_tps,replicate,warmup_s,measured_s,ramp_precondition"
SERIES = "cp"
CHAINS = (10, 40)
ALL_RATES = (15, 20, 25, 30, 40, 60, 80, 100, 120, 140, 160, 180, 200)       # the full ladder as designed; rungs above TIER1_MAX_RATE are NOT authorised
TIER1_MAX_RATE = 40
RATES = tuple(r for r in ALL_RATES if r <= TIER1_MAX_RATE)                     # what this generator emits by default
ANCHORS = {10: ("ql23", 10), 40: ("ql17", 10)}      # the P1a rung-1 runs the new ramps continue from: (run id, rate)
SEED = 20261113
TAU, PREFERRED_MAX_BYTES, WARMUP_S, MEASURED_S = "2s", 10485760, 30, 120
FULL_DESIGN_SHA256 = "e73f0a362273317e32cb774ec2e289860eaffe342fb871473f65c6cd26ed8053"     # the 26-row design of 2026-11-13 (reproducible with max_rate=200)


def tier(rate: int) -> int:
    return 1 if rate <= TIER1_MAX_RATE else 2


def rates_upto(max_rate: int = TIER1_MAX_RATE) -> tuple[int, ...]:
    if max_rate not in ALL_RATES:
        raise ValueError(f"max rate {max_rate} is not a rung of the ladder {ALL_RATES}")
    return tuple(r for r in ALL_RATES if r <= max_rate)


def precondition(B: int, k: int) -> str:
    """k = index of the rung in the ladder. HEALTHY := verdict VALID and no label under the corrected classification v2."""
    if k == 0:
        anchor, rate = ANCHORS[B]
        return f"run only if the P1a run {anchor} (B={B}, rate={rate}) is HEALTHY in the erratum E1-QL-ERR-001 derived table"
    return f"run only if the ramp run at B={B}, rate={ALL_RATES[k - 1]} was HEALTHY"


def rows(max_rate: int = TIER1_MAX_RATE) -> list[tuple]:
    rng = random.Random(SEED)
    out: list[tuple] = []
    seq = 0
    for k, rate in enumerate(rates_upto(max_rate)):
        order = list(CHAINS)
        rng.shuffle(order)
        for B in order:
            seq += 1
            run_id = f"{SERIES}{seq:02d}"
            out.append((seq, run_id, f"e1-{run_id}-b{B}-l{rate}", "ramp", B, TAU, PREFERRED_MAX_BYTES, rate, 1, WARMUP_S, MEASURED_S,
                        precondition(B, k)))
    return out


def matrix_text(max_rate: int = TIER1_MAX_RATE) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER.split(","))
    w.writerows(rows(max_rate))
    return buf.getvalue()


def matrix_bytes(max_rate: int = TIER1_MAX_RATE) -> bytes:
    return matrix_text(max_rate).encode("utf-8")


def matrix_sha256(max_rate: int = TIER1_MAX_RATE) -> str:
    return hashlib.sha256(matrix_bytes(max_rate)).hexdigest()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").strip().splitlines()[0])
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--max-rate-tps", type=int, default=TIER1_MAX_RATE,
                    help="40 = Tier 1, the authorised scope (default); 200 = the full 26-row design (Tier 2 is NOT authorised)")
    args = ap.parse_args(argv)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    data = matrix_bytes(args.max_rate_tps)
    with open(args.out, "xb") as fh:
        fh.write(data)
    print(f"wrote {args.out}  rows {len(rows(args.max_rate_tps))}  max rate {args.max_rate_tps} tx/s  sha256 {hashlib.sha256(data).hexdigest()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
