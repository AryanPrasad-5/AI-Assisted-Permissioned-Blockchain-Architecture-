"""Post-hoc timing sampler T01-T06, command line (task A0o, 2026-11-21). OFFLINE ONLY: it reads finished files and writes one JSON file.

    python scripts/e1/e1_a0o_timing_sampler.py --origin synthetic_fixture --issue-log LOG [--submits CSV --submits-clock-domain D] [--checkpoint CK --reader-clock-domain D
           [--offset-low-ms A --offset-high-ms B]] [--spool SPOOL] [--candidate-gap-limit-s S] --out OUT.json

The evidence origin is required and only ``synthetic_fixture`` and ``stored_scripted_clock_evidence`` are accepted: the output is fixture arithmetic, never a Caliper latency. Missing inputs
give NOT_MEASURABLE_FROM_EXISTING_EVIDENCE with the missing dependency named. The output path must not exist. No wall clock is read.

Exit codes: 0 computed (no input invalid), 1 at least one measurement had an invalid input, 2 invalid request.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import prelive_integrity as pi          # noqa: E402
from fabric_perf.controllers import timing_sampler as ts             # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--origin", required=True)
    parser.add_argument("--issue-log", type=Path)
    parser.add_argument("--submits", type=Path)
    parser.add_argument("--submits-clock-domain")
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--reader-clock-domain")
    parser.add_argument("--offset-low-ms", type=float)
    parser.add_argument("--offset-high-ms", type=float)
    parser.add_argument("--spool", type=Path)
    parser.add_argument("--candidate-gap-limit-s", type=float)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    if (args.offset_low_ms is None) != (args.offset_high_ms is None):
        print("REFUSED: --offset-low-ms and --offset-high-ms must be given together")
        return 2
    offset = None if args.offset_low_ms is None else [args.offset_low_ms, args.offset_high_ms]
    try:
        doc = ts.run_sampler(origin=args.origin, issue_log=args.issue_log, submits=args.submits, submits_clock_domain=args.submits_clock_domain, checkpoint=args.checkpoint,
                             reader_clock_domain=args.reader_clock_domain, offset_interval_ms=offset, spool=args.spool, candidate_gap_limit_s=args.candidate_gap_limit_s)
        pi.write_new_json(args.out, doc)
    except (ts.TimingSamplerError, OSError, ValueError) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return 2
    summary = {k: v["status"] for k, v in doc["measurements"].items()}
    print(json.dumps({"out": str(args.out), "origin": doc["evidence_origin"], "statuses": summary}, indent=2))
    return 1 if ts.S_INVALID in summary.values() else 0


if __name__ == "__main__":
    sys.exit(main())
