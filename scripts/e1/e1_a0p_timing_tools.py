"""A0p offline tools: verify an issue-log timing sidecar, run the incremental recorder-health bridge over a FINISHED spool, and compute T01-T06 (task A0p, 2026-11-22).

OFFLINE ONLY. It reads finished files and writes new ones; it contacts nothing and measures nothing. Every identity, clock-domain label and policy value is an explicit argument (there are no
defaults), and an output path that already exists is refused.

    python scripts/e1/e1_a0p_timing_tools.py verify-write-timing --log L --sidecar S --out OUT.json
    python scripts/e1/e1_a0p_timing_tools.py bridge  --spool SP --journal J --ledger LG --checkpoint CK --source-id ID --run-id RUN --spool-clock-domain D1 --bridge-clock-domain D2
            --heartbeat-interval-s N --max-heartbeat-gap-s G --freshness-limit-s F --clock-tolerance-ms T [--resume] [--source-ended] --out REPORT.json
    python scripts/e1/e1_a0p_timing_tools.py sample  --origin synthetic_fixture|stored_scripted_clock_evidence [--issue-log L --issue-log-timing S] [--spool SP --ingest-ledger LG] ... --out OUT.json

Exit codes: 0 verified / complete, 1 not verified or incomplete (a result file is still written), 2 invalid request.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
for _entry in (str(REPO / "src"),):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

from fabric_perf.controllers import issue_log_timing as ilt          # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi          # noqa: E402
from fabric_perf.controllers import recorder_health_bridge as rhb    # noqa: E402
from fabric_perf.controllers import recorder_health_incremental as rhi   # noqa: E402
from fabric_perf.controllers import timing_durable as td             # noqa: E402
from fabric_perf.controllers import timing_sampler as ts             # noqa: E402


def _write(out: Path, doc: dict) -> None:
    pi.write_new_json(out, doc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    v = sub.add_parser("verify-write-timing")
    v.add_argument("--log", required=True, type=Path)
    v.add_argument("--sidecar", required=True, type=Path)
    v.add_argument("--out", required=True, type=Path)
    b = sub.add_parser("bridge")
    for name in ("spool", "journal", "ledger", "checkpoint", "out"):
        b.add_argument(f"--{name}", required=True, type=Path)
    for name in ("source-id", "run-id", "spool-clock-domain", "bridge-clock-domain"):
        b.add_argument(f"--{name}", required=True)
    b.add_argument("--heartbeat-interval-s", required=True, type=int)
    for name in ("max-heartbeat-gap-s", "freshness-limit-s", "clock-tolerance-ms"):
        b.add_argument(f"--{name}", required=True, type=float)
    b.add_argument("--max-rows-per-poll", type=int, default=500)
    b.add_argument("--resume", action="store_true")
    b.add_argument("--source-ended", action="store_true")
    s = sub.add_parser("sample")
    s.add_argument("--origin", required=True)
    for name in ("issue-log", "issue-log-timing", "submits", "checkpoint", "spool", "ingest-ledger"):
        s.add_argument(f"--{name}", type=Path)
    for name in ("submits-clock-domain", "reader-clock-domain"):
        s.add_argument(f"--{name}")
    s.add_argument("--reader-offset-ms", nargs=2, type=float, metavar=("LO", "HI"))
    s.add_argument("--bridge-offset-ms", nargs=2, type=float, metavar=("LO", "HI"))
    s.add_argument("--candidate-gap-limit-s", type=float)
    s.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.out.exists():
            raise ValueError(f"{args.out} already exists; evidence is never overwritten")
        if args.phase == "verify-write-timing":
            doc = ilt.verify_timing(args.log, args.sidecar).as_dict()
            _write(args.out, doc)
            print(f"write-timing {doc['status']}: {doc['attested_records']} of {doc['log_records']} records attested, {doc['syncs']} sync row(s), "
                  f"{doc['unsynced_rseqs_count']} record(s) with no sync evidence")
            return 0 if doc["status"] == ilt.VERIFIED else 1
        if args.phase == "bridge":
            policy = rhb.RecorderHealthPolicy(args.heartbeat_interval_s, args.max_heartbeat_gap_s, args.freshness_limit_s, args.clock_tolerance_ms)
            cfg = rhi.BridgeConfig(args.spool, args.journal, args.ledger, args.checkpoint, source_id=args.source_id, run_id=args.run_id, policy=policy,
                                   spool_clock_domain=args.spool_clock_domain, bridge_clock_domain=args.bridge_clock_domain, max_rows_per_poll=args.max_rows_per_poll)
            bridge = rhi.IncrementalRecorderBridge.resume(cfg) if args.resume else rhi.IncrementalRecorderBridge.create(cfg)
            bridge.poll_until_drained()
            report = bridge.close(source_ended=args.source_ended)
            _write(args.out, report)
            print(f"incremental bridge: {report['journal_records']} journal rows, complete={report['complete']}, problems={[p['code'] for p in report['problems']]}")
            return 0 if report["complete"] else 1
        result = td.run_sampler_a0p(origin=args.origin, issue_log=args.issue_log, issue_log_timing=args.issue_log_timing, submits=args.submits,
                                    submits_clock_domain=args.submits_clock_domain, checkpoint=args.checkpoint, reader_clock_domain=args.reader_clock_domain,
                                    offset_interval_ms=args.reader_offset_ms, spool=args.spool, ingest_ledger=args.ingest_ledger, bridge_offset_interval_ms=args.bridge_offset_ms,
                                    candidate_gap_limit_s=args.candidate_gap_limit_s)
        _write(args.out, result)
        print("timing: " + ", ".join(f"{k}={m['status']}" for k, m in result["measurements"].items()))
        return 0
    except (ValueError, OSError, rhb.BridgeError, ts.TimingSamplerError) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
