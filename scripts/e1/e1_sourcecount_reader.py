"""SourceCount reader driver, command line (task A0o, 2026-11-21). OFFLINE ONLY: it reads an issue log and writes one JSON file.

    python scripts/e1/e1_sourcecount_reader.py inspect --run-id R --round N --worker w0 --log LOG --clock-domain D --read-time ISO --max-tick-gap-ms MS --out OUT.json
    python scripts/e1/e1_sourcecount_reader.py replay  --run-id R --round N --worker w0 --source-log LOG --work-dir DIR --schedule S.json --clock-domain D --max-tick-gap-ms MS --out OUT.json
    python scripts/e1/e1_sourcecount_reader.py follow  ...     (always refused: it could read an active live experiment)

Every identity (run, round, worker, log, clock domain) and every time is explicit; nothing is defaulted, no wall clock is read, and a naive time is refused. ``--max-tick-gap-ms`` has no
default because its placeholder is unapproved. The output is ``sourcecount_reader_audit/1`` (the evidence is ``source_count_evidence/1``). It is written to a path that must not exist.

Exit codes: 0 VERIFIED_SPANS, 1 PARTIAL / UNVERIFIED / INVALID (evidence is not complete: never read this as zero issuance), 2 invalid request, 5 follow refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import prelive_integrity as pi          # noqa: E402
from fabric_perf.controllers import sourcecount_reader as sr         # noqa: E402

EXIT_FOLLOW_REFUSED = 5


def _identity(args: argparse.Namespace, log: Path, checkpoint: Path | None) -> sr.ReaderIdentity:
    return sr.ReaderIdentity(run_id=args.run_id, round_index=args.round, worker_id=args.worker, log_path=log, clock_domain=args.clock_domain, checkpoint_path=checkpoint)


def _emit(audit: dict[str, Any], out: Path) -> int:
    pi.write_new_json(out, audit)
    status = audit["evidence"]["status"]
    print(f"{status}: {', '.join(audit['evidence']['failure_states']) or 'no failure state'} -> {out}")
    return 0 if status == sr.ST_VERIFIED else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in (sr.MODE_INSPECT, sr.MODE_REPLAY):
        p = sub.add_parser(name)
        p.add_argument("--run-id", required=True)
        p.add_argument("--round", required=True, type=int)
        p.add_argument("--worker", required=True)
        p.add_argument("--clock-domain", required=True)
        p.add_argument("--max-tick-gap-ms", required=True, type=int)
        p.add_argument("--out", required=True, type=Path)
        p.add_argument("--checkpoint", type=Path)
        if name == sr.MODE_INSPECT:
            p.add_argument("--log", required=True, type=Path)
            p.add_argument("--read-time", required=True, help="the reader's observation time, ISO-8601 with an offset")
        else:
            p.add_argument("--source-log", required=True, type=Path)
            p.add_argument("--work-dir", required=True, type=Path)
            p.add_argument("--schedule", required=True, type=Path)
            p.add_argument("--as-of", help="decision cutoff, ISO-8601 with an offset")
    sub.add_parser(sr.MODE_FOLLOW).add_argument("rest", nargs="*")
    args, _unknown = parser.parse_known_args(argv)
    if args.mode == sr.MODE_FOLLOW:
        try:
            sr.follow()
        except sr.SourceCountReaderError as exc:
            print(f"REFUSED: {exc}")
        return EXIT_FOLLOW_REFUSED
    try:
        limits = {"max_tick_gap_ms": args.max_tick_gap_ms}
        if args.mode == sr.MODE_INSPECT:
            audit = sr.inspect_log(_identity(args, args.log, args.checkpoint), read_time=args.read_time, policy_limits=limits)
        else:
            schedule = json.loads(args.schedule.read_text(encoding="utf-8"))
            steps = sr.load_schedule(schedule)
            work = args.work_dir / f"{args.run_id}_w0_r{args.round}.replay.issuelog.jsonl"
            _reader, audit = sr.replay_log(_identity(args, work, args.checkpoint), source_log=args.source_log, schedule=steps, policy_limits=limits, as_of=args.as_of)
        return _emit(audit, args.out)
    except (sr.SourceCountReaderError, OSError, ValueError) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
