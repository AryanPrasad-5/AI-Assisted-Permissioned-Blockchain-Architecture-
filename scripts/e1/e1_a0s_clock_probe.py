"""Windows <-> WSL clock probe, offline side only, A0s (2026-11-22). It parses and evaluates stored samples and prints the probe command as text. It samples nothing.

    python scripts/e1/e1_a0s_clock_probe.py plan     --distro NAME --remote-resolution-ns N --timeout-s S
    python scripts/e1/e1_a0s_clock_probe.py parse    --reading-file remote_stdout.txt
    python scripts/e1/e1_a0s_clock_probe.py check    --phases pre.json [post.json ...]
    python scripts/e1/e1_a0s_clock_probe.py evaluate --phases pre.json post.json --tolerance-ms T --min-valid-samples N --max-wall-monotonic-disagreement-ms M
    python scripts/e1/e1_a0s_clock_probe.py sample   (always refused)

A phase file is one ``wsl_clock_phase/1`` record. ``evaluate`` hands the samples to ``fabric_perf.controllers.clock_preflight`` (the A0e evaluator). The verdict is evidence about
the samples only. ``sample`` (the real measurement) is refused: real sampling is disabled in this version and needs a separately authorized tool supplying a process runner.

Exit codes: 0 AGREE / no problem, 1 DISAGREE or a structural problem, 2 invalid request, 3 INCONCLUSIVE, 5 sampling refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import wsl_clock_probe as wp      # noqa: E402

EXIT_REFUSED = 5


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "sample":
        print("REFUSED: real clock sampling is disabled in this version (wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED is False). WSL was not started and no clock was sampled.")
        return EXIT_REFUSED
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--distro", required=True)
    p.add_argument("--remote-resolution-ns", required=True, type=int)
    p.add_argument("--timeout-s", required=True, type=float)
    sub.add_parser("parse").add_argument("--reading-file", required=True, type=Path)
    sub.add_parser("check").add_argument("--phases", required=True, nargs="+", type=Path)
    e = sub.add_parser("evaluate")
    e.add_argument("--phases", required=True, nargs="+", type=Path)
    e.add_argument("--tolerance-ms", required=True, type=float)
    e.add_argument("--min-valid-samples", required=True, type=int)
    e.add_argument("--max-wall-monotonic-disagreement-ms", required=True, type=float)
    sub.add_parser("sample")
    args = parser.parse_args(argv)
    try:
        if args.phase == "plan":
            print(json.dumps(wp.probe_plan(args.distro, remote_resolution_ns=args.remote_resolution_ns, timeout_s=args.timeout_s), indent=2))
            return 0
        if args.phase == "parse":
            wall, mono = wp.parse_remote_reading(args.reading_file.read_bytes())
            print(json.dumps({"remote_wall_ns": wall, "remote_mono_ns": mono}))
            return 0
        records = [json.loads(path.read_text(encoding="utf-8")) for path in args.phases]
        if args.phase == "check":
            problems = [{"phase": r.get("phase"), **p} for r in records for p in wp.check_phase(r)]
            print(json.dumps({"problems": problems}, indent=2))
            return 1 if problems else 0
        result = wp.evaluate(records, tolerance_ms=args.tolerance_ms, min_valid_samples=args.min_valid_samples,
                             max_wall_monotonic_disagreement_ms=args.max_wall_monotonic_disagreement_ms)
        print(json.dumps(result, indent=2, sort_keys=True))
        return {"AGREE": 0, "DISAGREE": 1, "INCONCLUSIVE": 3}[result["verdict"]]
    except wp.SampleInvalid as exc:
        print(f"INVALID READING: {exc.code}: {exc}")
        return 2
    except (wp.ProbeRefused, OSError, ValueError) as exc:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
