"""WSL transport, clock-only plan and run-outcome tool, A0v (2026-11-22). OFFLINE: it reads files and writes NEW files. It starts nothing: no process, WSL, Docker, Caliper, Fabric, network or model.

    python scripts/e1/e1_a0v_transport.py clock-plan --out PLAN.json [--request REQUEST.json] [--created-utc ISO]
    python scripts/e1/e1_a0v_transport.py clock-activation-template --request REQUEST.json --out RECORD.json [--created-utc ISO]
    python scripts/e1/e1_a0v_transport.py classify-bounded --plan PLAN.json --addendum ADDENDUM.json --run-dir DIR [--write]
    python scripts/e1/e1_a0v_transport.py classify-clock --clock-plan CLOCK_PLAN.json --run-dir DIR [--write]
    python scripts/e1/e1_a0v_transport.py execute        (always refused)

``clock-plan`` without ``--request`` uses the blank template (every value unset); the result is an UNRESOLVED plan with no vector. ``classify-*`` re-reads stored evidence and prints the outcome
(exit 0 = verified success, 1 = anything else); ``--write`` additionally creates ``run_outcome.json`` exclusively in the run directory. ``execute`` (and ``sample``) takes no arguments and ignores
everything after it: no flag, environment variable or record turns real execution on in this version. This file imports no process, network or container API and reads no environment variable (a
test asserts it). Exit codes: 0 ok, 1 not a verified success, 2 invalid request (nothing created), 5 execute refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import bounded_execution as be          # noqa: E402
from fabric_perf.controllers import clock_only_round as co           # noqa: E402
from fabric_perf.controllers import clock_transport_plan as ctp      # noqa: E402
from fabric_perf.controllers import run_outcome as ro                # noqa: E402

TOOL_VERSION = "e1_a0v_transport 0.1 (A0v, 2026-11-22)"
EXIT_REFUSED = 5
DEFAULT_CREATED = "2026-11-22T00:00:00Z"


def _load(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_new(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "xb") as handle:                                   # never overwrites
        handle.write(text.encode("utf-8"))


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("execute", "sample"):
        print("REFUSED: real execution is disabled in this version (wsl_transport.REAL_SPAWN_COMPILED_IN, process_runner.REAL_TRANSPORT_ENABLED and bounded_execution.REAL_LAUNCH_ENABLED are all "
              "False). No process, WSL, Docker, Caliper, Fabric or clock sample was started.")
        return EXIT_REFUSED
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    p = sub.add_parser("clock-plan")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--request", type=Path)
    p.add_argument("--created-utc", default=DEFAULT_CREATED)
    p = sub.add_parser("clock-activation-template")
    p.add_argument("--request", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--created-utc", default=DEFAULT_CREATED)
    p = sub.add_parser("classify-bounded")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--addendum", required=True, type=Path)
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument("--write", action="store_true")
    p = sub.add_parser("classify-clock")
    p.add_argument("--clock-plan", required=True, type=Path)
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.phase == "clock-plan":
            request = _load(args.request) if args.request else co.blank_request_template()
            plan = ctp.build_clock_transport_plan(request, created_utc=args.created_utc)
            _write_new(args.out, ctp.plan_json(plan))
            print(f"clock transport plan -> {args.out} ({plan['status']}; plan_sha256 {plan['plan_sha256']}); unresolved request fields: {len(plan['unresolved_request_fields'])}; nothing was executed")
            return 0
        if args.phase == "clock-activation-template":
            plan = ctp.build_clock_transport_plan(_load(args.request), created_utc=args.created_utc)
            if plan["activation_record_template"] is None:
                print(f"REFUSED: the request is not complete and valid, so no activation template can be derived: {plan['unresolved_request_fields'] or plan['request_issues']}")
                return 2
            _write_new(args.out, json.dumps(plan["activation_record_template"], indent=2, sort_keys=True) + "\n")
            print(f"activation record template (every owner field unset) -> {args.out}; record_sha256 {plan['activation_record_template']['record_sha256']}")
            return 0
        if args.phase == "classify-bounded":
            outcome = ro.classify_bounded_run(args.run_dir, _load(args.plan), _load(args.addendum))
        else:
            outcome = ro.classify_clock_round(args.run_dir, _load(args.clock_plan))
        if args.write:
            ro.write_outcome(args.run_dir, outcome)
        print(json.dumps(outcome, indent=2, sort_keys=True))
        return 0 if outcome["success"] else 1
    except (OSError, ValueError, KeyError, be.RequestInvalid) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
