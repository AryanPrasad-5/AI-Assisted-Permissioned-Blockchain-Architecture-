"""Bounded execution entry point, A0s (2026-11-22). OFFLINE: it reads files and writes one new plan or template file. It starts nothing.

    python scripts/e1/e1_a0s_bounded_execute.py prepare --request R.json --out-plan PLAN.json --created-utc 2026-11-23T09:00:00Z
    python scripts/e1/e1_a0s_bounded_execute.py preflight --plan PLAN.json
    python scripts/e1/e1_a0s_bounded_execute.py inspect-plan --plan PLAN.json
    python scripts/e1/e1_a0s_bounded_execute.py authorization-template --plan PLAN.json --out AUTH_TEMPLATE.json
    python scripts/e1/e1_a0s_bounded_execute.py verify-authorization --plan PLAN.json --authorization AUTH.json
    python scripts/e1/e1_a0s_bounded_execute.py execute          (always refused)

``execute`` takes no arguments and ignores everything after it: there is no flag, environment variable or record that turns it on in this version
(``fabric_perf.controllers.bounded_execution.REAL_LAUNCH_ENABLED`` is False and this file imports no process, network or container API; a test asserts both).
A future launch needs a new, separately approved tool that supplies a process runner.

Exit codes: 0 pass, 1 a check failed, 2 invalid request (nothing created), 5 execute refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi       # noqa: E402

TOOL_VERSION = "e1_a0s_bounded_execute 0.1 (A0s, 2026-11-22)"
EXIT_EXECUTE_REFUSED = 5


def _load(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def render_plan(plan: dict) -> str:
    layout = plan["layout"]
    lines = [f"plan {plan['plan_sha256']}  ({plan['status']})", f"variant {plan['variant']}  channel {plan['channel']}  cell {plan['cell_id']}  run {plan['run_id']}  workers {plan['worker_count']}",
             f"envelope: {plan['envelope']['planned_transactions']:g} planned transactions over {plan['envelope']['planned_total_duration_s']:g} s; limits {plan['envelope']['limits']}",
             f"output directory (must not exist): {layout['output_dir']}", f"worker evidence root: {layout['worker_evidence_root_host']}",
             f"mounts: {plan['mounts']}", "steps:"]
    for step in plan["steps"]:
        lines.append(f"  {step['step']:<24} [{step['class']}] {step['kind']}  timeout={step['timeout_s']}  cwd={step['cwd']}")
        if step["argv"]:
            lines.append("      argv: " + " ".join(step["argv"]))
        lines.append(f"      {step['note']}")
        if step["kind"] == "gate_config":
            stage = "initial" if step["step"] == "X02_config_initial" else "final"
            for vector in plan["config_inspection"][stage]["steps"]:
                lines.append(f"      read-only vector {vector['step']}: " + " ".join(vector["argv"]))
    lines.append(f"termination: {plan['termination']['behavior']}")
    lines.append(f"retry: {plan['retry_policy']}")
    lines.append("claims not made: " + "; ".join(plan["claims_not_made"]))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "execute":
        print("REFUSED: execute is disabled in this version (REAL_LAUNCH_ENABLED is False). No flag, environment variable or approval record enables it; "
              "a launch needs a new, separately approved tool. Nothing was started.")
        return EXIT_EXECUTE_REFUSED
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--request", required=True, type=Path)
    p.add_argument("--out-plan", required=True, type=Path)
    p.add_argument("--created-utc", required=True, help="ISO-8601 with a timezone (no clock is read)")
    for name in ("preflight", "inspect-plan"):
        sub.add_parser(name).add_argument("--plan", required=True, type=Path)
    t = sub.add_parser("authorization-template")
    t.add_argument("--plan", required=True, type=Path)
    t.add_argument("--out", required=True, type=Path)
    v = sub.add_parser("verify-authorization")
    v.add_argument("--plan", required=True, type=Path)
    v.add_argument("--authorization", required=True, type=Path)
    v.add_argument("--allow-synthetic", action="store_true", help="accept a synthetic record for this CHECK only; it never enables a launch")
    sub.add_parser("execute")
    args = parser.parse_args(argv)
    try:
        if args.phase == "prepare":
            if args.out_plan.exists():
                print(f"REFUSED: {args.out_plan} already exists; a plan is never overwritten")
                return 2
            plan = be.build_execution_plan(_load(args.request), repo_root=REPO, created_utc=args.created_utc)
            digest = pi.write_new_json(args.out_plan, plan)
            print(f"plan -> {args.out_plan} (sha256 {digest}); plan_sha256 {plan['plan_sha256']}; {len(plan['steps'])} steps; nothing started")
            return 0
        plan = _load(args.plan)
        if args.phase == "preflight":
            problems = be.verify_plan(plan, repo_root=REPO)
            print(json.dumps({"tool": TOOL_VERSION, "plan_sha256": plan.get("plan_sha256"), "problems": problems, "live_ready": False,
                              "note": "an intact plan is not an authorization and not a live validation"}, indent=2))
            return 1 if problems else 0
        if args.phase == "inspect-plan":
            print(render_plan(plan))
            return 0
        if args.phase == "authorization-template":
            if args.out.exists():
                print(f"REFUSED: {args.out} already exists")
                return 2
            pi.write_new_json(args.out, be.authorization_template(plan))
            print(f"template -> {args.out} (every approval field unset)")
            return 0
        verdict = be.verify_authorization(_load(args.authorization), plan, repo_root=REPO, allow_synthetic=args.allow_synthetic)
        print(json.dumps(verdict, indent=2))
        return 0 if verdict["valid"] else 1
    except be.RequestInvalid as exc:
        print(json.dumps({"invalid": exc.issues}, indent=2))
        return 2
    except (OSError, ValueError) as exc:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
