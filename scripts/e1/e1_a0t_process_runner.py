"""Process-runner preparation tool, A0t (2026-11-22). OFFLINE: it reads files and writes NEW files. It starts nothing: no process, WSL, Docker, Caliper, Fabric, network or model.

    python scripts/e1/e1_a0t_process_runner.py inspect-invocations --plan PLAN.json --distro NAME --mount-root /mnt [--path-record REC.json]
    python scripts/e1/e1_a0t_process_runner.py path-record --plan PLAN.json --repo-windows C:\\path --env-json ENV.json --container-root /hyperledger/caliper/workspace \\
                                                --container-name caliper --container-source TEXT --out REC.json
    python scripts/e1/e1_a0t_process_runner.py authorization-template --plan PLAN.json --path-record REC.json --distro NAME --category bounded_transaction_smoke --out AUTH.json
    python scripts/e1/e1_a0t_process_runner.py verify-authorization --plan PLAN.json --path-record REC.json --authorization AUTH.json --category CAT [--allow-synthetic]
    python scripts/e1/e1_a0t_process_runner.py clock-only-plan --request REQ.json --out-plan PLAN.json --created-utc 2026-11-23T09:00:00Z
    python scripts/e1/e1_a0t_process_runner.py clock-only-template --plan CLOCK_PLAN.json --out AUTH.json
    python scripts/e1/e1_a0t_process_runner.py clock-only-verify --plan CLOCK_PLAN.json --authorization AUTH.json [--allow-synthetic]
    python scripts/e1/e1_a0t_process_runner.py capacity-design --headroom-results FILE --out DESIGN.json
    python scripts/e1/e1_a0t_process_runner.py capacity-schedule --design DESIGN.json --seed N --rungs 40,50,60 --replicates 3 --channel-family NAME
    python scripts/e1/e1_a0t_process_runner.py execute        (always refused)

``execute`` (and the alias ``sample``) takes no arguments and ignores everything after it: there is no flag, environment variable or record that turns real execution on in this version
(``process_runner.REAL_TRANSPORT_ENABLED`` and ``bounded_execution.REAL_LAUNCH_ENABLED`` are False; this file imports no process, network or container API and reads no environment variable;
a test asserts all of that). Exit codes: 0 pass, 1 a check failed, 2 invalid request (nothing created), 5 execute refused.
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
from fabric_perf.controllers import capacity_bracket as cb           # noqa: E402
from fabric_perf.controllers import clock_only_round as co           # noqa: E402
from fabric_perf.controllers import path_mapping as pm               # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi          # noqa: E402
from fabric_perf.controllers import process_runner as pr             # noqa: E402

TOOL_VERSION = "e1_a0t_process_runner 0.1 (A0t, 2026-11-22)"
EXIT_REFUSED = 5


def _load(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_new(path: Path, document) -> str:
    return pi.write_new_json(Path(path), document)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("execute", "sample"):
        print("REFUSED: real execution is disabled in this version (process_runner.REAL_TRANSPORT_ENABLED and bounded_execution.REAL_LAUNCH_ENABLED are False). "
              "No process, WSL, Docker, Caliper, Fabric or clock sample was started.")
        return EXIT_REFUSED
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    p = sub.add_parser("inspect-invocations")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--distro", required=True)
    p.add_argument("--mount-root", required=True)
    p.add_argument("--path-record", type=Path)
    p = sub.add_parser("path-record")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--repo-windows", required=True)
    p.add_argument("--env-json", required=True, type=Path)
    p.add_argument("--container-root", required=True)
    p.add_argument("--container-name", required=True)
    p.add_argument("--container-source", required=True)
    p.add_argument("--out", required=True, type=Path)
    p = sub.add_parser("authorization-template")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--path-record", required=True, type=Path)
    p.add_argument("--distro", required=True)
    p.add_argument("--category", required=True, choices=list(pr.OPERATION_CATEGORIES))
    p.add_argument("--out", required=True, type=Path)
    p = sub.add_parser("verify-authorization")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--path-record", required=True, type=Path)
    p.add_argument("--authorization", required=True, type=Path)
    p.add_argument("--category", required=True, choices=list(pr.OPERATION_CATEGORIES))
    p.add_argument("--allow-synthetic", action="store_true")
    p = sub.add_parser("clock-only-plan")
    p.add_argument("--request", required=True, type=Path)
    p.add_argument("--out-plan", required=True, type=Path)
    p.add_argument("--created-utc", required=True)
    p = sub.add_parser("clock-only-template")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p = sub.add_parser("clock-only-verify")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--authorization", required=True, type=Path)
    p.add_argument("--allow-synthetic", action="store_true")
    p = sub.add_parser("capacity-design")
    p.add_argument("--headroom-results", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p = sub.add_parser("capacity-schedule")
    p.add_argument("--design", required=True, type=Path)
    p.add_argument("--seed", required=True, type=int)
    p.add_argument("--rungs", required=True)
    p.add_argument("--replicates", required=True, type=int)
    p.add_argument("--channel-family", required=True)
    args = parser.parse_args(argv)
    try:
        if args.phase == "inspect-invocations":
            plan = _load(args.plan)
            record = _load(args.path_record) if args.path_record else None
            print(json.dumps(pr.describe_plan_invocations(plan, distro=args.distro, mount_root=args.mount_root, path_record=record), indent=2))
            return 0
        if args.phase == "path-record":
            plan, env = _load(args.plan), _load(args.env_json)
            mount = pm.ContainerMount(wsl_root=env["caliper_workspace"], container_root=args.container_root, container_name=args.container_name, source=args.container_source)
            record = pm.build_path_record(plan, repo_root_windows=args.repo_windows, repo_declared_wsl=env["repo"], repo_declaration_source=f"{args.env_json.as_posix()}: \"repo\"",
                                          mount_root=None, container_mount=mount, workspace_declaration_source=args.container_source)
            digest = _write_new(args.out, record)
            print(f"path-mapping record -> {args.out} (file sha256 {digest}; record sha256 {record['record_sha256']}); highest offline level {record['highest_offline_level_reached']}; runtime verified: no")
            return 0
        if args.phase == "authorization-template":
            plan, record = _load(args.plan), _load(args.path_record)
            if pm.verify_path_record(record):
                print("REFUSED: the path-mapping record does not verify")
                return 2
            description = pr.describe_plan_invocations(plan, distro=args.distro, mount_root=record["automount_root"], path_record=record)
            template = be.authorization_template(plan)
            template["a0t"] = pr.a0t_authorization_extension(plan, record, description, category=args.category)
            digest = _write_new(args.out, template)
            print(f"authorization template -> {args.out} (sha256 {digest}); every approval and A0t owner field is unset; the invocation description was computed for distro {args.distro!r}")
            return 0
        if args.phase == "verify-authorization":
            plan, record, auth = _load(args.plan), _load(args.path_record), _load(args.authorization)
            ext = auth.get("a0t") if isinstance(auth.get("a0t"), dict) else {}
            try:
                description = pr.describe_plan_invocations(plan, distro=str(ext.get("distro")), mount_root=record["automount_root"], path_record=record)
            except (pm.PathMappingError, pr.ArgvRefused, pr.RunnerRefused) as exc:
                description = {"error": str(exc)}
            verdict = pr.verify_a0t_authorization(auth, plan, record, description, repo_root=REPO, allow_synthetic=args.allow_synthetic, expected_category=args.category)
            print(json.dumps(verdict, indent=2))
            return 0 if verdict["valid"] else 1
        if args.phase == "clock-only-plan":
            plan = co.build_clock_only_plan(_load(args.request), created_utc=args.created_utc)
            digest = _write_new(args.out_plan, plan)
            print(f"clock-only plan -> {args.out_plan} (sha256 {digest}; plan digest {plan['plan_sha256']}); transactions planned: 0; status {plan['status']}")
            return 0
        if args.phase == "clock-only-template":
            digest = _write_new(args.out, co.authorization_template(_load(args.plan)))
            print(f"clock-only authorization template -> {args.out} (sha256 {digest}); every approval and transport field is unset")
            return 0
        if args.phase == "clock-only-verify":
            verdict = co.verify_authorization(_load(args.authorization), _load(args.plan), allow_synthetic=args.allow_synthetic)
            print(json.dumps(verdict, indent=2))
            return 0 if verdict["valid"] else 1
        if args.phase == "capacity-design":
            results = _load(args.headroom_results)
            design = cb.build_design(measured_pairs=results["measured_fabric_evidence"]["pairs"])
            problems = cb.validate_design(design)
            if problems:
                print(json.dumps(problems, indent=2))
                return 1
            digest = _write_new(args.out, design)
            print(f"capacity-bracket design -> {args.out} (sha256 {digest}); {design['status']}; owner parameters required: {len(design['required_owner_parameters'])}")
            return 0
        if args.phase == "capacity-schedule":
            design = _load(args.design)
            rungs = [int(x) for x in args.rungs.split(",")]
            print(json.dumps(cb.build_schedule(design, seed=args.seed, rungs_tps=rungs, replicates=args.replicates, channel_family=args.channel_family), indent=2))
            return 0
    except (be.RequestInvalid, pm.PathMappingError, pr.RunnerRefused, pr.ArgvRefused, cb.DesignInvalid, ValueError, KeyError, OSError) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
