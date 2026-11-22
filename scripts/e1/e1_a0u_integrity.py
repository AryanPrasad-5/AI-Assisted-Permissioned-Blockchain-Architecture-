"""X13 repair, runtime-identity and clock-only preparation tool, A0u (2026-11-22). OFFLINE: it reads files and writes NEW files. It starts nothing: no process, WSL, Docker, Caliper, Fabric, network or model.

    python scripts/e1/e1_a0u_integrity.py addendum --plan PLAN.json --path-record REC.json --distro NAME --mount-root /mnt --symlink-policy refuse \\
        --max-entries N --max-bytes N --max-depth N --digest-timeout-s S --out ADDENDUM.json
    python scripts/e1/e1_a0u_integrity.py verify-addendum --plan PLAN.json --path-record REC.json --addendum ADDENDUM.json
    python scripts/e1/e1_a0u_integrity.py authorization-template --plan PLAN.json --path-record REC.json --addendum ADDENDUM.json --distro NAME --category CAT --out AUTH.json
    python scripts/e1/e1_a0u_integrity.py verify-authorization --plan PLAN.json --path-record REC.json --addendum ADDENDUM.json --authorization AUTH.json --category CAT [--allow-synthetic]
    python scripts/e1/e1_a0u_integrity.py discovery-spec --plan PLAN.json --distro NAME --out SPEC.json
    python scripts/e1/e1_a0u_integrity.py clock-only-audit --plan CLOCK_PLAN.json
    python scripts/e1/e1_a0u_integrity.py clock-only-packet --plan CLOCK_PLAN.json --out PACKET.json
    python scripts/e1/e1_a0u_integrity.py execute        (always refused)

``execute`` (and the alias ``sample``) takes no arguments and ignores everything after it: no flag, environment variable or record turns real execution on in this version. This file imports no
process, network or container API and reads no environment variable (a test asserts it). Exit codes: 0 pass, 1 a check failed, 2 invalid request (nothing created), 5 execute refused.
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
from fabric_perf.controllers import clock_only_readiness as cr       # noqa: E402
from fabric_perf.controllers import path_mapping as pm               # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi          # noqa: E402
from fabric_perf.controllers import process_runner as pr             # noqa: E402
from fabric_perf.controllers import runtime_discovery as rd          # noqa: E402
from fabric_perf.controllers import tree_digest as td                # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi              # noqa: E402

TOOL_VERSION = "e1_a0u_integrity 0.1 (A0u, 2026-11-22)"
EXIT_REFUSED = 5


def _load(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("execute", "sample"):
        print("REFUSED: real execution is disabled in this version (process_runner.REAL_TRANSPORT_ENABLED and bounded_execution.REAL_LAUNCH_ENABLED are False). "
              "No process, WSL, Docker, Caliper, Fabric or clock sample was started.")
        return EXIT_REFUSED
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    p = sub.add_parser("addendum")
    for name in ("plan", "path-record", "out"):
        p.add_argument("--" + name, required=True, type=Path)
    for name in ("distro", "mount-root", "symlink-policy"):
        p.add_argument("--" + name, required=True)
    for name in ("max-entries", "max-bytes", "max-depth"):
        p.add_argument("--" + name, required=True, type=int)
    p.add_argument("--digest-timeout-s", required=True, type=float)
    p = sub.add_parser("verify-addendum")
    for name in ("plan", "path-record", "addendum"):
        p.add_argument("--" + name, required=True, type=Path)
    p = sub.add_parser("authorization-template")
    for name in ("plan", "path-record", "addendum", "out"):
        p.add_argument("--" + name, required=True, type=Path)
    p.add_argument("--distro", required=True)
    p.add_argument("--category", required=True, choices=list(pr.OPERATION_CATEGORIES))
    p = sub.add_parser("verify-authorization")
    for name in ("plan", "path-record", "addendum", "authorization"):
        p.add_argument("--" + name, required=True, type=Path)
    p.add_argument("--category", required=True, choices=list(pr.OPERATION_CATEGORIES))
    p.add_argument("--allow-synthetic", action="store_true")
    p = sub.add_parser("discovery-spec")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--distro", required=True)
    p.add_argument("--out", required=True, type=Path)
    p = sub.add_parser("clock-only-audit")
    p.add_argument("--plan", required=True, type=Path)
    p = sub.add_parser("clock-only-packet")
    p.add_argument("--plan", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.phase == "addendum":
            addendum = wi.build_integrity_addendum(_load(args.plan), record=_load(args.path_record), distro=args.distro, mount_root=args.mount_root, symlink_policy=args.symlink_policy,
                                                   limits={"max_entries": args.max_entries, "max_total_bytes": args.max_bytes, "max_depth": args.max_depth}, digest_timeout_s=args.digest_timeout_s,
                                                   repo_root=REPO)
            digest = pi.write_new_json(args.out, addendum)
            print(f"integrity addendum -> {args.out} (file sha256 {digest}; addendum sha256 {addendum['addendum_sha256']}); {addendum['status']}; nothing was started")
            return 0
        if args.phase == "verify-addendum":
            problems = wi.verify_addendum(_load(args.addendum), _load(args.plan), _load(args.path_record), repo_root=REPO)
            print(json.dumps({"problems": problems}, indent=2))
            return 1 if problems else 0
        if args.phase == "authorization-template":
            plan, record, addendum = _load(args.plan), _load(args.path_record), _load(args.addendum)
            description = pr.describe_plan_invocations(plan, distro=args.distro, mount_root=record["automount_root"], path_record=record)
            template = be.authorization_template(plan)
            template["a0t"] = pr.a0t_authorization_extension(plan, record, description, category=args.category)
            template["a0u"] = wi.a0u_authorization_extension(addendum)
            digest = pi.write_new_json(args.out, template)
            print(f"authorization template (a0s + a0t + a0u) -> {args.out} (sha256 {digest}); every approval and owner field is unset")
            return 0
        if args.phase == "verify-authorization":
            plan, record, addendum, auth = _load(args.plan), _load(args.path_record), _load(args.addendum), _load(args.authorization)
            ext = auth.get("a0t") if isinstance(auth.get("a0t"), dict) else {}
            try:
                description = pr.describe_plan_invocations(plan, distro=str(ext.get("distro")), mount_root=record["automount_root"], path_record=record)
            except (pm.PathMappingError, pr.ArgvRefused, pr.RunnerRefused) as exc:
                description = {"error": str(exc)}
            base = pr.verify_a0t_authorization(auth, plan, record, description, repo_root=REPO, allow_synthetic=args.allow_synthetic, expected_category=args.category)
            extra = wi.verify_a0u_authorization(auth, plan, addendum, allow_synthetic=args.allow_synthetic)
            problems = wi.verify_addendum(addendum, plan, record, repo_root=REPO)
            valid = base["valid"] and extra["valid"] and not problems
            print(json.dumps({"valid": valid, "a0t": base, "a0u": extra, "addendum_problems": problems, "real_transport_permitted": False}, indent=2))
            return 0 if valid else 1
        if args.phase == "discovery-spec":
            plan = _load(args.plan)
            spec = rd.discovery_spec(plan, rd.scan_container_declarations(REPO), distro=args.distro)
            digest = pi.write_new_json(args.out, spec)
            print(f"runtime-discovery specification -> {args.out} (sha256 {digest}); {spec['status']}; containers declared: {spec['summary']['distinct_container_names']}; nothing was executed")
            return 0
        if args.phase == "clock-only-audit":
            problems = cr.audit_clock_only_plan(_load(args.plan))
            print(json.dumps({"problems": problems, "transaction_free": not problems}, indent=2))
            return 1 if problems else 0
        if args.phase == "clock-only-packet":
            plan = _load(args.plan)
            digest = pi.write_new_json(args.out, cr.approval_packet_template(plan))
            print(f"clock-only approval packet -> {args.out} (sha256 {digest}); every owner and approval field is unset; no value is a proposal")
            return 0
    except (be.RequestInvalid, wi.IntegrityRefused, td.DigestRefused, pm.PathMappingError, pr.RunnerRefused, pr.ArgvRefused, ValueError, KeyError, OSError) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
