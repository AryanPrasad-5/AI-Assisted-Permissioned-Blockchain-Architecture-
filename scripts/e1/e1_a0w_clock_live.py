"""Clock-only live round: record preparation, sealing, verification and the single bounded run (task A0w, 2026-11-22).

Subcommands (every value comes from an argument or from the record; there is no flag that widens the scope and no environment variable is read):

    describe  --distro D --wsl-exe PATH                         text only: the one vector that would be started
    prepare   --distro D --wsl-exe PATH --manifest M --out-dir DIR --record-out R.json
                                                                 builds the UNSIGNED activation record from live observations (reads files and the registry; starts nothing)
    seal      --record R.json --approval-doc DOC --approved-by WHO --sealed-out S.json
                                                                 copies the approval block into a new record file after checking that DOC pins the record's record_sha256
    verify    --record S.json --manifest M                       every preflight gate; starts nothing
    run       --record S.json --manifest M                       the preflight gates, then the bounded round (the only subcommand that starts a process)

Exit codes: 0 ok / round ended; 2 a gate refused (nothing started); 3 round ended with a partial or non-complete outcome.
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
for entry in (str(REPO / "src"),):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from fabric_perf.controllers import clock_only_live as col         # noqa: E402

SCHEDULE, DIAGNOSTIC = col.SCHEDULE, col.DIAGNOSTIC


def _dump(doc) -> str:
    return json.dumps(doc, indent=2, sort_keys=True)


def build_record(distro: str, wsl_exe: str, manifest: Path, out_dir: Path) -> dict:
    return col.build_record(repo_root=REPO, distro=distro, wsl_exe=wsl_exe, manifest=manifest, out_dir=out_dir, node_name=platform.node(), os_version=platform.version(),
                            distros=col.registered_distributions())


def cmd_describe(args) -> int:
    allowed = [col.allowed_vector(args.distro, timeout_s=SCHEDULE["probe_timeout_s"], uses=SCHEDULE["max_process_starts"])]
    print(_dump({"operation": col.OPERATION, "argv": col.spawn_argv(args.wsl_exe, args.distro), "vectors": [a.as_dict() for a in allowed], "schedule": SCHEDULE, "caps": col.CAPS,
                 "nothing_started": True}))
    return 0


def cmd_prepare(args) -> int:
    record = build_record(args.distro, args.wsl_exe, args.manifest, args.out_dir)
    problems = col.check_scope(record["binding"])
    if problems:
        print(_dump({"refused": problems}))
        return 2
    with Path(args.record_out).open("xb") as fh:
        fh.write((_dump(record) + "\n").encode("utf-8"))
    print(_dump({"record": str(args.record_out), "record_sha256": record["record_sha256"], "candidate_manifest_sha256": record["candidate_manifest_sha256"]}))
    return 0


def cmd_seal(args) -> int:
    record = json.loads(Path(args.record).read_text(encoding="utf-8"))
    try:
        sealed = col.seal_record(record, args.approval_doc, args.approved_by)
    except col.ClockOnlyRefused as exc:
        print(_dump({"refused": exc.findings}))
        return 2
    with Path(args.sealed_out).open("xb") as fh:
        fh.write((_dump(sealed) + "\n").encode("utf-8"))
    print(_dump({"sealed": str(args.sealed_out), "record_sha256": sealed["record_sha256"], "approval_document_sha256": sealed["approval"]["record_sha256"]}))
    return 0


def _preflight(args):
    record = json.loads(Path(args.record).read_text(encoding="utf-8"))
    pre = col.preflight(record, repo_root=REPO, manifest_path=Path(args.manifest), node_name=platform.node(), os_version=platform.version(), distros=col.registered_distributions())
    return record, pre


def cmd_verify(args) -> int:
    try:
        _, pre = _preflight(args)
    except col.ClockOnlyRefused as exc:
        print(_dump({"refused": exc.gate, "findings": exc.findings, "nothing_started": True}))
        return 2
    print(_dump({"gates": pre["gates"], "record_sha256": pre["record_sha256"], "manifest_sha256": pre["manifest_sha256"], "nothing_started": True}))
    return 0


def cmd_run(args) -> int:
    try:
        record, pre = _preflight(args)
        status = col.execute(record, pre)
    except col.ClockOnlyRefused as exc:
        print(_dump({"refused": exc.gate, "findings": exc.findings, "nothing_started": True}))
        return 2
    outcome = json.loads((Path(record["binding"]["output_dir"]) / "run_outcome.json").read_text(encoding="utf-8"))
    print(_dump({"status": status["status"], "stop_reason": status["stop_reason"], "process_starts": status["process_starts"], "diagnostic_verdict": status.get("verdict_diagnostic"),
                 "outcome_class": outcome["class"], "output_dir": record["binding"]["output_dir"]}))
    return 0 if outcome["class"] == "REAL_CLOCK_EVIDENCE_COMPLETE" else 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("describe")
    d.add_argument("--distro", required=True)
    d.add_argument("--wsl-exe", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--distro", required=True)
    p.add_argument("--wsl-exe", required=True)
    p.add_argument("--manifest", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--record-out", required=True, type=Path)
    s = sub.add_parser("seal")
    s.add_argument("--record", required=True, type=Path)
    s.add_argument("--approval-doc", required=True, type=Path)
    s.add_argument("--approved-by", required=True)
    s.add_argument("--sealed-out", required=True, type=Path)
    for name in ("verify", "run"):
        q = sub.add_parser(name)
        q.add_argument("--record", required=True, type=Path)
        q.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    return {"describe": cmd_describe, "prepare": cmd_prepare, "seal": cmd_seal, "verify": cmd_verify, "run": cmd_run}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
