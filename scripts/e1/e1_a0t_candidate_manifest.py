"""Candidate integrity manifest, A0t successor (2026-11-22): the A0s candidate files at their CURRENT hashes plus the A0t additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-22_a0s.json`` and every earlier manifest are NOT edited. A0t changed NO file that the A0s manifest lists, so ``changed_since_a0s`` and
``superseded_entries`` are empty (a test recomputes both). A0t adds six new files: the path-mapping library, the plan-bound process-runner adapter, the clock-only round, the capacity-bracket design
builder, the process-runner CLI and this builder.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a workload,
  Caliper, Fabric, a probe, a model or a forecast. Nothing is pinned here.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0t_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-22_a0t.json
    python scripts/e1/e1_a0t_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-22_a0t.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
for entry in (str(HERE), str(REPO / "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

import e1_a0s_candidate_manifest as a0s                      # noqa: E402  (supplies the A0s specs, exclusions, unresolved items and open items)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-22_a0t.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0s.json"
VERSION = "a0t_candidate_manifest 0.1 (2026-11-22)"

A0T_ADDITIONS = [
    pi.FileSpec("src/fabric_perf/controllers/path_mapping.py", "Windows <-> WSL <-> container path mapping with explicit automount root, verification levels and a machine-readable path record "
                "(highest offline level REPO_DECLARATION_CROSS_CHECKED; runtime verification unreachable)", "path_mapping"),
    pi.FileSpec("src/fabric_perf/controllers/process_runner.py", "plan-bound, WSL-aware runner adapter over an injected transport: structured argument vectors, plan-vector table, no retry, "
                "A0t authorization extension; the real transport is a disabled stub (REAL_TRANSPORT_ENABLED = False); contains no process launcher", "process_runner"),
    pi.FileSpec("src/fabric_perf/controllers/clock_only_round.py", "transaction-free clock-only round: request, plan, authorization record, fixture-only execution (the real probe stays disabled)", "clock_only_round"),
    pi.FileSpec("src/fabric_perf/controllers/capacity_bracket.py", "capacity-bracket experiment design (PROPOSED, every owner parameter None), schedule builder, closed-form signed-rank bound", "capacity_bracket"),
    pi.FileSpec("scripts/e1/e1_a0t_process_runner.py", "CLI: inspect-invocations, path-record, authorization-template, verify-authorization, clock-only-*, capacity-*; execute is always refused (exit 5)",
                "process_runner_cli"),
    pi.FileSpec("scripts/e1/e1_a0t_candidate_manifest.py", "this manifest's builder and verifier", "a0t_manifest_builder"),
]
A0T_NEW_PATHS = sorted(spec.path for spec in A0T_ADDITIONS)

EXCLUSIONS = tuple(a0s.EXCLUSIONS) + (
    {"item": "tests/a0t_support.py, the five tests/test_a0t_*_2026_11_22.py files, docs/research/e1/a0t_example_artifacts_2026-11-22/ and the A0t report",
     "reason": "verification and documentation, not runtime dependencies; hashed in the A0t evidence SHA256SUMS, not listed here. tests/a0t_support.py contains the only code that starts processes "
              "(harmless local Python one-liners through FixtureTransport); no library or CLI listed here can."},
)
UNRESOLVED = tuple(a0s.UNRESOLVED) + (
    "A0t never ran a real process transport, wsl.exe, Docker, Caliper, Fabric, a Windows clock sample or Prometheus. Every wsl.exe vector, --cd/--exec behaviour and mapped path is text; "
    "that they work on the target machine, that /mnt is the automount root inside the distribution, and that the Caliper container exists with the declared bind mount are UNVERIFIED at runtime.",
    "The X13 integrity step hashes the worker evidence root, a WSL-native directory (/home/<user>/...), from the Windows host; no Windows path or supported UNC form exists for it. Either the orchestrator "
    "runs inside WSL or a read-only WSL-side hash vector must be added to the plan (new plan version). Not implemented.",
    "The capacity-bracket design is PROPOSED; every owner parameter is None; no run is authorized and no series-level authorization mechanism exists (bounded_execution authorizes one run).",
)
OPEN_ITEMS = tuple(a0s.OPEN_ITEMS) + (
    "A0t: owner approval of the six A0t additions has not been given. No process transport, WSL, Docker, Caliper, Fabric, Prometheus, model or actuator was started.",
    "A0t: the A0t authorization extension (a0t_process_authorization/1), the clock-only schemas and the capacity-bracket schemas are PROPOSED engineering schemas awaiting an owner decision.",
    "A0t: execution environment (Windows-host orchestrator with wsl.exe steps, as implemented, versus an orchestrator inside WSL, the repository's earlier convention) is an owner decision (N2).",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    base = a0s.build(repo, created_utc=created_utc)                                   # the A0s manifest rebuilt from the files as they are NOW
    new = pi.build_candidate_manifest(repo, A0T_ADDITIONS, created_utc=created_utc)["entries"]
    manifest = dict(base)
    manifest["entries"] = list(base["entries"]) + new
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    for stale in ("changed_since_a0q", "added_since_a0q", "removed_since_a0q"):
        manifest.pop(stale, None)
    manifest["changed_since_a0s"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0s"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0s"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0s_sha256": earlier[p], "a0t_sha256": current[p], "reason": "UNEXPLAINED: A0t changes no A0s candidate file; add a reason"}
                                      for p in manifest["changed_since_a0s"]]
    manifest["superseded_note"] = "A0t changed no file listed in the A0s manifest, so nothing is superseded."
    schema_versions = dict(base["schema_versions"])
    schema_versions.update({"a0t_path_mapping_record": "a0t_path_mapping_record/1 (new, proposed)", "a0t_planned_invocations": "a0t_planned_invocations/1 (new, proposed)",
                            "a0t_process_authorization": "a0t_process_authorization/1 (new, proposed; an 'a0t' block inside a0s_bounded_launch_authorization/1)",
                            "a0t_clock_only_request": "a0t_clock_only_request/1 (new, proposed)", "a0t_clock_only_plan": "a0t_clock_only_plan/1 (new, proposed)",
                            "a0t_clock_only_authorization": "a0t_clock_only_authorization/1 (new, proposed)", "a0t_clock_only_status": "a0t_clock_only_status/1 (new, proposed)",
                            "a0t_capacity_bracket_design": "a0t_capacity_bracket_design/1 (new, proposed)", "a0t_capacity_bracket_schedule": "a0t_capacity_bracket_schedule/1 (new, proposed)"})
    manifest["schema_versions"] = schema_versions
    manifest["launch_configuration"] = {
        "manifest_tool": VERSION, "supersedes": SUPERSEDES, "approved": False, "live_run": False,
        "entry_points": {**base["launch_configuration"]["entry_points"],
                         "process_runner": "scripts/e1/e1_a0t_process_runner.py (inspect-invocations, path-record, authorization-template, verify-authorization, clock-only-plan/template/verify, "
                                           "capacity-design, capacity-schedule; execute REFUSED)",
                         "execute": "none; real launch and the real transport are disabled (bounded_execution.REAL_LAUNCH_ENABLED = False, process_runner.REAL_TRANSPORT_ENABLED = False)"},
        "variants": base["launch_configuration"]["variants"]}
    manifest["runtime_dependencies"] = {**base["runtime_dependencies"], "process_or_network_api_imported_by_a0t_files": False, "a0t_new_imports": ["hashlib", "json", "posixpath", "random", "re"]}
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    manifest["open_items"] = list(OPEN_ITEMS)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    write = sub.add_parser("write", help="write the successor manifest to a path that does not exist")
    write.add_argument("--out", required=True, type=Path)
    verify = sub.add_parser("verify", help="compare a candidate manifest with the files on disk (read-only)")
    verify.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.phase == "write":
        try:
            manifest = build()
            digest = pi.write_new_json(args.out, manifest)
        except (OSError, ValueError) as exc:
            print(f"REFUSED: {exc}")
            return 2
        print(f"candidate manifest -> {args.out} ({len(manifest['entries'])} files, sha256 {digest}); status {manifest['status']}; "
              f"changed since A0s: {len(manifest['changed_since_a0s'])}; added: {len(manifest['added_since_a0s'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
