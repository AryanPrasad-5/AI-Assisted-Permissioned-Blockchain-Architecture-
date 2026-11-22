"""Candidate integrity manifest, A0u successor (2026-11-22): the A0t candidate files at their CURRENT hashes plus the A0u additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-22_a0t.json`` and every earlier manifest are NOT edited. A0u changed NO file that the A0t manifest lists, so ``changed_since_a0t`` and
``superseded_entries`` are empty (a test recomputes both). A0u adds seven new files: the WSL-capable tree-digest library, the X13 integrity adapter, the runtime-discovery module, the clock-only
readiness module, the WSL-side digest script, the A0u CLI and this builder.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a workload,
  Caliper, Fabric, a probe, a digest, a model or a forecast. Nothing is pinned here. The execution-environment recommendation recorded below is a RECOMMENDATION (N2_STATUS = RECOMMENDED_NOT_APPROVED).
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0u_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-22_a0u.json
    python scripts/e1/e1_a0u_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-22_a0u.json
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

import e1_a0t_candidate_manifest as a0t                      # noqa: E402  (supplies the A0t entries, exclusions, unresolved items and open items)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-22_a0u.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0t.json"
VERSION = "a0u_candidate_manifest 0.1 (2026-11-22)"

A0U_ADDITIONS = [
    pi.FileSpec("src/fabric_perf/controllers/tree_digest.py", "deterministic file / directory-tree digest (a0u_tree_digest/1), stdlib only, Python 3.9 compatible; runs inside the WSL distribution "
                "(loaded by file path by the digest script) and on the Windows side to re-verify what came back", "tree_digest"),
    pi.FileSpec("scripts/e1/e1_wsl_tree_digest.py", "WSL-side read-only digest entry point for the X13 repair: one JSON document on stdout, reads WSL_DISTRO_NAME for attestation only", "wsl_tree_digest_tool"),
    pi.FileSpec("src/fabric_perf/controllers/wsl_integrity.py", "X13 repair: integrity addendum bound to the plan, a0u authorization extension, plan-bound runner hook (pre-workload digest after staging, "
                "post-workload digest at X13), fail-closed verdict; contains no process launcher", "wsl_integrity"),
    pi.FileSpec("src/fabric_perf/controllers/runtime_discovery.py", "Caliper container identity declarations scanned from the repository, the read-only runtime-discovery specification (not executed) "
                "and the evaluation of a future discovery", "runtime_discovery"),
    pi.FileSpec("src/fabric_perf/controllers/clock_only_readiness.py", "clock-only transaction-freedom audit, A0e compatibility assessment and the unapproved A0u clock-only approval packet", "clock_only_readiness"),
    pi.FileSpec("scripts/e1/e1_a0u_integrity.py", "CLI: addendum, verify-addendum, authorization-template, verify-authorization, discovery-spec, clock-only-audit, clock-only-packet; execute is always refused (exit 5)",
                "a0u_cli"),
    pi.FileSpec("scripts/e1/e1_a0u_candidate_manifest.py", "this manifest's builder and verifier", "a0u_manifest_builder"),
]
A0U_NEW_PATHS = sorted(spec.path for spec in A0U_ADDITIONS)

EXCLUSIONS = tuple(a0t.EXCLUSIONS) + (
    {"item": "tests/a0u_support.py, the three tests/test_a0u_*_2026_11_22.py files, docs/research/e1/a0u_example_artifacts_2026-11-22/ and the A0u report and packets",
     "reason": "verification and documentation, not runtime dependencies. No library or CLI listed here can start a process; the test support only simulates a WSL file system in a temporary directory."},
)
X13_A0T_ITEM = "The X13 integrity step hashes the worker evidence root"        # replaced by the A0u item below (repaired offline; runtime verification still open)
UNRESOLVED = tuple(item for item in a0t.UNRESOLVED if not item.startswith(X13_A0T_ITEM)) + (
    "A0u: the X13 defect is repaired ONLY at the design and offline-test level. No real digest was produced; that python3 and the repository are visible inside the distribution, that WSL_DISTRO_NAME "
    "equals the authorized distribution, that the staged run directory holds exactly the authorized overlay, and that it holds no symlink or unexpected empty directory are UNVERIFIED at runtime.",
    "A0u: the Caliper container to use is UNRESOLVED. The rendered plan names 'caliper'; the benchmark scripts name 'caliper-benchmark-001'. Both were recorded as existing and Exited(255) on 2026-11-17 "
    "(dated); neither was re-observed. Which container exists now, which has the bound SUT packages, and its workspace mount need a separately authorized read-only discovery.",
    "A0u: the Windows-to-WSL clock offset, round trip and any tolerance are unmeasured; no value is proposed.",
)
OPEN_ITEMS = tuple(a0t.OPEN_ITEMS) + (
    "A0u: owner approval of the seven A0u additions has not been given. N2 is RECOMMENDED_NOT_APPROVED (Windows-host orchestrator with WSL-side integrity digest). Nothing was started.",
    "A0u: the a0u authorization block (a0u_integrity_authorization/1), the integrity addendum, the verdict, the runtime-discovery specification and the clock-only approval packet are PROPOSED engineering schemas.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    base = a0t.build(repo, created_utc=created_utc)                                   # the A0t manifest rebuilt from the files as they are NOW
    new = pi.build_candidate_manifest(repo, A0U_ADDITIONS, created_utc=created_utc)["entries"]
    manifest = dict(base)
    manifest["entries"] = list(base["entries"]) + new
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    for stale in ("changed_since_a0s", "added_since_a0s", "removed_since_a0s", "superseded_entries", "superseded_note"):
        manifest.pop(stale, None)
    manifest["changed_since_a0t"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0t"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0t"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0t_sha256": earlier[p], "a0u_sha256": current[p], "reason": "UNEXPLAINED: A0u changes no A0t candidate file; add a reason"} for p in manifest["changed_since_a0t"]]
    manifest["superseded_note"] = "A0u changed no file listed in the A0t manifest, so nothing is superseded."
    schema_versions = dict(base["schema_versions"])
    schema_versions.update({"a0u_tree_digest": "a0u_tree_digest/1 (new, proposed)", "a0u_tree_digest_evidence": "a0u_tree_digest_evidence/1 (new, proposed)",
                            "a0u_integrity_addendum": "a0u_integrity_addendum/1 (new, proposed)",
                            "a0u_integrity_authorization": "a0u_integrity_authorization/1 (new, proposed; an 'a0u' block inside a0s_bounded_launch_authorization/1)",
                            "a0u_integrity_verdict": "a0u_integrity_verdict/1 (new, proposed)", "a0u_runtime_discovery_spec": "a0u_runtime_discovery_spec/1 (new, proposed)",
                            "a0u_clock_only_approval_packet": "a0u_clock_only_approval_packet/1 (new, proposed)"})
    manifest["schema_versions"] = schema_versions
    manifest["architecture_recommendation"] = {
        "N2_STATUS": "RECOMMENDED_NOT_APPROVED", "option": "A: Windows-host orchestration with a WSL-side integrity digest", "execution_environment": "windows_host_orchestrator_with_wsl_steps",
        "integrity_environment": "wsl_side_digest_via_wsl_exec", "a0s_a0t_files_changed": False,
        "why": "keeps the Windows reference clock of the A0e clock contract, changes no pinned file, and adds one read-only vector per phase; see the A0u decision record"}
    manifest["launch_configuration"] = {
        "manifest_tool": VERSION, "supersedes": SUPERSEDES, "approved": False, "live_run": False,
        "entry_points": {**base["launch_configuration"]["entry_points"],
                         "integrity_cli": "scripts/e1/e1_a0u_integrity.py (addendum, verify-addendum, authorization-template, verify-authorization, discovery-spec, clock-only-audit, clock-only-packet; execute REFUSED)",
                         "wsl_side_digest": "scripts/e1/e1_wsl_tree_digest.py (digest; read-only; intended only as a structured wsl.exe --exec vector)",
                         "execute": "none; real launch and the real transport are disabled (bounded_execution.REAL_LAUNCH_ENABLED = False, process_runner.REAL_TRANSPORT_ENABLED = False)"},
        "variants": base["launch_configuration"]["variants"]}
    manifest["runtime_dependencies"] = {**base["runtime_dependencies"], "process_or_network_api_imported_by_a0u_files": False,
                                        "a0u_new_imports": ["hashlib", "json", "posixpath", "re", "stat", "pathlib", "ast", "argparse", "importlib.util", "datetime", "sys (script only)", "os (script only: WSL_DISTRO_NAME)"],
                                        "wsl_side_runtime": "python3 inside the distribution (version unverified; tool kept Python 3.9 compatible), the repository checkout under the declared /mnt path, no third-party package"}
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
              f"changed since A0t: {len(manifest['changed_since_a0t'])}; added: {len(manifest['added_since_a0t'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
