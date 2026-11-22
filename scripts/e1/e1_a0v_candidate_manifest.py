"""Candidate integrity manifest, A0v successor (2026-11-22): the A0u candidate files at their CURRENT hashes plus the A0v additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-22_a0u.json`` and every earlier manifest are NOT edited. A0v changed NO file that the A0u manifest lists (no execution gate, no executor, no
pinned tool), so ``changed_since_a0u`` and ``superseded_entries`` are empty (a test recomputes both). A0v adds the WSL transport, the run-outcome classifier, the clock-only transport plan, two
command-line tools and, as verification entries, the A0v test files.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a workload, Caliper,
  Fabric, a probe, a digest, a model or a forecast. Nothing is pinned here. ``N2_STATUS`` records the owner's architecture approval; that is not an approval of this manifest.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0v_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-22_a0v.json
    python scripts/e1/e1_a0v_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-22_a0v.json
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

import e1_a0u_candidate_manifest as a0u                      # noqa: E402  (supplies the A0u entries, exclusions, unresolved items and open items)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-22_a0v.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0u.json"
VERSION = "a0v_candidate_manifest 0.1 (2026-11-22)"

A0V_ADDITIONS = [
    pi.FileSpec("src/fabric_perf/controllers/wsl_transport.py", "operation-scoped WSL process transport: spawn permits, operation-specific activation record, supervised child processes (structured vectors, "
                "bounded capture, own-process termination, no retry), single-category routing; real start-up is NOT compiled in (REAL_SPAWN_COMPILED_IN = False)", "wsl_transport"),
    pi.FileSpec("src/fabric_perf/controllers/run_outcome.py", "independent success verdict: re-reads stored evidence; COMPLETED alone is never success; simulated evidence is never success", "run_outcome"),
    pi.FileSpec("src/fabric_perf/controllers/clock_transport_plan.py", "inspectable clock-only transport plan: exact vector, schedule, parser, stop conditions; every owner or unmeasured value stays unset", "clock_transport_plan"),
    pi.FileSpec("scripts/e1/e1_a0v_transport.py", "CLI: clock-plan, clock-activation-template, classify-bounded, classify-clock; execute is always refused (exit 5)", "a0v_cli"),
    pi.FileSpec("scripts/e1/e1_a0v_candidate_manifest.py", "this manifest's builder and verifier", "a0v_manifest_builder"),
    pi.FileSpec("tests/a0v_support.py", "verification support: process guard, fake Popen, synthetic activation records (not a runtime dependency)", "a0v_test_support"),
    pi.FileSpec("tests/test_a0v_wsl_transport_2026_11_22.py", "verification: transport behavior (not a runtime dependency)", "a0v_test"),
    pi.FileSpec("tests/test_a0v_authorization_2026_11_22.py", "verification: operation-specific authorization and the independent gates (not a runtime dependency)", "a0v_test"),
    pi.FileSpec("tests/test_a0v_integrity_outcome_2026_11_22.py", "verification: X13 integrity enforcement and the success verdict (not a runtime dependency)", "a0v_test"),
    pi.FileSpec("tests/test_a0v_clock_plan_2026_11_22.py", "verification: clock-only plan, A0e compatibility (not a runtime dependency)", "a0v_test"),
    pi.FileSpec("tests/test_a0v_safety_2026_11_22.py", "verification: safety and static-source checks (not a runtime dependency)", "a0v_test"),
    pi.FileSpec("tests/test_a0v_hardening_2026_11_22.py", "verification: hardening tests written after a mutation check (not a runtime dependency)", "a0v_test"),
    pi.FileSpec("tests/test_a0v_manifest_owner_2026_11_22.py", "verification: this manifest, the owner records and the frozen / historical hashes (not a runtime dependency)", "a0v_test"),
]
A0V_NEW_PATHS = sorted(spec.path for spec in A0V_ADDITIONS)

EXCLUSIONS = tuple(a0u.EXCLUSIONS) + (
    {"item": "docs/research/e1/a0v_example_artifacts_2026-11-22/ (generator and templates), docs/research/e1/a0v_verification_evidence_2026-11-22/ and the A0v report and packets",
     "reason": "documentation and evidence, not runtime dependencies. The A0v test files are listed above as verification entries; they are not needed to run anything."},
)
UNRESOLVED = tuple(a0u.UNRESOLVED) + (
    "A0v: the real WSL transport is implemented and tested OFFLINE ONLY (fixture mode with harmless local commands, and real mode over an injected fake Popen). That wsl.exe --cd/--exec behaves as the vectors "
    "assume, that `date +%s%N` prints nanoseconds in the distribution, and that terminating the wsl.exe process ends its descendants inside the distribution are UNVERIFIED.",
    "A0v: activating a real transport needs, independently, an operation-specific activation record pinned by the owner's approval document, wsl_transport.REAL_SPAWN_COMPILED_IN, "
    "process_runner.REAL_TRANSPORT_ENABLED, wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED (clock) or bounded_execution.REAL_LAUNCH_ENABLED with PERMITTED_RUNNER_KINDS (bounded runs), and a reviewed change of the "
    "executor's transport check. None of these was changed.",
    "A0v: the explicit absolute path of wsl.exe, the distribution name and the host identity are owner decisions and unset; the clock tolerance, counts, gap, timeout and lifetime are unset and unmeasured.",
    "A0v: the A0s import chain loads http.client, ssl, socket and urllib.request (library imports; no connection is made). The A0v modules add nothing to it (a test asserts this).",
)
OPEN_ITEMS = tuple(a0u.OPEN_ITEMS) + (
    "A0v: owner approval of the A0v additions has not been given. N2 is ARCHITECTURE_APPROVED_RUNTIME_NOT_VERIFIED (the owner's A0v instruction); this manifest is still CANDIDATE_NOT_APPROVED.",
    "A0v: the activation record schema (a0v_wsl_operation_activation/1), the transport evidence (a0v_wsl_transport_evidence/1), the run outcome (a0v_run_outcome/1) and the clock transport plan "
    "(a0v_clock_transport_plan/1) are PROPOSED engineering schemas.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    base = a0u.build(repo, created_utc=created_utc)                                   # the A0u manifest rebuilt from the files as they are NOW
    new = pi.build_candidate_manifest(repo, A0V_ADDITIONS, created_utc=created_utc)["entries"]
    manifest = dict(base)
    manifest["entries"] = list(base["entries"]) + new
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    for stale in ("changed_since_a0t", "added_since_a0t", "removed_since_a0t", "superseded_entries", "superseded_note"):
        manifest.pop(stale, None)
    manifest["changed_since_a0u"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0u"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0u"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0u_sha256": earlier[p], "a0v_sha256": current[p], "reason": "UNEXPLAINED: A0v changes no A0u candidate file; add a reason"} for p in manifest["changed_since_a0u"]]
    manifest["superseded_note"] = "A0v changed no file listed in the A0u manifest (no execution gate, executor or pinned tool was edited), so nothing is superseded."
    schema_versions = dict(base["schema_versions"])
    schema_versions.update({"a0v_wsl_operation_activation": "a0v_wsl_operation_activation/1 (new, proposed)", "a0v_wsl_transport_evidence": "a0v_wsl_transport_evidence/1 (new, proposed)",
                            "a0v_run_outcome": "a0v_run_outcome/1 (new, proposed)", "a0v_clock_transport_plan": "a0v_clock_transport_plan/1 (new, proposed)"})
    manifest["schema_versions"] = schema_versions
    manifest["architecture_recommendation"] = {
        "N2_STATUS": "ARCHITECTURE_APPROVED_RUNTIME_NOT_VERIFIED", "option": "A: Windows-host orchestration with WSL-side hashing (WINDOWS_HOST_ORCHESTRATION_WITH_WSL_SIDE_HASHING)",
        "execution_environment": "windows_host_orchestrator_with_wsl_steps", "integrity_environment": "wsl_side_digest_via_wsl_exec", "a0s_a0t_a0u_files_changed": False,
        "approval_source": "the owner's A0v task instruction of 2026-11-22 (architecture only)", "manifest_approved": False,
        "why": "keeps the Windows reference clock of the A0e clock contract, changes no pinned file, and adds one read-only vector per phase; see the A0u decision record and the A0v approval record"}
    manifest["launch_configuration"] = {
        "manifest_tool": VERSION, "supersedes": SUPERSEDES, "approved": False, "live_run": False,
        "entry_points": {**base["launch_configuration"]["entry_points"],
                         "transport_cli": "scripts/e1/e1_a0v_transport.py (clock-plan, clock-activation-template, classify-bounded, classify-clock; execute REFUSED)",
                         "execute": "none; real launch and the real transport are disabled (wsl_transport.REAL_SPAWN_COMPILED_IN, process_runner.REAL_TRANSPORT_ENABLED, bounded_execution.REAL_LAUNCH_ENABLED, "
                                    "wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED are all False)"},
        "variants": base["launch_configuration"]["variants"]}
    manifest["runtime_dependencies"] = {**base["runtime_dependencies"], "process_api_imported_by_a0v_files": "subprocess and threading, in src/fabric_perf/controllers/wsl_transport.py only (behind the permit checks)",
                                        "network_or_container_api_imported_by_a0v_files": False,
                                        "a0v_new_imports": ["hashlib", "json", "os", "re", "signal", "subprocess", "sys", "threading", "time", "dataclasses", "datetime", "pathlib", "typing"],
                                        "wsl_side_runtime": base["runtime_dependencies"].get("wsl_side_runtime")}
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
              f"changed since A0u: {len(manifest['changed_since_a0u'])}; added: {len(manifest['added_since_a0u'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
