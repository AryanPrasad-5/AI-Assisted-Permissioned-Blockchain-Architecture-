"""Candidate integrity manifest, A0w successor (2026-11-22): the A0v candidate files at their CURRENT hashes plus the A0w clock-only live route.

``docs/research/e1/prelive_candidate_manifest_2026-11-22_a0v.json`` and every earlier manifest are NOT edited. A0w adds files and changes none that the A0v manifest lists, so
``changed_since_a0v`` and ``superseded_entries`` are expected to be empty (the builder refuses to write a manifest in which an A0v file changed without a stated reason).

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. Listing a file is not approval. The clock-only activation record binds THIS file's SHA-256, which scopes the
  owner's authorization to exactly these hashes for one clock round; it does not approve the manifest for any other operation.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0w_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-22_a0w.json
    python scripts/e1/e1_a0w_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-22_a0w.json
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

import e1_a0v_candidate_manifest as a0v                      # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0v.json"
VERSION = "a0w_candidate_manifest 0.1 (2026-11-22)"

A0W_ADDITIONS = [
    pi.FileSpec("src/fabric_perf/controllers/clock_only_live.py", "dedicated single-operation live route for the Windows<->WSL clock round: hard-wired to one vector (explicit wsl.exe -d <distro> --exec date +%s%N), "
                "gated by the A0v activation record, a live binding (host, wsl.exe hash, registered distribution, source hashes, manifest hash), hard caps and an exclusive-create evidence directory; "
                "leaves the three A0v global switches False", "a0w_clock_live"),
    pi.FileSpec("scripts/e1/e1_a0w_clock_live.py", "CLI: describe, prepare, seal, verify, run. run is the only subcommand that starts a process; no flag or environment variable widens the scope", "a0w_cli"),
    pi.FileSpec("scripts/e1/e1_a0w_candidate_manifest.py", "this manifest's builder and verifier", "a0w_manifest_builder"),
    pi.FileSpec("tests/test_a0w_clock_live_2026_11_22.py", "verification: A0w clock-only route safety and behavior, synthetic only (not a runtime dependency)", "a0w_test"),
]
EXCLUSIONS = tuple(a0v.EXCLUSIONS) + (
    {"item": "docs/research/e1/a0w_runtime_evidence_2026-11-22/ and the A0w report, owner-decision packet update, authorization record and approval document",
     "reason": "evidence and records produced by or for the clock round, not runtime dependencies. The activation record and the approval document bind this manifest's hash; they are not listed in it."},
)
UNRESOLVED = tuple(a0v.UNRESOLVED) + (
    "A0w: the clock-only route is the only code path that can start wsl.exe. wsl_transport.REAL_SPAWN_COMPILED_IN, process_runner.REAL_TRANSPORT_ENABLED, wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED and "
    "bounded_execution.REAL_LAUNCH_ENABLED stay False, so the A0v transport, the A0s/A0t executors and every other operation category remain disabled.",
    "A0w: the activation record, the approval document and the A0w evidence schemas (a0w_clock_*) are PROPOSED engineering artifacts. The clock evaluator parameters used by the round are diagnostic and unapproved.",
)
OPEN_ITEMS = tuple(a0v.OPEN_ITEMS) + (
    "A0w: owner approval of this manifest as a candidate set has not been given. The clock round's authorization is scoped to one session and binds these hashes; it approves no tolerance and no other operation.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    base = a0v.build(repo, created_utc=created_utc)
    new = pi.build_candidate_manifest(repo, A0W_ADDITIONS, created_utc=created_utc)["entries"]
    manifest = dict(base)
    manifest["entries"] = list(base["entries"]) + new
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    for stale in ("changed_since_a0u", "added_since_a0u", "removed_since_a0u", "superseded_entries", "superseded_note"):
        manifest.pop(stale, None)
    manifest["changed_since_a0v"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0v"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0v"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0v_sha256": earlier[p], "a0w_sha256": current[p], "reason": "UNEXPLAINED: A0w changes no A0v candidate file; add a reason"} for p in manifest["changed_since_a0v"]]
    manifest["superseded_note"] = "A0w changed no file listed in the A0v manifest (no execution gate, executor, transport or pinned tool was edited), so nothing is superseded."
    schema_versions = dict(base["schema_versions"])
    schema_versions.update({"a0w_clock_round_status": "a0w_clock_round_status/1 (new, proposed)", "a0w_run_outcome": "a0w_run_outcome/1 (new, proposed)",
                            "a0w_clock_analysis": "a0w_clock_analysis/1 (new, proposed)", "a0w_clock_integrity": "a0w_clock_integrity/1 (new, proposed)"})
    manifest["schema_versions"] = schema_versions
    manifest["launch_configuration"] = {
        "manifest_tool": VERSION, "supersedes": SUPERSEDES, "approved": False, "live_run": False,
        "entry_points": {**base["launch_configuration"]["entry_points"],
                         "clock_only_live": "scripts/e1/e1_a0w_clock_live.py (describe, prepare, seal, verify, run); run starts at most 40 'wsl.exe -d <distro> --exec date +%s%N' processes, one at a time, and nothing else",
                         "execute": "none for any other operation; the A0v global switches stay False"},
        "variants": base["launch_configuration"]["variants"]}
    manifest["runtime_dependencies"] = {**base["runtime_dependencies"],
                                        "process_api_imported_by_a0w_files": "subprocess, in src/fabric_perf/controllers/clock_only_live.py only (one subprocess.run call, behind the gates)",
                                        "network_or_container_api_imported_by_a0w_files": False}
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
            if manifest["changed_since_a0v"]:
                raise ValueError(f"A0v candidate files changed: {manifest['changed_since_a0v']}")
            digest = pi.write_new_json(args.out, manifest)
        except (OSError, ValueError) as exc:
            print(f"REFUSED: {exc}")
            return 2
        print(f"candidate manifest -> {args.out} ({len(manifest['entries'])} files, sha256 {digest}); status {manifest['status']}; "
              f"changed since A0v: {len(manifest['changed_since_a0v'])}; added: {len(manifest['added_since_a0v'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
