"""Candidate integrity manifest, A0q successor (2026-11-22): the A0p candidate files at their CURRENT hashes plus the A0q additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-22_a0p.json`` and every earlier manifest are NOT edited. A0q changed NO file that the A0p manifest lists (the A0o reader driver, the A0l
coverage bridge and tail reader, ``collector_health.py``, the launch-variant tool, the pinned renderer and every A0e to A0p source are byte-identical), so ``changed_since_a0p`` and
``superseded_entries`` are empty. A0q adds three runtime/evidence files (the generator-route recovery module, the crash-matrix tool and this builder) and three verification files that
the guard depends on (the syntax-aware recorder-v3 selection guard, the A0j integrity test whose ``test_42`` now calls it, and the A0m integrity test whose ``test_35`` records the resulting hash
supersession). The two changed verification files are pinned in the frozen A0m evidence sums; their old and new hashes are recorded in ``changed_verification_files``.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a workload,
  a recorder, a bridge, Caliper, Fabric, a model or a forecast. Nothing is pinned here.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0q_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-22_a0q.json
    python scripts/e1/e1_a0q_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-22_a0q.json
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

import e1_a0p_candidate_manifest as a0p                      # noqa: E402  (supplies the A0p specs, exclusions, unresolved items and open items)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-22_a0q.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0p.json"
VERSION = "a0q_candidate_manifest 0.1 (2026-11-22)"

A0Q_ADDITIONS = [
    pi.FileSpec("src/fabric_perf/controllers/issue_log_bridge_recovery.py", "generator-route bridge restart recovery: deterministic re-simulation of the unmodified IssueLogJournalBridge at "
                "the original read times over a time-limited tail view, exact-prefix comparison with the journal, completion of provably missing rows, state transplant; "
                "RecoverableSourceCountReader", "generator_bridge_recovery"),
    pi.FileSpec("scripts/e1/e1_a0q_crash_matrix.py", "offline synthetic crash-matrix runner for the generator-route recovery (writes a new evidence file)", "a0q_crash_matrix_tool"),
    pi.FileSpec("scripts/e1/e1_a0q_candidate_manifest.py", "this manifest's builder and verifier", "a0q_manifest_builder"),
    pi.FileSpec("tests/recorder_v3_selection_guard.py", "syntax-aware recorder-v3 selection guard (AST scan of Python, comment-stripped scan of JS/shell/YAML, structural scan of JSON); "
                "scan_tree(root) is reusable on a synthetic tree", "recorder_v3_guard_library"),
    pi.FileSpec("tests/test_a0j_integrity_2026_11_21.py", "A0j integrity tests; test_42 now calls the syntax-aware guard (CHANGED in A0q, see changed_verification_files)",
                "a0j_integrity_test_changed"),
    pi.FileSpec("tests/test_a0m_integrity_2026_11_21.py", "A0m integrity tests; test_35 accepts a documented, hash-exact supersession of a pinned verification file (CHANGED in A0q)",
                "a0m_integrity_test_changed"),
]
A0Q_NEW_PATHS = sorted(spec.path for spec in A0Q_ADDITIONS)
A0Q_SPECS = list(a0p.A0P_SPECS) + A0Q_ADDITIONS

EXCLUSIONS = tuple(a0p.EXCLUSIONS) + (
    {"item": "tests/a0q_support.py and the four tests/test_a0q_*_2026_11_22.py files",
     "reason": "tests are verification, not runtime dependencies; they are hashed in the A0q report, not listed here (the guard library and the changed A0j integrity test ARE listed, because a guard "
               "is a dependency of the integrity checks)"},
    {"item": "docs/research/e1/a0q_example_artifacts_2026-11-22/ (crash matrix results, mutation results, SHA256SUMS) and the A0q report",
     "reason": "generated evidence and documentation of this task, not runtime dependencies; hashed in SHA256SUMS.txt"},
)
UNRESOLVED = tuple(a0p.UNRESOLVED) + (
    "The generator-route recovery was exercised only on synthetic issue logs, scripted read times and scripted process deaths. It has not run in a real Caliper worker, on the target "
    "(bind-mounted) filesystem, with a real clock, or under real load.",
    "Recovery cannot distinguish several polls at one read time from one (the checkpoint merges them); such a journal, a fragment written by an idle poll's sync, a restart after an integrity "
    "failure and a journal the checkpoint cannot explain are refused, not guessed.",
    "Rows completed after a restart carry the ORIGINAL read time as t_utc; a decision actually made before the restart used the shorter journal (its sha256 is in the recovery record). "
    "The Stage 3 history stores no journal digest per decision, so a later re-evaluation at the same cutoff sees the completed rows.",
    "Power loss, a storage device that misreports a flush and a second concurrent bridge over the same files are not covered; no lock is taken.",
)
OPEN_ITEMS = tuple(a0p.OPEN_ITEMS) + (
    "A0q: owner approval of the five A0q additions has not been given. No Caliper, Fabric, Docker, WSL, Prometheus, model or actuator was started.",
    "A0q: whether the legacy SourceCountReader.restart (still refusing) should be replaced by the recoverable class in any launch path is an owner decision; nothing selects the new class by default.",
    "A0q: the recorder-v3 guard registers four Python files by name and role; adding a fifth is a deliberate change to tests/recorder_v3_selection_guard.py.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None, old_hashes: dict[str, str] | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0Q_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES,
                              "entry_points": {"generator_recovery": "fabric_perf.controllers.issue_log_bridge_recovery.RecoverableSourceCountReader (explicit class; the legacy reader is unchanged)",
                                               "crash_matrix": "scripts/e1/e1_a0q_crash_matrix.py (offline, synthetic, writes a new evidence file)",
                                               "execute": "none; nothing in A0q launches a run"},
                              "variants": {"a0e_recorder_v2_legacy_workload": "historical; unchanged", "a0m_recorder_v3_issue_logged": "A0o; unchanged",
                                           "a0p_recorder_v3_timed_issue_log": "A0p; unchanged"},
                              "approved": False, "live_run": False},
        experimental_configuration_provenance=a0p.build(repo, created_utc=created_utc).get("experimental_configuration_provenance", {}))
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    manifest["changed_since_a0p"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0p"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0p"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0p_sha256": earlier[p], "a0q_sha256": current[p], "reason": "UNEXPLAINED: A0q changes no A0p candidate file; add a reason"}
                                      for p in manifest["changed_since_a0p"]]
    manifest["superseded_note"] = ("A0q changed no file listed in the A0p manifest, so nothing is superseded. sourcecount_reader.py, issue_log_coverage.py, issue_log_reader.py, "
                                   "collector_health.py and launch_variant.py are preserved byte-for-byte; the new recovery module is a separate file selected explicitly.")
    old_hashes = dict(old_hashes or {})
    reasons = {"tests/test_a0j_integrity_2026_11_21.py": ("test_42_nothing_selects_recorder_v3_implicitly now calls tests/recorder_v3_selection_guard.scan_tree instead of a filename/text scan "
                                                          "with a fixed allow-list", "the old scan failed on five later files (A0n/A0o) that name recorder v3 without selecting it"),
               "tests/test_a0m_integrity_2026_11_21.py": ("test_35 accepts a drift of a verification file from its A0m-era hash only when this manifest records exactly that old and that new hash",
                                                          "the A0m evidence sums pin the A0j integrity test, which A0q had to change; the A0m integrity test is itself pinned there, so it is listed too")}
    manifest["changed_verification_files"] = [
        {"path": path, "old_sha256": old_hashes.get(path), "new_sha256": current[path], "change": reasons[path][0], "reason": reasons[path][1],
         "pinned_in": "Integration/system_integration/a0m_2026-11-21/SHA256SUMS.txt (frozen evidence, left unchanged)"} for path in sorted(reasons)]
    manifest["schema_versions"] = dict(a0p.build(repo, created_utc=created_utc)["schema_versions"])
    manifest["schema_versions"]["generator_bridge_recovery_record"] = "generator_bridge_recovery/1 (new, proposed; one self-digested JSON line per recovery)"
    manifest["schema_versions"]["crash_matrix_results"] = "a0q_crash_matrix_results/1 (new evidence file)"
    manifest["runtime_dependencies"] = {"python_stdlib_only": True, "node_modules_required_by_new_js": [],
                                        "imports_unmodified": ["issue_log_coverage", "issue_log_reader", "sourcecount_reader", "source_completeness", "workload_coverage", "collector_health"]}
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0o", "changed_since_a0o", "removed_since_a0o", "superseded_note_a0o"):
        manifest.pop(key, None)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    write = sub.add_parser("write", help="write the successor manifest to a path that does not exist")
    write.add_argument("--out", required=True, type=Path)
    write.add_argument("--old-hash", action="append", default=[], help="PATH=SHA256 of a changed verification file before A0q (repeatable)")
    verify = sub.add_parser("verify", help="compare a candidate manifest with the files on disk (read-only)")
    verify.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.phase == "write":
        try:
            manifest = build(old_hashes=dict(item.split("=", 1) for item in args.old_hash))
            digest = pi.write_new_json(args.out, manifest)
        except (OSError, ValueError) as exc:
            print(f"REFUSED: {exc}")
            return 2
        print(f"candidate manifest -> {args.out} ({len(manifest['entries'])} files, sha256 {digest}); status {manifest['status']}; "
              f"changed since A0p: {len(manifest['changed_since_a0p'])}; added: {len(manifest['added_since_a0p'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
