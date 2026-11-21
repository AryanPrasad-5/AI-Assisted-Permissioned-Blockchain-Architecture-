"""Candidate integrity manifest, A0k successor (2026-11-21): the A0j candidate files at their CURRENT hashes, the two A0k modules, and an explicit record of
every file A0k changed.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0j.json`` (A0j, 70 files) and every earlier manifest are NOT edited. A0k had to edit eight files
the A0j manifest lists (all ``PENDING_OWNER_APPROVAL`` with no pin: candidate files, not frozen ones), because the generic ``VerifiedCoverage`` ignored the
journal's coverage scope. The successor lists every A0j entry again with its current hash, adds two modules, and records for each changed path the A0j hash,
the A0k hash and why it changed (``superseded_entries``), so nothing changes silently.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not
  authorization to run a recorder, a collector, Caliper, Fabric, a model or a forecast.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0k_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0k.json
    python scripts/e1/e1_a0k_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0k.json
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

import e1_a0j_candidate_manifest as a0j                   # noqa: E402  (supplies the A0j specs, exclusions, closure roots and walker)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0k.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0j.json"
VERSION = "a0k_candidate_manifest 0.1 (2026-11-21)"
C = "src/fabric_perf/controllers/"
CLOSURE_ROOTS = a0j.CLOSURE_ROOTS + (C + "source_completeness.py", C + "worker_coverage.py")
# the v3 recorder is found by pattern, not named: a guard (test_42 of the A0j integrity tests) keeps its path out of every other source file
V3_RECORDER = next((REPO / "scripts" / "e1" / "caliper").glob("*recorder_v3.js")).relative_to(REPO).as_posix()

A0K_ADDITIONS = [
    pi.FileSpec(C + "source_completeness.py", "source-count contract: descriptor classification, journal-reported and dispatch-log source evidence, feasibility table, timestamp inventory", "source_completeness"),
    pi.FileSpec(C + "worker_coverage.py", "worker roster and multi-worker coverage: every expected worker must accept a second; missing, duplicate or mislabelled workers fail closed", "worker_coverage"),
]
A0K_SPECS = list(a0j.A0J_SPECS) + A0K_ADDITIONS

# why each A0j-listed file changed (the manifest records the A0j and A0k hashes next to the reason)
CHANGE_REASONS = {
    C + "workload_coverage.py": "coverage scope rule: the generic VerifiedCoverage now reads coverage_scope from the hash-chained start row; a recorder_callback zero needs verified source evidence, an undeclared scope certifies no zero",
    C + "collector_health.py": "start() accepts an optional coverage_scope (validated against the vocabulary); omitted, the scope stays undeclared",
    C + "captured_history.py": "recorder-derived arrivals may only be attached to coverage declaring recorder_callback (coverage_scope_source_mismatch); provenance note extended",
    C + "recorder_coverage.py": "the zero rule moved into workload_coverage and the source-count verification into source_completeness; source_evidence passes through; rejection reason codes recorded",
    C + "recorder_health_bridge.py": "RECORDER_START_EXTRAS carries the source_counter descriptor from the spool start row into the journal",
    V3_RECORDER: "optional sourceCount.descriptor copied unchanged into the start row (source_counter); the recorder never judges it",
    "scripts/e1/e1_a0i_coverage_demo.py": "the demo collector declares coverage_scope=source_stream_observer (its stored summary is unchanged)",
    "scripts/e1/e1_a0j_recorder_health_demo.py": "the scripted recorder start row carries a synthetic source_counter descriptor (its stored summary is unchanged)",
}

EXCLUSIONS = tuple(a0j.EXCLUSIONS) + (
    {"item": "tests/a0k_support.py, tests/a0k_pins.py, the five tests/test_a0k_*_2026_11_21.py files and the v3 recorder's descriptor Node test",
     "reason": "tests are verification, not runtime dependencies; they are hashed in the A0k report, not listed here"},
    {"item": "Integration/system_integration/a0k_2026-11-21/ (mutation script, results and evidence)",
     "reason": "generated evidence of this task, not runtime dependencies; hashed in the A0k report"},
    {"item": "fabric-testbed/caliper/workloads/*.js and scripts/e1/e1_la2_render.py (read, not edited)",
     "reason": "inspected for the source-count feasibility finding; e1_la2_render.py remains pinned elsewhere and the workload modules are not Stage 3 dependencies"},
)
UNRESOLVED = tuple(a0j.UNRESOLVED) + (
    "Source completeness remains UNVERIFIED. The only generator-side per-transaction record in the repository is the trace-replay dispatch log; the pilot "
    "workload (e1_create_asset.js, used by e1_la2_render.py) logs and exposes nothing. No reader turns the dispatch log into a live provider, and Caliper's own "
    "counters are not in this repository.",
    "The independence of any source counter is DECLARED by the collector and checked for structure only (source_completeness.classify_descriptor). Nothing here "
    "proves it, and live_source_coverage_verified is false on every route.",
    "DispatchLogEvidence.max_callback_lag_ms is a declared, unmeasured assumption: a settled boundary assumes every callback lands within it. A wrong value can only "
    "leave a span unverified or, if too small, mislabel in-flight transactions as missed callbacks; it is not validated against real Caliper scheduling.",
    "coverage_scope source_stream_observer is a DECLARED claim (the A0i premise for a collector that itself receives the source stream). It is refused for any journal "
    "that carries recorder fields and for recorder-derived arrivals, but nothing verifies it for a real collector.",
    "Multi-worker coverage is a contract tested on synthetic streams only. Both E1 workload modules refuse more than one worker, their asset keys carry no worker index, "
    "and every config uses workers = 1, so a multi-worker E1 run cannot happen today. The roster is declared, never proven; worker clocks are never assumed synchronized.",
    "Journals written before A0k declare no coverage scope; positives are still observed but their zeros are unknown. The A0i and A0j fixtures were upgraded to declare a "
    "scope (a deliberate contract tightening; the A0j test that asserted the defect was inverted).",
    "The A0i and A0j manifests record the pre-A0k hashes of the eight changed files; they were left byte-identical and describe those earlier states.",
)
OPEN_ITEMS = tuple(a0j.OPEN_ITEMS) + (
    "A0k: owner approval of the A0k additions and of the eight changed candidate files has not been given. No recorder, collector, Caliper, Fabric, Docker or WSL was started.",
    "A0k: whether to instrument e1_create_asset.js (or switch the pilot to the trace-replay workload) so a generator-side issue log exists for the pilot is undecided (owner decision).",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0K_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "entry_point": "scripts/e1/e1_stage3_shadow_runner.py",
                              "modes": {"fixture": "implemented, offline; optional captured history (A0h), verified coverage (A0i), recorder-derived coverage (A0j) or worker-aware coverage (A0k)",
                                        "live-shadow": "refusal only (exit 5)", "predictive real": "prepared, DISABLED (exit 6)",
                                        "real-inference smoke test": "specification only; run command refuses (exit 6)"}},
        experimental_configuration_provenance={"source": "operator-supplied parameters (prelive_params/1), controller policy (stage3_controller_policy/2), recorder health policy and a source counter descriptor",
                                               "note": "no cell, threshold, freshness limit, heartbeat interval, gap or callback-lag allowance is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    manifest["changed_since_a0j"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0j"] = sorted(p for p in current if p not in earlier)
    manifest["superseded_entries"] = [{"path": p, "a0j_sha256": earlier[p], "a0k_sha256": current[p], "reason": CHANGE_REASONS.get(p, "UNEXPLAINED: add a reason")}
                                      for p in manifest["changed_since_a0j"]]
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0i", "changed_since_a0i"):          # A0j-era change lists belong to the A0j manifest; they are not repeated as if they were A0k's
        manifest.pop(key, None)
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
              f"changed since A0j: {len(manifest['changed_since_a0j'])}; added: {len(manifest['added_since_a0j'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
