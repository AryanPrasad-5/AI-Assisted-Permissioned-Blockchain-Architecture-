"""Candidate integrity manifest, A0j successor (2026-11-21): the A0i candidate files plus the recorder-health instrumentation files.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0i.json`` (A0i, 66 files) and every earlier manifest are NOT edited. This script
writes a clearly versioned SUCCESSOR that lists every A0i entry again with its CURRENT hash and adds the A0j files. A0j edited NO file that
the A0i manifest lists (the integration is built from new modules that sit on top of the pinned ones), so ``changed_since_a0i`` is expected to
be empty; it is computed, not asserted, so a changed file would be shown.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not
  approval and not authorization to run a recorder, a collector, Caliper, Fabric, a model or a forecast.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0j_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0j.json
    python scripts/e1/e1_a0j_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0j.json
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

import e1_a0i_candidate_manifest as a0i                   # noqa: E402  (supplies the A0i specs, exclusions and closure walker)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0j.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0i.json"
VERSION = "a0j_candidate_manifest 0.1 (2026-11-21)"
C = "src/fabric_perf/controllers/"
CLOSURE_ROOTS = a0i.CLOSURE_ROOTS + (C + "recorder_health_bridge.py", C + "recorder_coverage.py")

A0J_ADDITIONS = [
    pi.FileSpec("scripts/e1/caliper/e1_per_tx_recorder_v3.js", "recorder v3: v2 plus the recorder-health spool (recorder_callback coverage scope; requires v2 as a sibling)", "recorder_v3_health"),
    pi.FileSpec(C + "recorder_health_bridge.py", "replays the recorder health spool through the one CollectorHealthJournal; timing policy with no defaults", "recorder_health_bridge"),
    pi.FileSpec(C + "recorder_coverage.py", "recorder coverage: zeros need an independent source count; submit-event binning; Stage 3 history adapter", "recorder_coverage"),
    pi.FileSpec("scripts/e1/e1_a0j_recorder_health_demo.py", "synthetic demonstration: recorder spool -> journal -> coverage -> Stage 3 (zero certified, heartbeat-only refused)", "demonstration"),
]
A0J_SPECS = list(a0i.A0I_SPECS) + A0J_ADDITIONS

EXCLUSIONS = tuple(a0i.EXCLUSIONS) + (
    {"item": "tests/a0j_support.py, the four tests/test_a0j_*_2026_11_21.py files, tests/e1/js/e1_per_tx_recorder_v3.test.js and tests/e1/js/a0j_v3_round.js",
     "reason": "tests are verification, not runtime dependencies; they are hashed in the A0j report, not listed here"},
    {"item": "Integration/system_integration/a0j_2026-11-21/ (demonstration runs, mutation script and results)",
     "reason": "generated evidence of this task, not runtime dependencies; hashed in the A0j report"},
)
UNRESOLVED = tuple(a0i.UNRESOLVED) + (
    "The recorder-health spool is written by a recorder that has NEVER run under real Caliper. e1_per_tx_recorder_v3.js was exercised only under a "
    "scripted wall clock and a fake dispatch; its behaviour under real Caliper scheduling, a loaded event loop and Windows-WSL clocks is unmeasured.",
    "No independent source-completeness counter exists in this repository. Without options.health.sourceCount every zero-arrival second from the recorder "
    "stays zero_count_unknown_coverage, so the strict predictive controller still abstains on any live zero second.",
    "The independence of a supplied source counter is DECLARED by the collector; recorder_coverage cannot verify it. live_source_coverage_verified is always false.",
    "Callback completeness cannot be shown from the observer side: Caliper offers no per-event sequence number at the TxSubmitted callback, and the recorder is a passive observer.",
    "Heartbeat interval, maximum gap, freshness limit and clock tolerance are operator-supplied engineering proposals (CANDIDATE_PROPOSAL); none is validated against real scheduling.",
    "One recorder worker (workers = 1) is supported. Several workers would need one journal per worker and a defined combination rule; none is written.",
    "The spool is not tamper-evident until it is replayed into the journal; between the recorder and the replay it is an ordinary file. Replay uses the collector's own timestamps, so a live decision "
    "would also need evidence of WHEN each row became available to the decision process.",
    "Stage 3 reads the evidence offline. No live process replays the spool or builds the history while a run is in progress.",
)
OPEN_ITEMS = tuple(a0i.OPEN_ITEMS) + (
    "A0j: owner approval of the A0j additions has not been given. No recorder was started, no collector ran, no Caliper or Fabric was contacted.",
    "A0j: whether and how an independent source counter can be supplied for a real run is undecided (owner decision).",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0J_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "entry_point": "scripts/e1/e1_stage3_shadow_runner.py",
                              "modes": {"fixture": "implemented, offline; optional captured history (A0h), verified coverage (A0i) or recorder-derived coverage (A0j)",
                                        "live-shadow": "refusal only (exit 5)", "predictive real": "prepared, DISABLED (exit 6)",
                                        "real-inference smoke test": "specification only; run command refuses (exit 6)"}},
        experimental_configuration_provenance={"source": "operator-supplied parameters (prelive_params/1), controller policy (stage3_controller_policy/2) and recorder health policy",
                                               "note": "no cell, threshold, freshness limit, heartbeat interval or gap is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    manifest["changed_since_a0i"] = sorted(e["path"] for e in manifest["entries"] if e["path"] in earlier and e["sha256"] != earlier[e["path"]])
    manifest["added_since_a0i"] = sorted(e["path"] for e in manifest["entries"] if e["path"] not in earlier)
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0h", "changed_since_a0g", "changed_since_a0h"):       # A0i-era change lists belong to the A0i manifest; they are not repeated as if they were A0j's
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
              f"changed since A0i: {len(manifest['changed_since_a0i'])}; added: {len(manifest['added_since_a0i'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
