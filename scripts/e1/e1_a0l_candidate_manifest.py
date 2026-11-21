"""Candidate integrity manifest, A0l successor (2026-11-21): the A0k candidate files at their CURRENT hashes plus the four A0l additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0k.json`` (A0k, 72 files) and every earlier manifest are NOT edited. A0l changed NO file that the A0k manifest
lists: the pilot workload ``e1_create_asset.js`` (a candidate, never pinned, but historical) is left byte-identical, and the issuance logging is a VERSIONED SUCCESSOR
workload that extends it (``e1_create_asset_issue_logged.js``) plus a writer module (``e1_issue_log.js``), a Python reader (``issue_log_reader.py``) and a coverage
bridge (``issue_log_coverage.py``). The successor lists every A0k entry again with its current hash, adds the four files, and records an empty ``changed_since_a0k``
and an empty ``superseded_entries``, so nothing changes silently and the absence of a change is itself on the record.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not
  authorization to run a workload, a recorder, a collector, Caliper, Fabric, a model or a forecast.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0l_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0l.json
    python scripts/e1/e1_a0l_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0l.json
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

import e1_a0k_candidate_manifest as a0k                   # noqa: E402  (supplies the A0k specs, exclusions, closure roots and walker)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0l.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0k.json"
VERSION = "a0l_candidate_manifest 0.1 (2026-11-21)"
C = "src/fabric_perf/controllers/"
W = "fabric-testbed/caliper/workloads/"
CLOSURE_ROOTS = a0k.CLOSURE_ROOTS + (C + "issue_log_reader.py", C + "issue_log_coverage.py")

A0L_ADDITIONS = [
    pi.FileSpec(W + "e1_issue_log.js", "generator-side issuance log writer: hash-chained JSON lines (start, intent, attempt, outcome, tick, stop), written before sendRequests, fail-closed", "workload_issue_log"),
    pi.FileSpec(W + "e1_create_asset_issue_logged.js", "versioned successor of e1_create_asset.js with OPT-IN issuance logging (legacy behaviour unless round arguments carry issueLog)", "workload_module"),
    pi.FileSpec(C + "issue_log_reader.py", "incremental strict reader of the issue log: checkpoint and restart, replacement and truncation detection, read-time visibility, IssueLogEvidence for the recorder route", "issue_log_reader"),
    pi.FileSpec(C + "issue_log_coverage.py", "generator-issuance route: journal bridge, GeneratorIssueCoverage, GeneratorIssueSource and GeneratorIssueCapturedHistory", "issue_log_coverage"),
]
A0L_SPECS = list(a0k.A0K_SPECS) + A0L_ADDITIONS

EXCLUSIONS = tuple(a0k.EXCLUSIONS) + (
    {"item": "tests/a0l_support.py, the five tests/test_a0l_*_2026_11_21.py files, tests/e1/js/e1_issue_log.test.js and tests/e1/js/a0l_issue_round.js",
     "reason": "tests and the offline driver are verification, not runtime dependencies; they are hashed in the A0l report, not listed here"},
    {"item": "Integration/system_integration/a0l_2026-11-21/ (end-to-end demonstration script, stored synthetic evidence, mutation script and results)",
     "reason": "generated evidence of this task, not runtime dependencies; hashed in the A0l report"},
    {"item": "fabric-testbed/caliper/workloads/e1_create_asset.js, e1_workload_core.js and e1_trace_replay.js (read; the first two are listed above from the A0k manifest, unchanged)",
     "reason": "A0l did not edit them. The successor workload extends e1_create_asset.js through its exported build(); e1_trace_replay.js is not a Stage 3 dependency"},
    {"item": "scripts/e1/e1_la2_render.py (read, not edited, still pinned elsewhere)",
     "reason": "the pilot renderer still points at e1_create_asset.js. Selecting the successor workload and passing round arguments issueLog needs a versioned renderer successor that A0l does not write"},
)
UNRESOLVED = tuple(a0k.UNRESOLVED) + (
    "Generator-side issuance logging is implemented and tested OFFLINE ONLY. No Caliper worker has loaded e1_create_asset_issue_logged.js; the Caliper base class, sutAdapter "
    "behaviour, the return value of sendRequests and whether txSubmitted is emitted for a failed send are not read here (A0k caliper_check U1).",
    "The independence of the issue log from the recorder is DECLARED by data flow (the writer is called from the workload's sendRequests wrapper and the reader reads a file) "
    "and checked structurally by tests; it is not measured. Both run in one Node process on one Date.now(); a stalled event loop or a wall-clock step affects both.",
    "Generator observation is the generator's own tick records. The tick interval, max_tick_gap_ms, max_clock_skew_ms and max_callback_lag_ms are placeholders with no "
    "scientific justification, and none has been measured under a real Caliper worker.",
    "A fully observed generator interval supports a claim about the experimental generator's issuance only. It does not establish external Ethereum mempool capture, Caliper "
    "submission, Fabric acceptance or ledger commitment, and live_source_coverage_verified remains false on every route.",
    "The issue log is tamper-EVIDENT (hash-chained, canonical) but not tamper-proof: a writer or an operator with file access can rebuild a consistent chain. The chain head "
    "is not anchored anywhere outside the log.",
    "Durability: the writer fsyncs at each tick by default; a process crash loses nothing that write() returned, a power loss can lose records since the last tick.",
    "The pilot renderer (pinned) still selects e1_create_asset.js; no configuration selects the successor workload or passes issueLog. Nothing has been rendered or run.",
    "One worker only: both E1 workloads and the issue-log writer reject more than one worker, and asset keys carry no worker index. Multi-worker issuance evidence needs workload changes first.",
    "Journal rows written by IssueLogJournalBridge use the reader's read time as t_utc, so a reader that starts late loses the early seconds (no retroactive claim). Nothing replays "
    "the journal live; the bridge has only been driven by scripted read times.",
)
OPEN_ITEMS = tuple(a0k.OPEN_ITEMS) + (
    "A0l: owner approval of the four A0l additions has not been given. No workload, recorder, collector, Caliper, Fabric, Docker or WSL was started; Node ran only the offline driver.",
    "A0l: decide whether the pilot renders the successor workload (a versioned renderer successor with an issueLog argument and a per-round log path) or switches to the trace-replay workload.",
    "A0l: measure callback latency, tick scheduling and event-loop behaviour under a real Caliper worker before any max_tick_gap_ms or max_callback_lag_ms is defended.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0L_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "entry_point": "scripts/e1/e1_stage3_shadow_runner.py",
                              "modes": {"fixture": "implemented, offline; optional captured history (A0h), verified coverage (A0i), recorder-derived coverage (A0j), worker-aware coverage (A0k) "
                                                   "or generator-issuance coverage (A0l)",
                                        "live-shadow": "refusal only (exit 5)", "predictive real": "prepared, DISABLED (exit 6)",
                                        "real-inference smoke test": "specification only; run command refuses (exit 6)"},
                              "issue_log": {"schema": "e1_issue_log/1", "workload": W + "e1_create_asset_issue_logged.js", "opt_in": "round argument issueLog; absent means legacy behaviour",
                                            "approved": False, "live_run": False}},
        experimental_configuration_provenance={"source": "operator-supplied parameters (prelive_params/1), controller policy (stage3_controller_policy/2), recorder health policy, "
                                                         "a source counter descriptor and an issue-log policy (IssueLogPolicy)",
                                               "note": "no cell, threshold, freshness limit, heartbeat interval, tick interval, gap or callback-lag allowance is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    manifest["changed_since_a0k"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0k"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0k"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0k_sha256": earlier[p], "a0l_sha256": current[p], "reason": "UNEXPLAINED: A0l changes no candidate file; add a reason"}
                                      for p in manifest["changed_since_a0k"]]
    manifest["superseded_note"] = ("A0l changed no file listed in the A0k manifest, so nothing is superseded. The pilot workload e1_create_asset.js is preserved byte-for-byte "
                                   "and its versioned successor is a new file.")
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0j", "changed_since_a0j", "superseded_entries_a0j"):          # A0k-era lists belong to the A0k manifest; they are not repeated as if they were A0l's
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
              f"changed since A0k: {len(manifest['changed_since_a0k'])}; added: {len(manifest['added_since_a0k'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
