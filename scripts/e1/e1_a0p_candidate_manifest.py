"""Candidate integrity manifest, A0p successor (2026-11-22): the A0o candidate files at their CURRENT hashes plus the A0p additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0o.json`` and every earlier manifest are NOT edited. A0p changed NO file that the A0o manifest lists (the pinned renderer, recorder v1,
the legacy workload, the A0l issue-log writer and reader, the A0j whole-file bridge, the A0o reader driver, sampler and launch-variant tool and every A0e to A0m source are byte-identical),
so ``changed_since_a0o`` and ``superseded_entries`` are empty and the absence of a change is itself on the record. A0p adds eight new runtime files:

* the timed issue-log writer and the timed workload (JavaScript);
* the issue-log timing-sidecar verifier, the incremental recorder-health bridge, the T01/T06 sampler extension and the timed-variant selection library (Python);
* the timed-variant render/preflight tool and the A0p timing tools command line.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a workload, a
  recorder, a bridge, Caliper, Fabric, a model or a forecast. Nothing is pinned here.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0p_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-22_a0p.json
    python scripts/e1/e1_a0p_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-22_a0p.json
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

import e1_a0o_candidate_manifest as a0o                      # noqa: E402  (supplies the A0o specs, exclusions, unresolved items and open items)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-22_a0p.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0o.json"
VERSION = "a0p_candidate_manifest 0.1 (2026-11-22)"

A0P_ADDITIONS = [
    pi.FileSpec("fabric-testbed/caliper/workloads/e1_issue_log_timed.js", "timed issue-log writer (subclass of the A0l writer): same e1_issue_log/1 log, plus the e1_issue_log_timing/1 sidecar "
                "with write-start, write-return and fsync-return stamps; fails closed", "issue_log_timed_writer"),
    pi.FileSpec("fabric-testbed/caliper/workloads/e1_create_asset_issue_logged_timed.js", "timed workload successor: reads round argument issueLogTimed (never issueLog), "
                "log path and timing-sidecar path, one worker", "workload_issue_logged_timed_selected"),
    pi.FileSpec("src/fabric_perf/controllers/issue_log_timing.py", "timing-sidecar verifier: hash chain, link to the validated log, ordering, crash-tail and unsynced reporting",
                "issue_log_timing_verifier"),
    pi.FileSpec("src/fabric_perf/controllers/recorder_health_incremental.py", "incremental restart-safe recorder-health bridge: ingest ledger with explicit stamps, reconciled checkpoint, "
                "crash recovery by deterministic replay", "recorder_health_incremental_bridge"),
    pi.FileSpec("src/fabric_perf/controllers/timing_durable.py", "T01 direct write/sync timing and T06 ingestion delay on top of the unchanged A0o sampler", "timing_durable_library"),
    pi.FileSpec("src/fabric_perf/controllers/timed_variant.py", "timed-variant selection: variant derivation, deterministic benchmark conversion, mixed-version refusals, overlay verification",
                "timed_variant_library"),
    pi.FileSpec("scripts/e1/e1_a0p_launch_variant.py", "timed-variant dry render (qualified run id, overlay, marker), preflight, execute refused", "timed_variant_entry_point"),
    pi.FileSpec("scripts/e1/e1_a0p_timing_tools.py", "verify-write-timing, incremental bridge over a finished spool, T01-T06 sampler; writes new files only", "a0p_timing_tools_entry_point"),
]
A0P_NEW_PATHS = sorted(spec.path for spec in A0P_ADDITIONS)
A0P_SPECS = list(a0o.A0O_SPECS) + A0P_ADDITIONS

EXCLUSIONS = tuple(a0o.EXCLUSIONS) + (
    {"item": "tests/a0p_support.py, tests/e1/js/a0p_timed_round.js and the six tests/test_a0p_*_2026_11_22.py files",
     "reason": "tests are verification, not runtime dependencies; they are hashed in the A0p report, not listed here"},
    {"item": "docs/research/e1/a0p_example_artifacts_2026-11-22/ (example artifacts, mutation results, SHA256SUMS) and the A0p report",
     "reason": "generated evidence and documentation of this task, not runtime dependencies; hashed in SHA256SUMS.txt"},
)
UNRESOLVED = tuple(a0o.UNRESOLVED) + (
    "The timed writer, the timed workload and the incremental bridge have only run under a fake Caliper driver, scripted clocks and synthetic spools. Neither has run in a real Caliper "
    "worker, on the target (bind-mounted) filesystem, or under real load; no value they produced is a measurement of Caliper or Fabric.",
    "Write-return and fsync-return stamps are the file-system API's own acknowledgements. They are not a guarantee against every storage failure, and on a Windows-WSL bind mount the "
    "guarantee behind fsync is itself unmeasured.",
    "T06 across two clock domains needs a T12 offset interval, which does not exist; a same-domain T06 rests on an operator declaration. Windows-WSL clock agreement was not measured.",
    "The incremental bridge's restart safety is proven only for crashes at the named boundaries on a scripted file system; a real power loss, a lying disk and a concurrent second bridge "
    "are not covered. A second concurrent bridge over the same files is not detected (no lock is taken).",
    "The timed variant is an overlay beside an untouched A0m render: the A0m preflight validates the render, the A0p preflight validates the overlay. A launch tool that consumes the "
    "overlay does not exist; execute is refused.",
    "The sidecar is written inside the log writer's call path; its synchronous cost on the target filesystem is unmeasured and may change the achieved send rate (Gate 4).",
)
OPEN_ITEMS = tuple(a0o.OPEN_ITEMS) + (
    "A0p: owner approval of the eight A0p additions has not been given. No Caliper, Fabric, Docker, WSL, Prometheus, model or actuator was started; Node ran only the offline fake-Caliper test "
    "driver.",
    "A0p: the schema additions (e1_issue_log_timing/1, recorder_health_ingest/1, recorder_health_bridge_checkpoint/1, recorder_health_incremental_report/1) are PROPOSED engineering "
    "schemas awaiting owner decision; the historical schemas are unchanged.",
    "A0p: the dedicated validation channel, the experimental cell, the bridge poll cadence, every heartbeat setting and every operator threshold remain unset or unapproved; the A0j "
    "candidate values are unverified engineering proposals.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0P_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES,
                              "entry_points": {"render": "scripts/e1/e1_a0p_launch_variant.py render (timed-writer overlay beside an untouched A0m render, offline)",
                                               "preflight": "scripts/e1/e1_a0p_launch_variant.py preflight (overlay checks + the unchanged A0m preflight)",
                                               "timing_tools": "scripts/e1/e1_a0p_timing_tools.py verify-write-timing | bridge | sample (offline)",
                                               "execute": "refused (exit 5) in every A0p tool; not implemented"},
                              "variants": {"a0e_recorder_v2_legacy_workload": "historical; unqualified run id; tool unchanged",
                                           "a0m_recorder_v3_issue_logged": "A0o; run id suffix -v3il; tool unchanged",
                                           "a0p_recorder_v3_timed_issue_log": "A0p; run id suffix -v3ilt; directory a0p-v3ilt/<render id>; overlay + sibling marker"},
                              "approved": False, "live_run": False},
        experimental_configuration_provenance={"source": "operator-supplied parameters: prelive_params/1 (A0e), a0m_render_params/1 and the bridge / sampler arguments",
                                               "note": "no cell, channel, threshold, tick interval, poll interval, heartbeat interval or clock-domain declaration is approved; none is "
                                                       "claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    manifest["changed_since_a0o"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0o"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0o"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0o_sha256": earlier[p], "a0p_sha256": current[p], "reason": "UNEXPLAINED: A0p changes no candidate file; add a reason"}
                                      for p in manifest["changed_since_a0o"]]
    manifest["superseded_note"] = ("A0p changed no file listed in the A0o manifest, so nothing is superseded. The pinned renderer, recorder v1, the legacy workload, the A0l writer and reader, "
                                   "the whole-file bridge and every A0o file are preserved byte-for-byte; the eight A0p files are new.")
    manifest["schema_versions"] = {"e1_issue_log": "e1_issue_log/1 (unchanged)", "issue_log_timing_sidecar": "e1_issue_log_timing/1 (new, proposed)",
                                   "recorder_health_spool": "recorder_health_spool/1 (unchanged)", "collector_health_journal": "collector_health_event/1 (unchanged)",
                                   "ingest_ledger": "recorder_health_ingest/1 (new, proposed)", "bridge_checkpoint": "recorder_health_bridge_checkpoint/1 (new, proposed)",
                                   "bridge_report": "recorder_health_incremental_report/1 (new; compatible_with recorder_health_ingest_report/1)",
                                   "timing_results": "a0p_timing_results/1 (wraps a0o_timing_results/1)", "timing_sample": "a0n_timing_sample/1 (unchanged)"}
    manifest["runtime_dependencies"] = {"node_modules_required_by_new_js": ["fs", "path"], "python_stdlib_only": True,
                                        "new_js_sibling_requires": ["e1_issue_log.js", "e1_create_asset.js", "e1_workload_core.js", "e1_issue_log_timed.js"],
                                        "caliper_package": "@hyperledger/caliper-core (resolved only inside a Caliper worker; unverifiable offline)"}
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0m", "changed_since_a0m", "removed_since_a0m", "superseded_note_a0m"):      # A0o-era lists belong to the A0o manifest; they are not repeated as A0p's
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
              f"changed since A0o: {len(manifest['changed_since_a0o'])}; added: {len(manifest['added_since_a0o'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
