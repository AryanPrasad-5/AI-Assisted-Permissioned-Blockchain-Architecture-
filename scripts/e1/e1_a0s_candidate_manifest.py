"""Candidate integrity manifest, A0s successor (2026-11-22): the A0q candidate files at their CURRENT hashes plus the A0s additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-22_a0q.json`` and every earlier manifest are NOT edited. A0s changed NO file that the A0q manifest lists, so ``changed_since_a0q`` and
``superseded_entries`` are empty. A0s adds eight new files (the bounded execution library, the read-only configuration-inspection library, the Windows<->WSL clock-probe collector,
the run-layout module, three command-line wrappers and this builder) and lists one older tool (``scripts/e1/e1_metrics_scraper.py``, the Prometheus scraper whose command line the
execution plan quotes) for the first time.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a
  workload, Caliper, Fabric, a probe, a model or a forecast. Nothing is pinned here. The first-run instrumentation variant named below is a PROPOSAL.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0s_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-22_a0s.json
    python scripts/e1/e1_a0s_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-22_a0s.json
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

import e1_a0q_candidate_manifest as a0q                      # noqa: E402  (supplies the A0q specs, exclusions, unresolved items and open items)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402
from fabric_perf.controllers import run_layout as rl          # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-22_a0s.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0q.json"
VERSION = "a0s_candidate_manifest 0.1 (2026-11-22)"
A0R_EDITED_TEST = "tests/test_a0d_recorder_wiring_2026_11_21.py"

A0S_ADDITIONS = [
    pi.FileSpec("src/fabric_perf/controllers/bounded_execution.py", "bounded, explicitly gated execution entry point: request validation, execution plan, authorization record verification, "
                "future execute adapter that accepts only an injected mock/fixture runner (REAL_LAUNCH_ENABLED = False)", "bounded_execution"),
    pi.FileSpec("src/fabric_perf/controllers/config_inspection.py", "read-only channel-configuration inspection: allow-listed vectors, decoded-block parsing against the expected cell, "
                "before/after comparison (LIVE_INSPECTION_ENABLED = False)", "config_inspection"),
    pi.FileSpec("src/fabric_perf/controllers/wsl_clock_probe.py", "Windows<->WSL clock sample collector producing clock_preflight samples; injected runner and clocks only "
                "(LIVE_CLOCK_SAMPLING_ENABLED = False)", "clock_probe"),
    pi.FileSpec("src/fabric_perf/controllers/run_layout.py", "unified run layout and explicit instrumentation selection derived from a rendered A0p variant directory", "run_layout"),
    pi.FileSpec("scripts/e1/e1_a0s_bounded_execute.py", "CLI: prepare, preflight, inspect-plan, authorization-template, verify-authorization; execute is always refused (exit 5)", "bounded_execution_cli"),
    pi.FileSpec("scripts/e1/e1_a0s_config_inspect.py", "CLI: plan, parse, compare; live inspect is always refused (exit 5)", "config_inspection_cli"),
    pi.FileSpec("scripts/e1/e1_a0s_clock_probe.py", "CLI: plan, parse, check, evaluate; sample is always refused (exit 5)", "clock_probe_cli"),
    pi.FileSpec("scripts/e1/e1_a0s_candidate_manifest.py", "this manifest's builder and verifier", "a0s_manifest_builder"),
    pi.FileSpec("scripts/e1/e1_metrics_scraper.py", "older Prometheus scraper whose scrape command line the execution plan quotes (first listed in A0s; a modified tracked file, uncommitted; never run by A0s)",
                "prometheus_scraper_legacy"),
]
A0S_NEW_PATHS = sorted(spec.path for spec in A0S_ADDITIONS)
A0S_SPECS = list(a0q.A0Q_SPECS) + A0S_ADDITIONS

EXCLUSIONS = tuple(a0q.EXCLUSIONS) + (
    {"item": "tests/a0s_support.py and the five tests/test_a0s_*_2026_11_22.py files, docs/research/e1/a0s_* evidence and the A0s report",
     "reason": "verification and documentation, not runtime dependencies; hashed in the A0s evidence SHA256SUMS, not listed here"},
    {"item": "scripts/e1/e1_independent_config_verify.py and scripts/e1/e1_clock_handshake.py",
     "reason": "reference only: the A0s tests build the existing verifier with a fake runner to compare argument vectors, and the older clock handshake measures host<->container offsets in another format; "
               "neither is a runtime dependency of the A0s tools, so neither is listed"},
    {"item": A0R_EDITED_TEST, "reason": "edited by A0r (allow-list of two exact paths); it was never listed in a candidate manifest, so no supersession applies. Its current hash is recorded under a0r_edited_verification_file"},
)
UNRESOLVED = tuple(a0q.UNRESOLVED) + (
    "No A0s tool has run against a real process, Caliper worker, Fabric network, WSL distribution, Windows clock or Prometheus endpoint. Every argument vector in an execution plan is text; "
    "that it works on the target machine, and the clock probe's round trip, are unmeasured.",
    "The execution plan stages the rendered directory with mkdir and cp argument vectors and expects WSL paths for the rendered directory, output directory, network description and repository; "
    "none of these paths is verified, and no WSL path translation tool exists.",
    "The clock probe uses wsl.exe plus date +%s%N. Process start-up is expected to dominate the round trip; whether the uncertainty can fall under any chosen tolerance is unknown.",
    "The first-run instrumentation selection (rl.SELECTION) is a proposal. Source-count and recorder-health evidence are produced post-run from finished files, not in-run.",
    "Real launch is disabled by constants and by the absence of any process runner in the A0s modules; a future tool that supplies one is a new, separately approved artifact.",
)
OPEN_ITEMS = tuple(a0q.OPEN_ITEMS) + (
    "A0s: owner approval of the eight A0s additions and of the first listing of scripts/e1/e1_metrics_scraper.py has not been given. No Caliper, Fabric, Docker, WSL, Prometheus, model or actuator was started.",
    "A0s: the dedicated validation channel, the experimental cell, every operator threshold, the clock tolerance and the Prometheus choice (before_after or deferred) are unset or unapproved.",
    "A0s: the authorization record schema (a0s_bounded_launch_authorization/1), the request schema and the plan schema are PROPOSED engineering schemas awaiting an owner decision.",
    "A0s: a runner that can start a process (needed for any real launch, inspection or clock sample) does not exist and is not authorized.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    a0q_manifest = a0q.build(repo, created_utc=created_utc)
    manifest = pi.build_candidate_manifest(
        repo, A0S_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "approved": False, "live_run": False,
                              "entry_points": {"bounded_execution": "scripts/e1/e1_a0s_bounded_execute.py (prepare, preflight, inspect-plan, authorization-template, verify-authorization; execute REFUSED)",
                                               "config_inspection": "scripts/e1/e1_a0s_config_inspect.py (plan, parse, compare; live inspect REFUSED)",
                                               "clock_probe": "scripts/e1/e1_a0s_clock_probe.py (plan, parse, check, evaluate; sample REFUSED)",
                                               "execute": "none; real launch is disabled (bounded_execution.REAL_LAUNCH_ENABLED = False) and no process runner exists in any A0s file"},
                              "variants": {"a0e_recorder_v2_legacy_workload": "historical; unchanged", "a0m_recorder_v3_issue_logged": "A0o; unchanged",
                                           "a0p_recorder_v3_timed_issue_log": "A0p; unchanged; PROPOSED for the first bounded validation (the only variant bounded_execution accepts)"}},
        experimental_configuration_provenance=a0q_manifest.get("experimental_configuration_provenance", {}))
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    manifest["changed_since_a0q"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0q"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0q"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0q_sha256": earlier[p], "a0s_sha256": current[p], "reason": "UNEXPLAINED: A0s changes no A0q candidate file; add a reason"}
                                      for p in manifest["changed_since_a0q"]]
    manifest["superseded_note"] = "A0s changed no file listed in the A0q manifest, so nothing is superseded."
    manifest["a0r_edited_verification_file"] = {"path": A0R_EDITED_TEST, "sha256": pi.sha256_file(repo / A0R_EDITED_TEST),
                                                "note": "edited by A0r, outside every candidate manifest; recorded here so the hash is not lost (the pre-edit hash was not captured by A0r)"}
    schema_versions = dict(a0q_manifest["schema_versions"])
    schema_versions.update({"a0s_execution_request": "a0s_execution_request/1 (new, proposed)", "a0s_execution_plan": "a0s_execution_plan/1 (new, proposed)",
                            "a0s_bounded_launch_authorization": "a0s_bounded_launch_authorization/1 (new, proposed)", "a0s_run_status": "a0s_run_status/1 (new, proposed)",
                            "a0s_integrity_manifest": "a0s_integrity_manifest/1 (new, proposed)", "config_inspection_plan": "config_inspection_plan/1 (new, proposed)",
                            "config_inspection_evidence": "config_inspection_evidence/1 (new, proposed)", "wsl_clock_phase": "wsl_clock_phase/1 (new, proposed; samples are clock_preflight samples)",
                            "a0s_run_layout": "a0s_run_layout/1 (new, proposed)"})
    manifest["schema_versions"] = schema_versions
    manifest["selected_instrumentation_variant"] = {
        "status": "PROPOSED_NOT_APPROVED", "variant": rl.FIRST_LIVE_VARIANT,
        "selection": {name: {k: v for k, v in spec.items() if k != "why"} for name, spec in rl.SELECTION.items()},
        "note": "explicit; no component is chosen as 'latest'. DEFERRED and POST_RUN states and their effects on the measurements are stated in the A0s report."}
    manifest["runtime_dependencies"] = {"python_stdlib_only": True, "node_modules_required_by_new_js": [],
                                        "imports_unmodified": ["channel_selection", "config_evidence", "clock_preflight", "prelive_integrity", "launch_variant", "timed_variant", "issue_log_reader",
                                                               "issue_log_timing", "ledger_evidence", "recorder_health_bridge", "recorder_health_incremental", "sourcecount_reader", "timing_durable"],
                                        "process_or_network_api_imported_by_a0s_files": False}
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0p", "changed_since_a0p", "removed_since_a0p", "superseded_note_a0p", "changed_verification_files"):
        manifest.pop(key, None)
    manifest["changed_verification_files_a0q"] = a0q_manifest.get("changed_verification_files", [])
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
              f"changed since A0q: {len(manifest['changed_since_a0q'])}; added: {len(manifest['added_since_a0q'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
