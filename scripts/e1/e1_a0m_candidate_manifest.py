"""Candidate integrity manifest, A0m successor (2026-11-21): the A0l candidate files at their CURRENT hashes plus the A0m additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0l.json`` (A0l, 76 files) and every earlier manifest are NOT edited. A0m changed NO file that the A0l manifest lists:
the pinned renderer, recorder v1, the legacy workload and every A0l source are byte-identical. A0m adds the versioned dry-render orchestrator and the two input files the
render reads (the renderer environment and the network description), which are runtime inputs of the generated renderer. The successor lists every A0l entry again with
its current hash, records an empty ``changed_since_a0l`` and an empty ``superseded_entries``, so the absence of a change is itself on the record.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a
  workload, a recorder, a collector, Caliper, Fabric, a model or a forecast. Nothing is pinned here.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0m_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0m.json
    python scripts/e1/e1_a0m_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0m.json
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

import e1_a0l_candidate_manifest as a0l                   # noqa: E402  (supplies the A0l specs, exclusions, closure roots)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0m.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0l.json"
VERSION = "a0m_candidate_manifest 0.1 (2026-11-21)"
CLOSURE_ROOTS = a0l.CLOSURE_ROOTS

A0M_ADDITIONS = [
    pi.FileSpec("scripts/e1/e1_a0m_dry_render.py", "versioned DRY-RENDER orchestrator: A0e prepare (pinned renderer + v2 wiring) then a derived run that selects e1_create_asset_issue_logged.js, "
                "recorder v3 and per-round issue-log paths; manifest, preflight, execute refused", "dry_render_orchestrator"),
    pi.FileSpec("fabric-testbed/network/env-e1-caliper-benchmark.json", "renderer environment input (WSL paths, protocol) read by the pinned renderer through A0e prepare",
                "renderer_environment_input"),
    pi.FileSpec("fabric-testbed/network/network-e1-recovery.json", "network description input named by the A0e parameters (read, never contacted)", "network_description_input"),
]
A0M_SPECS = list(a0l.A0L_SPECS) + A0M_ADDITIONS

EXCLUSIONS = tuple(a0l.EXCLUSIONS) + (
    {"item": "tests/a0m_support.py, tests/test_a0m_dry_render_2026_11_21.py, tests/test_a0m_followup_2026_11_21.py, tests/test_a0m_integrity_2026_11_21.py, "
             "tests/e1/js/a0m_generated_run.js, tests/e1/js/a0m_module_resolution.test.js and tests/e1/js/a0m_issue_log_followup.test.js",
     "reason": "tests and the offline driver are verification, not runtime dependencies; they are hashed in the A0m report, not listed here"},
    {"item": "Integration/system_integration/a0m_2026-11-21/ (mutation runner, recorded results and summary, SHA256SUMS) and docs/research/e1/a0m_dry_render_evidence_2026-11-21/ (the dry-render "
             "evidence, kept outside the roots scanned by the A0d guard that no other file may name recorder v2)",
     "reason": "generated evidence of this task, not runtime dependencies; hashed in the A0m report and in SHA256SUMS.txt"},
    {"item": "scripts/e1/e1_la2_render.py (read through A0e prepare, not edited, still pinned elsewhere)",
     "reason": "the pinned renderer still names e1_create_asset.js and recorder v1. The A0m orchestrator derives a NEW run from the v2-wired output and replaces the monitor and "
               "the workload module there; the pinned file is untouched"},
)
UNRESOLVED = tuple(a0l.UNRESOLVED) + (
    "The A0m dry render is OFFLINE ONLY: a rendered configuration is not an executed experiment. Neither recorder v3 nor e1_create_asset_issue_logged.js has been loaded by a real "
    "Caliper worker; the Node driver loads them from the rendered directory under a FAKE Caliper base class and a scripted clock.",
    "A JSON benchmark cannot carry options.health.sourceCount (a provider function), so the rendered recorder has no source-count provider. The generator issue log, read by "
    "the Python reader, is the declared-independent source count; its independence is DECLARED by data flow and was never verified live.",
    "Reader lag: a recorder callback whose generator record has not been read yet reads as recorder_exceeds_source (fail closed, invalid at that decision time) and resolves when "
    "the record is read. A genuine surplus does not resolve. The reader's poll cadence is not set, measured or rendered anywhere.",
    "The container paths in the rendered configuration (/hyperledger/caliper/workspace/runs/<runId>/...) are assumed from the pinned renderer; nothing has verified them inside a "
    "Caliper container.",
    "Static module resolution cannot check @hyperledger/caliper-core (and its package.json); only a Caliper worker provides it. Node built-ins (fs, path, crypto, os, perf_hooks) "
    "are assumed available.",
    "One worker only. A worker count other than 1 is refused before any file is created.",
)
OPEN_ITEMS = tuple(a0l.OPEN_ITEMS) + (
    "A0m: owner approval of the three A0m additions has not been given. No Caliper, Fabric, Docker, WSL, Prometheus, model or actuator was started; Node ran only offline drivers and tests.",
    "A0m: the dedicated validation channel name, the experimental cell and every timing value (tick interval, heartbeat interval, max_tick_gap_ms, max_callback_lag_ms) are "
    "operator-supplied placeholders; none is approved or scientifically justified.",
    "A0m: a future, separately authorized task must supply its own execution tool; this one has no execute phase.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0M_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "entry_point": "scripts/e1/e1_a0m_dry_render.py",
                              "phases": {"dry-render": "implemented, offline; reuses A0e prepare, derives a recorder-v3 + issue-logged run", "preflight": "implemented, offline",
                                         "execute": "refused (exit 5); not implemented"},
                              "selections": {"workload": "e1_create_asset_issue_logged/1", "recorder": "recorder v3 (explicit; see the dry-render orchestrator)", "workers": 1},
                              "approved": False, "live_run": False},
        experimental_configuration_provenance={"source": "operator-supplied parameters: prelive_params/1 (A0e) and a0m_render_params/1",
                                               "note": "no cell, channel, threshold, tick interval or heartbeat interval is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    manifest["changed_since_a0l"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0l"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0l"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0l_sha256": earlier[p], "a0m_sha256": current[p], "reason": "UNEXPLAINED: A0m changes no candidate file; add a reason"}
                                      for p in manifest["changed_since_a0l"]]
    manifest["superseded_note"] = ("A0m changed no file listed in the A0l manifest, so nothing is superseded. The pinned renderer, recorder v1, the legacy workload and every "
                                   "A0l source are preserved byte-for-byte; the dry render is a new file that derives a new directory from the A0e output.")
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0k", "changed_since_a0k", "removed_since_a0k"):               # A0l-era lists belong to the A0l manifest; they are not repeated as A0m's
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
              f"changed since A0l: {len(manifest['changed_since_a0l'])}; added: {len(manifest['added_since_a0l'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
