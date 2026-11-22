"""Candidate integrity manifest, A0o successor (2026-11-21): the A0m candidate files at their CURRENT hashes plus the A0o additions.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0m.json`` and every earlier manifest are NOT edited. A0o changed NO file that the A0m manifest lists (the pinned renderer,
recorder v1, the legacy workload, the A0e and A0m orchestrators and every A0l/A0m source are byte-identical), so ``changed_since_a0m`` and ``superseded_entries`` are empty and the
absence of a change is itself on the record. A0o adds six new runtime files: the SourceCount reader driver (library + command line), the post-hoc timing sampler (library + command
line) and the launch-variant isolation (library + command line).

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not approval and not authorization to run a
  workload, a recorder, a reader, Caliper, Fabric, a model or a forecast. Nothing is pinned here.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0o_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0o.json
    python scripts/e1/e1_a0o_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0o.json
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

import e1_a0m_candidate_manifest as a0m                   # noqa: E402  (supplies the A0m specs, exclusions, unresolved items and open items)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0o.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0m.json"
VERSION = "a0o_candidate_manifest 0.1 (2026-11-21)"

A0O_ADDITIONS = [
    pi.FileSpec("src/fabric_perf/controllers/sourcecount_reader.py", "SourceCount reader driver library: round-bound tail and bridge, reader identity and round-set validation, descriptor "
                "validation, inspect / replay modes, follow refused, source_count_evidence/1 builder, Stage 3 history hand-over", "sourcecount_reader_library"),
    pi.FileSpec("scripts/e1/e1_sourcecount_reader.py", "SourceCount reader driver command line (inspect, replay, follow refused); writes one new audit JSON file", "sourcecount_reader_entry_point"),
    pi.FileSpec("src/fabric_perf/controllers/timing_sampler.py", "post-hoc timing sampler library for T01-T06 (specification formulas, nearest-rank statistics, "
                "NOT_MEASURABLE_FROM_EXISTING_EVIDENCE with the missing dependency)", "timing_sampler_library"),
    pi.FileSpec("scripts/e1/e1_a0o_timing_sampler.py", "post-hoc timing sampler command line; fixture origins only; writes one new JSON file", "timing_sampler_entry_point"),
    pi.FileSpec("src/fabric_perf/controllers/launch_variant.py", "launch-variant identity and isolation library: variant derivation from generated benchmark files, qualified run id, "
                "occupied-directory and stale-manifest refusals, marker verification", "launch_variant_library"),
    pi.FileSpec("scripts/e1/e1_a0o_launch_variant.py", "variant-isolated A0m dry render (qualified run id, variant directory, sibling marker), variant preflight, collision check, execute refused",
                "launch_variant_entry_point"),
]
A0O_SPECS = list(a0m.A0M_SPECS) + A0O_ADDITIONS

EXCLUSIONS = tuple(a0m.EXCLUSIONS) + (
    {"item": "tests/a0o_support.py, tests/test_a0o_sourcecount_reader_2026_11_21.py, tests/test_a0o_stage3_2026_11_21.py, tests/test_a0o_timing_sampler_2026_11_21.py, "
             "tests/test_a0o_launch_variant_2026_11_21.py and tests/test_a0o_integrity_2026_11_21.py",
     "reason": "tests are verification, not runtime dependencies; they are hashed in the A0o report, not listed here"},
    {"item": "Integration/system_integration/a0o_2026-11-21/ (the pre-change collision reproduction, fixture outputs, SHA256SUMS) and the A0o report",
     "reason": "generated evidence and documentation of this task, not runtime dependencies; hashed in SHA256SUMS.txt"},
)
UNRESOLVED = tuple(a0m.UNRESOLVED) + (
    "The SourceCount reader driver has only run on synthetic scripted logs and on the stored scripted-clock A0m evidence. No live issue log has been read; follow mode is refused and "
    "its interface is designed but not implemented. Reader poll cadence and max_tick_gap_ms are caller-supplied and unapproved.",
    "The timing sampler computes fixture arithmetic. T01's direct end-of-write latency and T06's bridge delay are NOT_MEASURABLE_FROM_EXISTING_EVIDENCE (no end-of-write stamp in "
    "e1_issue_log/1, no ingest stamp in the recorder health bridge). T07 to T12 are outside A0o. No Windows-WSL clock agreement was measured.",
    "The launch-variant isolation renders the A0m variant under a qualified run id and a variant directory. A historical A0m render (unqualified run id, no marker) is still a valid "
    "historical artifact but is refused by the variant preflight; the A0e tool and its renders are unchanged and keep their unqualified run id.",
    "The coverage bridge's own instance name still ends in the literal ':r0' (issue_log_coverage.py is an A0l candidate file and was not edited); the round-carrying bridge is a subclass in "
    "sourcecount_reader.py.",
    "Restart is supported for the reader tail only; a journal bridge keeps state that is not checkpointed and a restart with a bridge is refused.",
)
OPEN_ITEMS = tuple(a0m.OPEN_ITEMS) + (
    "A0o: owner approval of the six A0o additions has not been given. No Caliper, Fabric, Docker, WSL, Prometheus, model or actuator was started; Node was not run by A0o.",
    "A0o: the dedicated validation channel, the experimental cell, the reader poll cadence, max_tick_gap_ms, max_callback_lag_ms, every heartbeat setting and every operator threshold "
    "remain unset or unapproved; the A0j candidate values are unverified engineering proposals.",
    "A0o: the live runbook still needs an execution tool, a clock probe, a live Stage 3 entry point and a separate authorization for each live step.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0O_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES,
                              "entry_points": {"render": "scripts/e1/e1_a0o_launch_variant.py render (variant-qualified A0m render, offline)",
                                               "preflight": "scripts/e1/e1_a0o_launch_variant.py preflight (variant checks + the unchanged A0m preflight)",
                                               "reader": "scripts/e1/e1_sourcecount_reader.py inspect | replay (follow refused)",
                                               "timing": "scripts/e1/e1_a0o_timing_sampler.py (fixture origins only)",
                                               "execute": "refused (exit 5) in every A0o tool; not implemented"},
                              "variants": {"a0e_recorder_v2_legacy_workload": "historical; unqualified run id; tool unchanged",
                                           "a0m_recorder_v3_issue_logged": "run id suffix -v3il; directory a0m-v3il/<render id>; sibling marker"},
                              "approved": False, "live_run": False},
        experimental_configuration_provenance={"source": "operator-supplied parameters: prelive_params/1 (A0e), a0m_render_params/1 and the reader/sampler arguments",
                                               "note": "no cell, channel, threshold, tick interval, poll interval or heartbeat interval is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    manifest["changed_since_a0m"] = sorted(p for p in current if p in earlier and current[p] != earlier[p])
    manifest["added_since_a0m"] = sorted(p for p in current if p not in earlier)
    manifest["removed_since_a0m"] = sorted(p for p in earlier if p not in current)
    manifest["superseded_entries"] = [{"path": p, "a0m_sha256": earlier[p], "a0o_sha256": current[p], "reason": "UNEXPLAINED: A0o changes no candidate file; add a reason"}
                                      for p in manifest["changed_since_a0m"]]
    manifest["superseded_note"] = ("A0o changed no file listed in the A0m manifest, so nothing is superseded. The pinned renderer, recorder v1, the legacy workload and every A0l and A0m source "
                                   "are preserved byte-for-byte; the six A0o files are new.")
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
    for key in ("added_since_a0l", "changed_since_a0l", "removed_since_a0l"):               # A0m-era lists belong to the A0m manifest; they are not repeated as A0o's
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
              f"changed since A0m: {len(manifest['changed_since_a0m'])}; added: {len(manifest['added_since_a0m'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
