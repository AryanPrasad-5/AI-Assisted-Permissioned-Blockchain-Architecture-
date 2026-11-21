"""Candidate integrity manifest, A0i successor (2026-11-21): the A0h candidate files plus the coverage-attestation, collector-health,
abstention-accounting and real-inference-smoke-test preparation files.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0h.json`` (A0h, 60 files), the A0g manifest and the A0e manifest are NOT
edited. This script writes a clearly versioned SUCCESSOR that lists every A0h entry again with its CURRENT hash (A0i edited three of them:
the captured-history adapter, the Stage 3 runner and the predictive module) and adds the new A0i files. ``changed_since_a0h`` names each
entry whose hash differs from the A0h manifest, so a changed file is shown, not hidden; ``changed_since_a0g`` does the same against A0g.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. A candidate hash is not an approved pin. Listing a file is not
  approval and not authorization to run a model, a forecast, a collector, Fabric or anything else.
* ``live_integration.py`` is unchanged and still pinned by the A0e manifest. The frozen model artifacts, frozen selection, stored series and
  stored forecast tables are NOT listed (they are pinned by ``Integration/system_integration/offline_replay_inputs.json`` and named in
  ``exclusions``); the smoke-test contract records their hashes separately.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0i_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0i.json
    python scripts/e1/e1_a0i_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0i.json
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

import e1_a0h_candidate_manifest as a0h                   # noqa: E402  (supplies the A0h specs, exclusions and the import-closure walker via a0g)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0i.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0h.json"
A0G_MANIFEST = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0g.json"
VERSION = "a0i_candidate_manifest 0.1 (2026-11-21)"
C = "src/fabric_perf/controllers/"
CLOSURE_ROOTS = a0h.CLOSURE_ROOTS + (C + "workload_coverage.py", C + "collector_health.py", C + "abstention_accounting.py", C + "real_inference_smoke.py")

A0I_ADDITIONS = [
    pi.FileSpec(C + "workload_coverage.py", "coverage classes, hash-chained collector-health evidence verification, attestation document: when a zero may be believed", "coverage_contract"),
    pi.FileSpec(C + "collector_health.py", "collector-health journal WRITER (offline contract; wired into no recorder yet)", "collector_health"),
    pi.FileSpec(C + "abstention_accounting.py", "explicit-denominator accounting of valid, shared-invalid, initialization and controller-specific abstention frames", "abstention_accounting"),
    pi.FileSpec(C + "real_inference_smoke.py", "DISABLED real-inference smoke-test contract, input hash verification and output validator", "smoke_preparation"),
    pi.FileSpec("scripts/e1/e1_a0i_real_inference_smoke.py", "smoke-test CLI: spec, verify-inputs, and a run command that always refuses (exit 6)", "smoke_preparation"),
    pi.FileSpec("scripts/e1/e1_a0i_coverage_demo.py", "synthetic-day demonstration of genuine zero, unknown zero and interrupted collector through Stage 3", "demonstration"),
]
A0I_SPECS = list(a0h.A0H_SPECS) + A0I_ADDITIONS

EXCLUSIONS = tuple(a0h.EXCLUSIONS) + (
    {"item": "tests/a0i_support.py and the five tests/test_a0i_*_2026_11_21.py files",
     "reason": "tests are verification, not runtime dependencies; they are hashed in the A0i report, not listed here"},
    {"item": "Integration/system_integration/a0i_2026-11-21/ (demonstration runs, mutation script and results)",
     "reason": "generated evidence of this task, not runtime dependencies; hashed in the A0i report"},
)
UNRESOLVED = tuple(a0h.UNRESOLVED) + (
    "No running recorder writes collector-health journals. collector_health.CollectorHealthJournal is an offline-tested contract that is wired "
    "into nothing; until the arrival recorder emits start, heartbeat, error and stop rows, no live interval can be certified and the strict "
    "predictive path fails closed.",
    "The archived Flashbots days carry no coverage evidence (sources column empty; no heartbeat, sequence or lifecycle record). Their zero "
    "seconds stay UNKNOWN under the strict contract; benchmark_zero_fill reproduces the stored benchmark input, is labelled non-live and never satisfies the contract.",
    "CoveragePolicy.max_heartbeat_gap_s defaults to a placeholder with no scientific justification; an experiment must set and justify its own.",
    "The real-inference smoke test is a specification only (DESIGNED BUT NOT EXECUTED). REAL_INFERENCE_AUTHORIZED is still a single False constant; the "
    "post-authorization wiring is deliberately not written; the Orderers value must be supplied from a verified channel configuration.",
    "2026-09-21 12:00Z lies in the half-day used for model selection: a smoke-test match with the stored forecast is reproducibility, not independent validation.",
    "The stored below-grid proportion (528 of 2,150 = 24.6%) is a historical proxy for predictive abstention, not a measured live abstention rate.",
    "Abstention accounting counts recommendations, not realised performance; no controller comparison or improvement may be computed from shadow-only recommendations.",
    "live_integration.py is unchanged (pinned by the A0e manifest); the Stage 3 freshness subclass workaround remains. Any change to a pinned file needs a separate successor-pin task.",
)
OPEN_ITEMS = tuple(a0h.OPEN_ITEMS) + (
    "A0i: owner approval of the A0i additions and of the three edited A0h files (captured_history.py, stage3_predictive.py, stage3_shadow_runner.py) has not been given.",
    "A0i: no recorder was instrumented, no collector was started, and no real inference was run; each needs separate owner authorization.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0I_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "entry_point": "scripts/e1/e1_stage3_shadow_runner.py",
                              "modes": {"fixture": "implemented, offline; optional captured history with strict coverage evidence (A0i)",
                                        "live-shadow": "refusal only (exit 5)", "predictive real": "prepared, DISABLED (exit 6)",
                                        "real-inference smoke test": "specification only; run command refuses (exit 6)"}},
        experimental_configuration_provenance={"source": "operator-supplied parameters (prelive_params/1) and controller policy (stage3_controller_policy/2)",
                                               "note": "no cell, threshold, freshness limit or heartbeat gap is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    manifest["changed_since_a0h"] = sorted(e["path"] for e in manifest["entries"] if e["path"] in earlier and e["sha256"] != earlier[e["path"]])
    manifest["added_since_a0h"] = sorted(e["path"] for e in manifest["entries"] if e["path"] not in earlier)
    older = {e["path"]: e["sha256"] for e in json.loads((repo / A0G_MANIFEST).read_text(encoding="utf-8"))["entries"]}
    manifest["changed_since_a0g"] = sorted(e["path"] for e in manifest["entries"] if e["path"] in older and e["sha256"] != older[e["path"]])
    manifest["manifest_version"] = VERSION
    manifest["exclusions"] = [dict(item) for item in EXCLUSIONS]
    manifest["unresolved_dependencies"] = list(UNRESOLVED)
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
              f"changed since A0h: {len(manifest['changed_since_a0h'])}; added: {len(manifest['added_since_a0h'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
