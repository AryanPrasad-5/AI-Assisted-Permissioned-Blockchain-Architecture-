"""Candidate integrity manifest, A0h successor (2026-11-21): the A0g candidate files plus the captured-history adapter and the analysis.

``docs/research/e1/prelive_candidate_manifest_2026-11-21_a0g.json`` (A0g, 57 files) and the A0e manifest are NOT edited. This script
writes a clearly versioned SUCCESSOR that lists every A0g entry again, with its CURRENT hash (A0h edited three of them: the Stage 3
runner, its CLI and the predictive module), and adds the new A0h files. ``changed_since_a0g`` names each entry whose hash differs from
the A0g manifest, so a changed file is shown, not hidden.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. Listing a file is not approval and not authorization to
  run a model, a forecast, Fabric or anything else.
* Frozen model artifacts, the frozen selection, the stored series and the stored forecast tables are NOT opened here; they are pinned
  by ``Integration/system_integration/offline_replay_inputs.json`` and are named in ``exclusions``.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0h_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0h.json
    python scripts/e1/e1_a0h_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0h.json
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

import e1_a0g_candidate_manifest as a0g                  # noqa: E402  (supplies the A0g specs, exclusions and the import-closure walker)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0h.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0g.json"
VERSION = "a0h_candidate_manifest 0.1 (2026-11-21)"
C = "src/fabric_perf/controllers/"
CLOSURE_ROOTS = a0g.CLOSURE_ROOTS + (C + "captured_history.py", C + "forecast_boundary.py")

A0H_ADDITIONS = [
    pi.FileSpec(C + "captured_history.py", "captured workload-history adapter: stored arrival observations -> UTC-day-prefix history store", "history_adapter"),
    pi.FileSpec(C + "forecast_boundary.py", "strict 10-200 tx/s boundary classification and counts for stored forecasts (pure functions)", "boundary_analysis"),
    pi.FileSpec("scripts/e1/e1_a0h_forecast_boundary.py", "read-only analysis of already-stored 60 s forecasts against the HFBTP workload range", "boundary_analysis"),
]
A0H_SPECS = list(a0g.A0G_SPECS) + A0H_ADDITIONS

EXCLUSIONS = tuple(a0g.EXCLUSIONS) + (
    {"item": "stored Flashbots series (data/processed/flashbots/series_*_1s.csv.gz) and stored forecast tables "
             "(experiments/results/flashbots_horizon_benchmark_v2, Flashbots/results/FB-FC-v1)",
     "reason": "historical data and results; read by tests and the analysis script, never edited; pinned or hashed in the A0h report, not listed here"},
)
UNRESOLVED = (
    "A captured workload history now exists for FIXTURE mode only: stored Ethereum mempool arrivals (2026-09-21, 2026-10-12), not Fabric observations. "
    "Real mode still has no live or Fabric workload-history source.",
    "Zero-count seconds in the stored 1 s series cannot be told from missing collector coverage. Under the default verified_only policy the first "
    "zero (second 207 on 2026-09-21, 114 on 2026-10-12) makes the UTC-day prefix incomplete; the benchmark_zero_fill policy is explicit, labelled and not live.",
    "Real mode needs REAL_INFERENCE_AUTHORIZED changed in stage3_predictive.py: a source edit that changes that file's hash and so this manifest.",
    "The performance model's Orderers feature must come from a verified channel configuration block; the fixture's value is not verified.",
    "HFBTP training domain is 10-200 tx/s. A forecast below 10 tx/s is refused (not clipped), so the predictive controller abstains there. "
    "Stored 60 s forecasts: 528 of 2,150 (24.6%) are below 10 tx/s; this is a proxy, not a measured abstention rate.",
    "Freshness limits in the example policy are placeholders. The operator must supply approved values; no scientific justification is claimed.",
    "live_integration.py is pinned by the A0e candidate manifest and two tests; the Stage 3 freshness policy is injected at the Stage 3 boundary "
    "(Stage3ShadowRunner) instead of through IntegratedControllerRunner's constructor.",
    "Windows-WSL clock agreement has not been measured; no offset is compensated.",
)
OPEN_ITEMS = tuple(a0g.OPEN_ITEMS) + (
    "A0h: owner approval of the A0h additions and of the three edited A0g files has not been given.",
    "A0h: the frozen EWMA and the frozen HFBTP model were not executed; separate owner authorization is required.",
)


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0H_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "entry_point": "scripts/e1/e1_stage3_shadow_runner.py",
                              "modes": {"fixture": "implemented, offline; optional captured history (A0h)", "live-shadow": "refusal only (exit 5)",
                                        "predictive real": "prepared, DISABLED (exit 6)"}},
        experimental_configuration_provenance={"source": "operator-supplied parameters (prelive_params/1) and controller policy (stage3_controller_policy/2)",
                                               "note": "no cell, threshold or freshness limit is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old),
                              "note": "left byte-identical; its entries are repeated here with their current hashes"}
    earlier = {e["path"]: e["sha256"] for e in json.loads(old.read_text(encoding="utf-8"))["entries"]}
    manifest["changed_since_a0g"] = sorted(e["path"] for e in manifest["entries"] if e["path"] in earlier and e["sha256"] != earlier[e["path"]])
    manifest["added_since_a0g"] = sorted(e["path"] for e in manifest["entries"] if e["path"] not in earlier)
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
              f"changed since A0g: {len(manifest['changed_since_a0g'])}; added: {len(manifest['added_since_a0g'])}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
