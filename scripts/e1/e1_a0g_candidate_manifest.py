"""Candidate integrity manifest, A0g successor (2026-11-21): the A0e candidate files plus the Stage 3 runner and predictive dependencies.

``docs/research/e1/prelive_candidate_manifest_2026-11-21.json`` (A0e, 25 files) is NOT edited: it stays the record of what the A0e
candidate files were. This script writes a clearly versioned SUCCESSOR that lists those 25 entries again (same specs, so a changed
A0e file is detected, not hidden) and adds every file the Stage 3 runner imports, plus the runner and its CLI.

* status stays ``CANDIDATE_NOT_APPROVED`` and ``approved`` stays ``false``. Listing a file is not approval and not authorization to
  run a model, a forecast, Fabric or anything else.
* Frozen model artifacts, the frozen forecast selection, the stored series and the HFBTP dataset are NOT hashed or opened here. They are
  pinned by ``Integration/system_integration/offline_replay_inputs.json`` and are named in ``unresolved_dependencies``.
* Files are only read. The manifest is written to a path that must not exist. No process, network or container API is used.

    python scripts/e1/e1_a0g_candidate_manifest.py write  --out docs/research/e1/prelive_candidate_manifest_2026-11-21_a0g.json
    python scripts/e1/e1_a0g_candidate_manifest.py verify --manifest docs/research/e1/prelive_candidate_manifest_2026-11-21_a0g.json
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
for entry in (str(HERE), str(REPO / "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

import e1_prelive_validation as pv                       # noqa: E402  (offline by construction; supplies the A0e specs)
from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

SUCCESSOR_NAME = "prelive_candidate_manifest_2026-11-21_a0g.json"
SUPERSEDES = "docs/research/e1/prelive_candidate_manifest_2026-11-21.json"
VERSION = "a0g_candidate_manifest 0.1 (2026-11-21)"
CLOSURE_ROOTS = ("src/fabric_perf/controllers/stage3_shadow_runner.py", "src/fabric_perf/controllers/stage3_predictive.py")

C = "src/fabric_perf/controllers/"
A0G_ADDITIONS = [
    pi.FileSpec("scripts/e1/e1_stage3_shadow_runner.py", "Stage 3 shadow runner CLI: fixture mode; live mode and real predictive mode refuse", "stage3_runner_cli"),
    pi.FileSpec(C + "stage3_shadow_runner.py", "Stage 3 runner library: frames, three controllers, decision records, freshness limits", "stage3_runner"),
    pi.FileSpec(C + "stage3_predictive.py", "predictive contracts, guarded adapters, DISABLED real-inference builder", "predictive_contract"),
    pi.FileSpec(C + "adapters.py", "HFBTPArtifactPerformancePredictor, ApprovedMaxMessageCountSpace, ForecastResult (model interface)", "model_interface"),
    pi.FileSpec(C + "live_foundation.py", "StreamingEWMA / StreamingWorkload (frozen forecaster interface)", "forecaster_interface"),
    pi.FileSpec(C + "workload_history.py", "UTC-day-prefix arrival history store", "forecaster_interface"),
    pi.FileSpec("src/fabric_perf/forecasting/horizon_benchmark.py", "ewma_states recurrence used by StreamingEWMA", "forecaster_interface"),
    pi.FileSpec("src/fabric_perf/forecasting/flashbots.py", "baseline forecasts imported by adapters", "forecaster_interface"),
    pi.FileSpec("src/fabric_perf/forecasting/__init__.py", "package initializer", "forecaster_interface"),
    pi.FileSpec("src/fabric_perf/models/artifacts.py", "load_hfbtp_artifact (the only joblib loader; not called by the fixture path)", "model_interface"),
    pi.FileSpec("src/fabric_perf/models/baseline.py", "FEATURE_COLUMNS and add_engineered_features (feature schema)", "model_interface"),
    pi.FileSpec("src/fabric_perf/models/base.py", "model base", "model_interface"),
    pi.FileSpec("src/fabric_perf/models/__init__.py", "package initializer", "model_interface"),
    pi.FileSpec("src/fabric_perf/data/hfbtp.py", "HFBTP loader imported by the artifact module (import-time dependency)", "import_dependency"),
    pi.FileSpec("src/fabric_perf/data/mmbpd.py", "MMBPD loader imported with the data package (import-time dependency; not used)", "import_dependency"),
    pi.FileSpec("src/fabric_perf/data/base.py", "data base", "import_dependency"),
    pi.FileSpec("src/fabric_perf/data/__init__.py", "package initializer", "import_dependency"),
    pi.FileSpec(C + "base.py", "controller state, FabricConfig, StaticController", "controller_base"),
    pi.FileSpec(C + "strategies.py", "ReactiveController and PredictiveController (unchanged algorithms)", "controllers"),
    pi.FileSpec(C + "harness.py", "ShadowActuator", "shadow_actuator"),
    pi.FileSpec(C + "research_stages.py", "assert_no_live_actuation stage guard", "stage_guard"),
    pi.FileSpec(C + "telemetry_freshness.py", "FreshnessPolicy: source skew, future skew, clock discrepancy", "freshness_policy"),
    pi.FileSpec(C + "telemetry_errors.py", "telemetry read-failure structures", "telemetry_adapter"),
    pi.FileSpec(C + "offline_replay.py", "verified_inputs (pin check used by the disabled real-mode builder)", "pin_verifier"),
    pi.FileSpec(C + "config_evidence.py", "configuration evidence (imported by the controllers package)", "import_dependency"),
    pi.FileSpec(C + "common_replay.py", "replay helpers (imported by the controllers package)", "import_dependency"),
    pi.FileSpec(C + "metric_baseline.py", "metric baseline (imported by the controllers package)", "import_dependency"),
    pi.FileSpec(C + "predictive_pipeline.py", "predictive replay pipeline (imported by the controllers package; not used by Stage 3)", "import_dependency"),
    pi.FileSpec(C + "reactive_pipeline.py", "reactive replay pipeline (imported by the controllers package; not used by Stage 3)", "import_dependency"),
    pi.FileSpec(C + "__init__.py", "controllers package initializer", "import_dependency"),
    pi.FileSpec("src/fabric_perf/__init__.py", "package initializer", "import_dependency"),
    pi.FileSpec("Integration/system_integration/a0g_2026-11-21/fixtures/controller_policy_v2.json",
                "EXAMPLE stage3_controller_policy/2 (synthetic placeholders equal to library defaults; no scientific basis)", "example_input"),
]
A0G_SPECS = list(pv.CANDIDATE_SPECS) + A0G_ADDITIONS

EXCLUSIONS = (
    {"item": "HFBTP model artifacts (experiments/artifacts/hfbtp/{metadata.json,throughput.joblib,avg_latency.joblib})",
     "reason": "frozen; pinned in Integration/system_integration/offline_replay_inputs.json; not opened, hashed or loaded by this task"},
    {"item": "frozen forecast selection (selection_frozen.json) and stored Flashbots series", "reason": "frozen; pinned in offline_replay_inputs.json; not opened"},
    {"item": "HFBTP.csv and MMBPD.csv", "reason": "raw data, immutable, gitignored"},
    {"item": "test files (tests/test_a0*.py, tests/a0*_support.py)", "reason": "verification, not runtime dependencies"},
    {"item": "third-party packages (pandas, numpy, scikit-learn, joblib)", "reason": "versions are recorded by the environment, not hashed here; the artifact loader refuses a different scikit-learn version"},
)
UNRESOLVED = (
    "A real predictive route needs a live or captured WORKLOAD HISTORY (StreamingWorkload). The runner has none; the fixture history is a placeholder.",
    "Real mode needs REAL_INFERENCE_AUTHORIZED changed in stage3_predictive.py: a source edit that changes that file's hash and so this manifest.",
    "The performance model's Orderers feature must come from a verified channel configuration block; the fixture's value is not verified.",
    "HFBTP training domain is 10-200 tx/s. A forecast below 10 tx/s is refused (not clipped), so the predictive controller abstains there.",
    "Freshness limits in the example policy are placeholders. The operator must supply approved values; no scientific justification is claimed.",
    "Windows-WSL clock agreement has not been measured; no offset is compensated.",
)
OPEN_ITEMS = tuple(pv.CANDIDATE_OPEN_ITEMS) + (
    "A0g: owner approval of every PENDING_OWNER_APPROVAL file, including the A0g additions, has not been given.",
    "A0g: separate owner authorization is required for any frozen-model or frozen-forecaster computation.",
)


def local_import_closure(roots=CLOSURE_ROOTS, repo: Path = REPO) -> set[str]:
    """Repo-relative paths of every ``fabric_perf`` module reachable by import from ``roots`` (static AST walk; imports nothing)."""
    src = repo / "src"

    def resolve(module: str) -> list[Path]:
        base = src.joinpath(*module.split("."))
        if base.with_suffix(".py").is_file():
            return [base.with_suffix(".py")]
        return [base / "__init__.py"] if (base / "__init__.py").is_file() else []

    def package_of(path: Path) -> str:
        parts = list(path.relative_to(src).with_suffix("").parts)
        return ".".join(parts[:-1])          # __init__ files: their own package is the directory (last part is "__init__")

    seen: set[Path] = set()
    stack = [repo / r for r in roots]
    while stack:
        path = stack.pop()
        if path in seen:
            continue
        seen.add(path)
        package = package_of(path)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names: set[str] = set()
            if isinstance(node, ast.Import):
                names |= {alias.name for alias in node.names}
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    parts = package.split(".")
                    parts = parts[:len(parts) - (node.level - 1)]
                    module = ".".join(parts + ([node.module] if node.module else []))
                else:
                    module = node.module or ""
                names.add(module)
                names |= {f"{module}.{alias.name}" for alias in node.names}
            for name in names:
                if not name.startswith("fabric_perf"):
                    continue
                pieces = name.split(".")
                for index in range(1, len(pieces) + 1):          # every parent package initializer runs too
                    for target in resolve(".".join(pieces[:index])):
                        if target not in seen:
                            stack.append(target)
    return {p.relative_to(repo).as_posix() for p in seen}


def build(repo: Path = REPO, *, created_utc: str | None = None) -> dict:
    manifest = pi.build_candidate_manifest(
        repo, A0G_SPECS, created_utc=created_utc, open_items=OPEN_ITEMS,
        launch_configuration={"manifest_tool": VERSION, "supersedes": SUPERSEDES, "entry_point": "scripts/e1/e1_stage3_shadow_runner.py",
                              "modes": {"fixture": "implemented, offline", "live-shadow": "refusal only (exit 5)",
                                        "predictive real": "prepared, DISABLED (exit 6)"}},
        experimental_configuration_provenance={"source": "operator-supplied parameters (prelive_params/1) and controller policy (stage3_controller_policy/2)",
                                               "note": "no cell, threshold or freshness limit is approved; none is claimed scientifically justified"})
    old = repo / SUPERSEDES
    manifest["supersedes"] = {"path": SUPERSEDES, "sha256": pi.sha256_file(old), "note": "left byte-identical; its 25 entries are repeated here"}
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
        print(f"candidate manifest -> {args.out} ({len(manifest['entries'])} files, sha256 {digest}); status {manifest['status']}")
        return 0
    findings = pi.verify_candidate_manifest(json.loads(args.manifest.read_text(encoding="utf-8")), REPO)
    print(json.dumps({"findings": [f.as_dict() for f in findings]}, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
