"""Stage 3 shadow runner entry point (task A0f, 2026-11-21): offline fixture mode runs; live mode is refused.

  fixture      Mode A. Run the complete Stage 3 pipeline (client + ledger evidence -> validated frames -> static, reactive and
               predictive controllers -> decision records) over a recorded or synthetic FIXTURE, into a NEW directory. Nothing is
               contacted: the ledger, the client rows, the Prometheus endpoint, the clock, the performance model and the
               forecaster are all fixtures. The frozen HFBTP model is never loaded. No configuration is ever applied.
  live-shadow  Mode B. Prints which independent gates a future live shadow run is missing and exits 5. It opens no connection
               and has no flag, environment variable or default that turns a live run on. There is no authorization mechanism in
               the repository, so it cannot be satisfied today.

Inputs (all required, no defaults): ``--params`` (prelive_params/1, the A0e contract), ``--controller-policy``
(stage3_controller_policy/1) and, for fixture mode, ``--fixture`` (stage3_fixture/1) and ``--out-dir`` (must not exist).

A0h: ``fixture`` accepts an optional CAPTURED workload history (``--captured-history`` with its SHA-256 and UTC day): a stored 1 s Ethereum
mempool arrival series (NOT Fabric observations) replaces the placeholder history. It needs a stage3_controller_policy/2 policy with
predictive.horizon_seconds 60. The forecast and the performance table remain labelled fixtures.

Exit codes: 0 completed, 1 a comparability or consistency check failed, 2 invalid request (nothing created), 4 the run ended at
an operator-supplied safety limit (as designed, not a tool failure), 5 live mode refused, 6 real predictive mode refused (A0g), 130 interrupted.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import captured_history as ch                # noqa: E402
from fabric_perf.controllers import channel_selection as cs               # noqa: E402
from fabric_perf.controllers import prelive_contract as pc                # noqa: E402
from fabric_perf.controllers import stage3_predictive as sp                # noqa: E402
from fabric_perf.controllers import stage3_shadow_runner as runner        # noqa: E402

EXIT_OK, EXIT_INCONSISTENT, EXIT_INVALID, EXIT_SAFETY_STOP, EXIT_LIVE_REFUSED, EXIT_INTERRUPTED = 0, 1, 2, 4, 5, 130
EXIT_REAL_INFERENCE_REFUSED = 6   # A0g: --predictive-mode real is prepared but disabled
DEFAULT_CANDIDATE_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21.json"

TOOL_FILES = {
    "cli": "scripts/e1/e1_stage3_shadow_runner.py",
    "runner_module": "src/fabric_perf/controllers/stage3_shadow_runner.py",
    "runner_library": "src/fabric_perf/controllers/live_integration.py",
    "frame_quality": "src/fabric_perf/controllers/stage3_frame_quality.py",
    "telemetry_adapter": "src/fabric_perf/controllers/telemetry.py",
    "ledger_collector": "src/fabric_perf/controllers/ledger_evidence.py",
    "predictive_contract": "src/fabric_perf/controllers/stage3_predictive.py",
    "captured_history": "src/fabric_perf/controllers/captured_history.py",
    "controllers": "src/fabric_perf/controllers/strategies.py",
    "controller_base": "src/fabric_perf/controllers/base.py",
    "parameter_contract": "src/fabric_perf/controllers/prelive_contract.py",
    "channel_policy": "src/fabric_perf/controllers/channel_selection.py",
}


def prelive_link(run_dir: Path, params_values: dict[str, Any]) -> dict[str, Any]:
    """Tie a fixture run to an A0e prepared directory: same parameters, and an offline preflight that has not failed.

    Imports the A0e orchestrator only here (it imports the pinned renderer); nothing is executed, and the preflight is run with
    ``write=False`` so the prepared directory is not modified.
    """
    sys.path.insert(0, str(HERE))
    import e1_prelive_validation as prelive                              # noqa: E402  (offline by construction)
    run_dir = Path(run_dir)
    launch = json.loads((run_dir / "launch_manifest.json").read_text(encoding="utf-8"))
    stored = json.loads((run_dir / "params.validated.json").read_text(encoding="utf-8"))["values"]
    if stored != params_values:
        raise runner.Stage3RunnerError("the supplied parameters differ from the prepared directory's validated parameters")
    result = prelive.preflight(run_dir, write=False)
    if result["offline_preflight"] == prelive.FAIL:
        failed = [c["check"] if "check" in c else c for c in result["checks"] if c["status"] == prelive.FAIL]
        raise runner.Stage3RunnerError(f"the prepared directory fails its offline preflight: {failed}")
    return {"prepared_run_dir_name": run_dir.name, "launch_manifest_sha256": runner.sha256_file(run_dir / "launch_manifest.json"),
            "launch_status": launch.get("status"), "offline_preflight": result["offline_preflight"],
            "live_ready": result["live_ready"],
            "note": "an offline preflight is not a live validation and not an authorization"}


def load_captured_history(args: argparse.Namespace, forecast_horizon_s: int) -> "ch.CapturedHistory | None":
    """Build the optional captured history (A0h) from the CLI flags. Reads the stored series (its SHA-256 must match) and nothing else."""
    if args.captured_history is None:
        for flag in ("captured_history_sha256", "captured_history_utc_day", "coverage_attestation"):
            if getattr(args, flag) is not None:
                raise ch.CapturedHistoryError("captured_history_flags_incomplete", f"--{flag.replace('_', '-')} needs --captured-history")
        return None
    if not args.captured_history_sha256 or not args.captured_history_utc_day:
        raise ch.CapturedHistoryError("captured_history_flags_incomplete",
                                      "--captured-history needs --captured-history-sha256 and --captured-history-utc-day (nothing is inferred)")
    source = ch.CapturedSource(source_id=args.captured_history_source_id or f"flashbots-mempool-{args.captured_history_utc_day}",
                               utc_day=args.captured_history_utc_day)
    binned = ch.load_binned_series(args.captured_history, source, expected_sha256=args.captured_history_sha256)
    attestation = None if args.coverage_attestation is None else ch.CoverageAttestation.from_file(args.coverage_attestation)
    return ch.CapturedHistory(binned, zero_policy=args.zero_policy, attestation=attestation, forecast_horizon_s=forecast_horizon_s)


def cmd_fixture(args: argparse.Namespace) -> int:
    if args.predictive_mode == sp.PREDICTIVE_MODE_REAL:
        # Refused before any input file is opened: there is no flag, environment variable or file that enables real inference.
        print("REFUSED: real predictive inference (frozen HFBTP model and frozen EWMA) is not authorized; the fixture mode is the "
              "only authorized predictive mode. Nothing was read or created.")
        return EXIT_REAL_INFERENCE_REFUSED
    try:
        inputs = runner.load_inputs(args.params, args.controller_policy, args.fixture,
                                    require_operator_freshness=args.require_operator_freshness)
        link = None if args.prelive_run_dir is None else prelive_link(args.prelive_run_dir, dict(inputs.values))
        history = load_captured_history(args, inputs.policy.predictive["horizon_seconds"])
    except (runner.Stage3RunnerError, cs.ChannelSelectionError, pc.PreliveParameterError, ch.CapturedHistoryError, OSError, ValueError, KeyError) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return EXIT_INVALID
    try:
        manifest = runner.run_fixture_shadow(inputs, args.out_dir, tool_files={k: REPO / v for k, v in TOOL_FILES.items()},
                                             prelive_link=link, captured_history=history)
    except FileExistsError as exc:
        print(f"REFUSED: {exc}")
        return EXIT_INVALID
    except runner.Stage3RunnerError as exc:
        print(f"FAILED: {exc}")
        return EXIT_INCONSISTENT
    summary = json.loads((Path(args.out_dir) / "run_summary.json").read_text(encoding="utf-8"))
    print(json.dumps({"status": manifest["status"], "evidence_class": manifest["evidence_class"], "run_dir": str(args.out_dir),
                      "frames_processed": summary["frames_processed"], "frame_status_counts": summary["frame_status_counts"],
                      "controller_outcome_counts": summary["controller_outcome_counts"], "stop_reasons": summary["stop_reasons"],
                      "comparable": summary["comparability"]["comparable"], "configuration_changes_applied": 0,
                      "live_ready": False}, indent=2))
    return {"COMPLETED": EXIT_OK, "STOPPED_BY_SAFETY_LIMIT": EXIT_SAFETY_STOP, "INTERRUPTED": EXIT_INTERRUPTED}.get(
        manifest["status"], EXIT_INCONSISTENT)


def cmd_live(args: argparse.Namespace) -> int:
    report = runner.evaluate_live_gates(args.params, candidate_manifest_path=args.candidate_manifest, repo_root=REPO,
                                        prelive_run_dir=args.prelive_run_dir)
    print(json.dumps(report, indent=2))
    return EXIT_LIVE_REFUSED


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    fx = sub.add_parser("fixture", help="Mode A: offline fixture run (authorized)")
    fx.add_argument("--params", required=True, type=Path)
    fx.add_argument("--controller-policy", required=True, type=Path)
    fx.add_argument("--fixture", required=True, type=Path)
    fx.add_argument("--out-dir", required=True, type=Path)
    fx.add_argument("--prelive-run-dir", type=Path, help="optional A0e prepared directory to tie this run to")
    fx.add_argument("--predictive-mode", choices=sp.PREDICTIVE_MODES, default=sp.PREDICTIVE_MODE_FIXTURE,
                    help="fixture (default, authorized) or real (prepared but disabled: exits 6 without reading anything)")
    fx.add_argument("--require-operator-freshness", action="store_true",
                    help="refuse a stage3_controller_policy/1 file: require operator-supplied source-skew, future-skew and "
                         "clock-discrepancy limits (stage3_controller_policy/2)")
    fx.add_argument("--captured-history", type=Path,
                    help="A0h: a stored flashbots_series_1s/1 file (Ethereum mempool arrivals, NOT Fabric observations) used as the workload history")
    fx.add_argument("--captured-history-sha256", help="required with --captured-history: the file's expected SHA-256")
    fx.add_argument("--captured-history-utc-day", help="required with --captured-history: the UTC day (YYYY-MM-DD) the file covers")
    fx.add_argument("--captured-history-source-id", help="optional source identity recorded in every evidence row")
    fx.add_argument("--zero-policy", choices=ch.ZERO_POLICIES, default=ch.ZERO_POLICY_VERIFIED_ONLY,
                    help="verified_only (default): a zero-count second is genuine only inside --coverage-attestation, else unknown coverage "
                         "and the UTC-day prefix is incomplete; benchmark_zero_fill: reproduces the stored benchmark's zero-fill "
                         "(coverage UNVERIFIED, labelled)")
    fx.add_argument("--coverage-attestation", type=Path, help="captured_history_coverage/1 file naming the intervals the source was verified listening")
    live = sub.add_parser("live-shadow", help="Mode B: reports the missing gates and refuses (exit 5)")
    live.add_argument("--params", required=True, type=Path)
    live.add_argument("--candidate-manifest", type=Path, default=None)
    live.add_argument("--prelive-run-dir", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return cmd_fixture(args) if args.mode == "fixture" else cmd_live(args)


if __name__ == "__main__":
    sys.exit(main())
