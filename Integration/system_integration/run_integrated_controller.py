"""Run all three controllers through the shared integration contract.

The command is deterministic replay/shadow mode by default and never submits
Fabric configuration updates. Live mode is intentionally a library-only path:
it requires an explicitly constructed verified actuator for every strategy and
``enable_live=True`` in :class:`IntegratedControllerRunner`.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers import (  # noqa: E402
    ApprovedMaxMessageCountSpace,
    FabricConfig,
    FlashbotsSeriesForecaster,
    HFBTPArtifactPerformancePredictor,
    IntegratedControllerRunner,
    PredictiveController,
    ReactiveController,
    StaticController,
    StopPolicy,
    load_jsonl_frames,
)
from fabric_perf.controllers.stage3_frame_quality import Stage3FrameQualityContract  # noqa: E402


def parse_map(text: str) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for item in text.split(","):
        left, sep, right = item.partition(":")
        if not sep:
            raise ValueError("mapping must contain comma-separated FabricB:HFBTPBlockSize pairs")
        mapping[int(left)] = int(right)
    return mapping


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--hfbtp-artifact-dir", type=Path, required=True)
    parser.add_argument("--flashbots-series", type=Path, required=True)
    parser.add_argument("--flashbots-baseline", default="ewma_alpha_0.2")
    parser.add_argument("--horizon-seconds", type=int, default=10)
    # No block size is assumed. Earlier versions defaulted --batch-size to 40 and the candidates to
    # 40,45, which silently mislabelled any channel that is not at B=40 (2026-11-17, fix 1).
    parser.add_argument("--candidate-batch-sizes", required=True,
                        help="comma-separated approved MaxMessageCount values (no default is assumed)")
    parser.add_argument("--hfbtp-block-map", required=True,
                        help="FabricB:HFBTPBlockSize pairs for exactly the approved candidates")
    parser.add_argument("--batch-size", type=int, required=True,
                        help="the channel's MaxMessageCount; must equal the first frame's recorded configuration")
    parser.add_argument("--batch-timeout-ms", type=int, default=None,
                        help="optional assertion about the recorded BatchTimeout; the recorded value is used")
    parser.add_argument("--preferred-max-bytes", type=int, default=None,
                        help="optional assertion about the recorded PreferredMaxBytes; the recorded value is used")
    parser.add_argument("--latency-high-ms", type=float, default=1500.0)
    parser.add_argument("--latency-low-ms", type=float, default=1200.0)
    parser.add_argument("--queue-high", type=float)
    parser.add_argument("--queue-low", type=float)
    parser.add_argument("--required-fields", default="",
                        help="comma-separated critical ControllerState fields; empty for replay compatibility")
    parser.add_argument("--max-telemetry-age-s", type=float, default=5.0)
    parser.add_argument("--max-queue-depth", type=float)
    parser.add_argument("--stage3-frame-quality", action="store_true",
                        help="opt in to the Stage 3 frame-quality contract: the client measurements are required of every "
                             "frame, a frame needs a continuous client source and a known age, every controller abstains "
                             "on an unusable frame, and the run stops after --max-consecutive-abstentions abstained frames "
                             "in a row. Off by default.")
    parser.add_argument("--max-consecutive-abstentions", type=int, default=None,
                        help="operator-supplied positive integer; REQUIRED with --stage3-frame-quality, refused without it. "
                             "A safety stop, not a statistical threshold; no default is assumed.")
    parser.add_argument("--max-initialization-frames", type=int, default=None,
                        help="operator-supplied positive integer; OPTIONAL with --stage3-frame-quality, refused without it. "
                             "Allows up to this many warm-up frames at the start of a run, during which committed "
                             "throughput has no interval yet (only the ledger baseline exists): every controller abstains "
                             "with a distinct reason and the warm-up does not spend --max-consecutive-abstentions. One more "
                             "warm-up frame than this stops the run. Omitted: a first frame without committed throughput is "
                             "an ordinary abstention. No default and no scientific value is assumed.")
    args = parser.parse_args(argv)
    # Checked first, before any file is read or any model is loaded: a Stage 3 run without a bound does not start.
    frame_quality = None
    if args.stage3_frame_quality:
        try:
            options = {"max_consecutive_abstentions": args.max_consecutive_abstentions}
            if args.max_initialization_frames is not None:
                options["max_initialization_frames"] = args.max_initialization_frames
            frame_quality = Stage3FrameQualityContract.from_mapping(options)
        except ValueError as exc:
            parser.error(f"--stage3-frame-quality needs --max-consecutive-abstentions (and a positive "
                         f"--max-initialization-frames if given): {exc}")
    elif args.max_consecutive_abstentions is not None:
        parser.error("--max-consecutive-abstentions applies only with --stage3-frame-quality")
    elif args.max_initialization_frames is not None:
        parser.error("--max-initialization-frames applies only with --stage3-frame-quality")
    if args.out_dir.exists():
        parser.error(f"refusing to overwrite {args.out_dir}")

    try:
        frames = load_jsonl_frames(args.snapshots, run_id=args.run_id, strict_config=True)
    except (OSError, ValueError) as exc:
        parser.error(f"cannot load frames: {exc}")
    # The initial configuration is the configuration the first frame recorded. The command-line
    # values are assertions about it, not a substitute for it.
    initial = frames[0].state.current_config
    asserted = {"max_message_count": args.batch_size, "batch_timeout_ms": args.batch_timeout_ms,
                "preferred_max_bytes": args.preferred_max_bytes}
    mismatch = {name: (value, getattr(initial, name)) for name, value in asserted.items()
                if value is not None and value != getattr(initial, name)}
    if mismatch:
        parser.error("asserted configuration differs from the first frame's recorded configuration "
                     f"(asserted, recorded): {mismatch}")
    sizes = {int(x) for x in args.candidate_batch_sizes.split(",") if x.strip()}
    mapping = parse_map(args.hfbtp_block_map)
    policy = ApprovedMaxMessageCountSpace(frozenset(sizes), mapping, initial)
    controllers = {
        "static": StaticController(initial),
        "reactive": ReactiveController(
            initial,
            congested_config=replace(initial, max_message_count=min(sizes)),
            latency_high_ms=args.latency_high_ms, latency_low_ms=args.latency_low_ms,
            queue_high=args.queue_high, queue_low=args.queue_low, consecutive_samples=2, cooldown_samples=3,
        ),
        "predictive": PredictiveController(
            HFBTPArtifactPerformancePredictor(args.hfbtp_artifact_dir, candidate_space=policy), initial,
            arrival_forecaster=FlashbotsSeriesForecaster(args.flashbots_series, baseline=args.flashbots_baseline),
            candidate_configs=[replace(initial, max_message_count=size) for size in sorted(sizes)],
            horizon_seconds=args.horizon_seconds,
        ),
    }
    required_fields = tuple(value for value in args.required_fields.split(",") if value.strip())
    runner = IntegratedControllerRunner(
        controllers, initial, run_id=args.run_id, shadow=True,
        configuration_policy=policy, required_fields=required_fields,
        max_telemetry_age_s=args.max_telemetry_age_s,
        stop_policy=StopPolicy(max_queue_depth=args.max_queue_depth),
        invalid_frame_policy="abstain" if frame_quality is not None else "stop",
        frame_quality=frame_quality,
    )
    result = runner.run(frames, output_dir=args.out_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "mode": result["mode"],
        "strategies": result["strategies"],
        "frame_count": result["frame_count"],
        "record_count": result["record_count"],
        "identical_input_digests": result["identical_input_digests"],
        "live_submission_enabled": result["live_submission_enabled"],
        "stop_reasons": result["stop_reasons"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

