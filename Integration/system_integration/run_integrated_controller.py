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
    parser.add_argument("--candidate-batch-sizes", default="40,45")
    parser.add_argument("--hfbtp-block-map", required=True)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--batch-timeout-ms", type=int, default=2000)
    parser.add_argument("--preferred-max-bytes", type=int, default=10_485_760)
    parser.add_argument("--latency-high-ms", type=float, default=1500.0)
    parser.add_argument("--latency-low-ms", type=float, default=1200.0)
    parser.add_argument("--queue-high", type=float)
    parser.add_argument("--queue-low", type=float)
    parser.add_argument("--required-fields", default="",
                        help="comma-separated critical ControllerState fields; empty for replay compatibility")
    parser.add_argument("--max-telemetry-age-s", type=float, default=5.0)
    parser.add_argument("--max-queue-depth", type=float)
    args = parser.parse_args(argv)
    if args.out_dir.exists():
        parser.error(f"refusing to overwrite {args.out_dir}")

    initial = FabricConfig(max_message_count=args.batch_size, batch_timeout_ms=args.batch_timeout_ms,
                           preferred_max_bytes=args.preferred_max_bytes)
    sizes = {int(x) for x in args.candidate_batch_sizes.split(",") if x.strip()}
    mapping = parse_map(args.hfbtp_block_map)
    policy = ApprovedMaxMessageCountSpace(frozenset(sizes), mapping, initial)
    frames = load_jsonl_frames(args.snapshots, run_id=args.run_id)
    controllers = {
        "static": StaticController(initial),
        "reactive": ReactiveController(
            initial,
            congested_config=FabricConfig(max_message_count=min(sizes), batch_timeout_ms=initial.batch_timeout_ms,
                                          preferred_max_bytes=initial.preferred_max_bytes),
            latency_high_ms=args.latency_high_ms, latency_low_ms=args.latency_low_ms,
            queue_high=args.queue_high, queue_low=args.queue_low, consecutive_samples=2, cooldown_samples=3,
        ),
        "predictive": PredictiveController(
            HFBTPArtifactPerformancePredictor(args.hfbtp_artifact_dir, candidate_space=policy), initial,
            arrival_forecaster=FlashbotsSeriesForecaster(args.flashbots_series, baseline=args.flashbots_baseline),
            candidate_configs=[FabricConfig(max_message_count=size, batch_timeout_ms=initial.batch_timeout_ms,
                                            preferred_max_bytes=initial.preferred_max_bytes) for size in sorted(sizes)],
            horizon_seconds=args.horizon_seconds,
        ),
    }
    required_fields = tuple(value for value in args.required_fields.split(",") if value.strip())
    runner = IntegratedControllerRunner(
        controllers, initial, run_id=args.run_id, shadow=True,
        configuration_policy=policy, required_fields=required_fields,
        max_telemetry_age_s=args.max_telemetry_age_s,
        stop_policy=StopPolicy(max_queue_depth=args.max_queue_depth),
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

