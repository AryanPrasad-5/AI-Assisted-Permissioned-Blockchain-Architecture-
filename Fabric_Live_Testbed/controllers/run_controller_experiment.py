"""Run one controller over a recorded, common telemetry snapshot stream.

This command is intentionally offline by default. It exercises the actual
controller decisions and records them, but does not mutate Fabric. A live
Fabric actuator can be supplied by a future experiment runner through the
library ``CallableActuator`` after the channel-update protocol is verified.

Input is JSON Lines; each line is a ControllerState-compatible mapping. The
same snapshot file should be reused for all three strategies when comparing
decision logic. The input must contain arrival history for predictive runs.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers import (  # noqa: E402
    ControllerExperimentHarness,
    ControllerState,
    ApprovedMaxMessageCountSpace,
    FabricConfig,
    FlashbotsSeriesForecaster,
    HFBTPArtifactPerformancePredictor,
    PredictiveController,
    ReactiveController,
    RecordingActuator,
    RollingMeanArrivalForecaster,
    StaticController,
)


def _config(args: argparse.Namespace) -> FabricConfig:
    return FabricConfig(
        max_message_count=args.batch_size,
        batch_timeout_ms=args.batch_timeout_ms,
        preferred_max_bytes=args.preferred_max_bytes,
    )


def _states(path: Path, initial: FabricConfig) -> list[ControllerState]:
    rows = []
    with path.open(encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                if "current_config" not in raw:
                    raw["current_config"] = initial
                rows.append(ControllerState.from_mapping(raw))
            except Exception as exc:
                raise ValueError(f"{path}:{line_number}: invalid state: {exc}") from exc
    if not rows:
        raise ValueError(f"snapshot file {path} is empty")
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy", choices=("static", "reactive", "predictive"), required=True)
    parser.add_argument("--snapshots", type=Path, required=True, help="JSONL telemetry snapshots")
    parser.add_argument("--out", type=Path, required=True, help="new JSON decision log")
    parser.add_argument("--batch-size", type=int, default=45)
    parser.add_argument("--batch-timeout-ms", type=int, default=2000)
    parser.add_argument("--preferred-max-bytes", type=int, default=10_485_760)
    parser.add_argument("--latency-high-ms", type=float, default=1500.0)
    parser.add_argument("--latency-low-ms", type=float, default=1200.0)
    parser.add_argument("--queue-high", type=float, default=None)
    parser.add_argument("--queue-low", type=float, default=None)
    parser.add_argument("--congested-batch-size", type=int, default=40)
    parser.add_argument("--congested-timeout-ms", type=int, default=None,
                        help="deprecated: live policy requires the initial timeout")
    parser.add_argument("--candidate-batch-sizes", default="40,45")
    parser.add_argument("--candidate-timeouts-ms", default=None,
                        help="deprecated: live policy requires the initial timeout")
    parser.add_argument("--hfbtp-artifact-dir", type=Path)
    parser.add_argument("--hfbtp-block-map", help="explicit Fabric B:HFBTP Block Size map, e.g. 40:40,45:45")
    parser.add_argument("--flashbots-series", type=Path,
                        help="one processed UTC daily Flashbots series for predictive shadow replay")
    parser.add_argument("--flashbots-baseline", default="ewma_alpha_0.2",
                        choices=("persistence", "rolling_mean_60s", "ewma_alpha_0.2", "ewma_alpha_0.5",
                                 "seasonal_naive_60s", "seasonal_naive_300s", "poisson_rate_300s"))
    parser.add_argument("--frequency-seconds", type=int, choices=(1, 5, 10), default=1)
    parser.add_argument("--forecast-window", type=int, default=60)
    parser.add_argument("--horizon-seconds", type=int, default=10)
    args = parser.parse_args(argv)

    initial = _config(args)
    if args.strategy == "static":
        controller = StaticController(initial)
        approved = {initial.max_message_count}
    elif args.strategy == "reactive":
        congested = FabricConfig(
            max_message_count=args.congested_batch_size,
            batch_timeout_ms=args.congested_timeout_ms or initial.batch_timeout_ms,
            preferred_max_bytes=args.preferred_max_bytes,
        )
        controller = ReactiveController(
            initial,
            congested_config=congested,
            latency_high_ms=args.latency_high_ms,
            latency_low_ms=args.latency_low_ms,
            queue_high=args.queue_high,
            queue_low=args.queue_low,
            consecutive_samples=2,
            cooldown_samples=3,
        )
        approved = {initial.max_message_count, congested.max_message_count}
    else:
        if args.hfbtp_artifact_dir is None or args.hfbtp_block_map is None or args.flashbots_series is None:
            parser.error("predictive strategy requires --hfbtp-artifact-dir, --hfbtp-block-map and --flashbots-series")
        sizes = [int(x) for x in args.candidate_batch_sizes.split(",") if x.strip()]
        timeouts = [initial.batch_timeout_ms]
        if args.candidate_timeouts_ms:
            timeouts = [int(x) for x in args.candidate_timeouts_ms.split(",") if x.strip()]
            if timeouts != [initial.batch_timeout_ms]:
                parser.error("live candidate space permits only the initial BatchTimeout")
        candidates = [
            FabricConfig(max_message_count=size, batch_timeout_ms=timeout, preferred_max_bytes=args.preferred_max_bytes)
            for size in sizes for timeout in timeouts
        ]
        mapping = {}
        for item in args.hfbtp_block_map.split(","):
            key, sep, value = item.partition(":")
            if not sep:
                parser.error("--hfbtp-block-map must contain comma-separated FabricB:HFBTPBlockSize pairs")
            mapping[int(key)] = int(value)
        approved = set(sizes)
        space = ApprovedMaxMessageCountSpace(frozenset(approved), mapping, initial)
        controller = PredictiveController(
            HFBTPArtifactPerformancePredictor(args.hfbtp_artifact_dir, candidate_space=space),
            initial,
            arrival_forecaster=FlashbotsSeriesForecaster(
                args.flashbots_series, baseline=args.flashbots_baseline,
                frequency_seconds=args.frequency_seconds,
            ),
            candidate_configs=candidates,
            horizon_seconds=args.horizon_seconds,
        )

    states = _states(args.snapshots, initial)
    policy = ApprovedMaxMessageCountSpace(
        frozenset(approved), {value: value for value in approved}, initial,
    )
    harness = ControllerExperimentHarness(controller, RecordingActuator(initial), run_id=args.strategy,
                                         configuration_policy=policy)
    harness.run(states, args.out)
    print(f"wrote {args.out} ({len(states)} decisions; no Fabric side effects)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
