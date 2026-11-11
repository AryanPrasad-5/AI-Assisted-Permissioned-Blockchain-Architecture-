"""Run all three controllers over identical telemetry snapshots without Fabric side effects."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers import (  # noqa: E402
    ApprovedMaxMessageCountSpace,
    ControllerExperimentHarness,
    ControllerState,
    FabricConfig,
    FlashbotsSeriesForecaster,
    HFBTPArtifactPerformancePredictor,
    PredictiveController,
    ReactiveController,
    ShadowActuator,
    StaticController,
)


def read_states(path: Path, initial: FabricConfig) -> list[ControllerState]:
    states = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                row.setdefault("current_config", initial.as_dict())
                states.append(ControllerState.from_mapping(row))
            except Exception as exc:
                raise ValueError(f"{path}:{line_no}: invalid telemetry snapshot: {exc}") from exc
    if not states:
        raise ValueError("telemetry snapshot file is empty")
    return states


def parse_map(text: str) -> dict[int, int]:
    mapping = {}
    for item in text.split(","):
        left, sep, right = item.partition(":")
        if not sep:
            raise ValueError("mapping must contain comma-separated FabricB:HFBTPBlockSize pairs")
        mapping[int(left)] = int(right)
    return mapping


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--hfbtp-artifact-dir", type=Path, required=True)
    parser.add_argument("--flashbots-series", type=Path, required=True)
    parser.add_argument("--flashbots-baseline", default="ewma_alpha_0.2")
    parser.add_argument("--horizon-seconds", type=int, default=10)
    parser.add_argument("--candidate-batch-sizes", default="40,45")
    parser.add_argument("--hfbtp-block-map", required=True)
    parser.add_argument("--batch-size", type=int, default=45)
    parser.add_argument("--batch-timeout-ms", type=int, default=2000)
    parser.add_argument("--preferred-max-bytes", type=int, default=10_485_760)
    parser.add_argument("--latency-high-ms", type=float, default=1500.0)
    parser.add_argument("--latency-low-ms", type=float, default=1200.0)
    parser.add_argument("--queue-high", type=float)
    parser.add_argument("--queue-low", type=float)
    args = parser.parse_args(argv)
    if args.out_dir.exists():
        parser.error(f"refusing to overwrite {args.out_dir}")

    initial = FabricConfig(max_message_count=args.batch_size, batch_timeout_ms=args.batch_timeout_ms,
                           preferred_max_bytes=args.preferred_max_bytes)
    sizes = {int(x) for x in args.candidate_batch_sizes.split(",") if x.strip()}
    mapping = parse_map(args.hfbtp_block_map)
    policy = ApprovedMaxMessageCountSpace(frozenset(sizes), mapping, initial)
    states = read_states(args.snapshots, initial)
    args.out_dir.mkdir(parents=True)

    controllers = {
        "static": StaticController(initial),
        "reactive": ReactiveController(
            initial, congested_config=FabricConfig(max_message_count=min(sizes), batch_timeout_ms=initial.batch_timeout_ms,
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
    for name, controller in controllers.items():
        harness = ControllerExperimentHarness(controller, ShadowActuator(initial), run_id=f"shadow-{name}",
                                             configuration_policy=policy)
        records = harness.run(states)
        (args.out_dir / f"{name}.json").write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")
    manifest = {
        "mode": "shadow",
        "live_submission": False,
        "shared_snapshot_path": str(args.snapshots),
        "strategies": sorted(controllers),
        "approved_max_message_counts": sorted(sizes),
        "hfbtp_block_size_mapping": mapping,
        "forecast_baseline": args.flashbots_baseline,
        "horizon_seconds": args.horizon_seconds,
    }
    (args.out_dir / "shadow_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"out_dir": str(args.out_dir), "strategies": sorted(controllers), "live_submission": False}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
