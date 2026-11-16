"""Demonstrate controllers with pinned recorded data; live actuation is unavailable."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers.offline_replay import run_offline_replay


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("fabric", "flashbots"), default="fabric")
    parser.add_argument("--start", type=int, default=600, help="Flashbots origin seconds from UTC midnight")
    parser.add_argument("--samples", type=int, default=10, help="Flashbots origins, spaced 60 seconds")
    parser.add_argument("--mode", choices=("all", "static", "reactive", "predictive"), default="all")
    parser.add_argument("--actuator", choices=("shadow", "recording"), default="shadow",
                        help="recording changes in-memory reference state only; never submits to Fabric")
    parser.add_argument("--initial-batch-size", type=int, help="Flashbots offline reference; Fabric must match its recording")
    parser.add_argument("--out-dir", type=Path, help="optional new directory; existing paths are refused")
    args = parser.parse_args(argv)
    try:
        result = run_offline_replay(ROOT, Path(__file__).with_name("offline_replay_inputs.json"),
                                    source=args.source, start=args.start, samples=args.samples,
                                    output_dir=args.out_dir, modes=("static", "reactive", "predictive") if args.mode == "all" else (args.mode,),
                                    actuator_mode=args.actuator, initial_batch_size=args.initial_batch_size)
    except (OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))
    print(json.dumps({key: result[key] for key in ("status", "mode", "live_submission_enabled", "frame_count",
                                                 "record_count", "stop_reasons", "replay", "fairness",
                                                 "deterministic_signature", "summary")}, indent=2))
    return 0 if result["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
