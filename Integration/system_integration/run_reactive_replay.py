"""Reactive-only genuine recorded replay, always shadow; no science stack needed."""

import argparse
from collections import Counter
from dataclasses import replace
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers.base import confirmed_config
from fabric_perf.controllers.reactive_pipeline import ReactiveTelemetryPipeline, replay_reactive_jsonl
from fabric_perf.controllers.strategies import ReactiveController


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=Path, help="canonical recorded ControllerState JSONL")
    parser.add_argument("--expected-sha256")
    parser.add_argument("--out", type=Path, help="optional new audit JSONL; existing files refused")
    parser.add_argument("--congested-batch-size", type=int, default=40)
    args = parser.parse_args(argv)
    try:
        if args.snapshots is None:
            entry = json.loads(Path(__file__).with_name("offline_replay_inputs.json").read_text())["files"]["snapshots"]
            path, expected = ROOT / entry["path"], entry["sha256"]
        else:
            path, expected = args.snapshots, args.expected_sha256
        with path.open(encoding="utf-8") as stream:
            first = next((json.loads(line) for line in stream if line.strip()), None)
        if not isinstance(first, dict):
            raise ValueError("first record must include a complete initial configuration")
        payload = first.get("state", first)
        initial = confirmed_config(payload["current_config"])
        target = replace(initial, max_message_count=args.congested_batch_size)
        # Existing replay profile values; no tuning or new live policy.
        controller = ReactiveController(initial, congested_config=target, latency_high_ms=1500,
                                        latency_low_ms=1200, consecutive_samples=2, cooldown_samples=3)
        pipeline = ReactiveTelemetryPipeline(controller, run_id="reactive-recorded-replay")
        records = replay_reactive_jsonl(path, pipeline, output_path=args.out, expected_sha256=expected)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))
    print(json.dumps({"mode": "shadow", "live_submission_enabled": False, "records": len(records),
                      "outcomes": dict(Counter(row["decision"]["outcome"] for row in records)),
                      "reasons": dict(Counter(row["decision"]["reason"] for row in records)),
                      "no_action_profile": initial == target,
                      "input_sha256": records[0]["audit"]["source_file_sha256"],
                      "performance_improvement_evaluated": False}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
