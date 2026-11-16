"""Frozen EWMA → HFBTP → predictive decision replay; no live actuation."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers.predictive_pipeline import frozen_predictive_pipeline, replay_frozen_predictive


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=43200)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--initial-batch-size", type=int, choices=(10, 40), default=10,
                        help="offline reference state; not a live configuration or approval")
    parser.add_argument("--out", type=Path, help="optional new audit JSONL; parent must exist")
    args = parser.parse_args(argv)
    try:
        if args.out is not None and args.out.exists():
            raise FileExistsError(f"refusing to overwrite {args.out}")
        pipeline = frozen_predictive_pipeline(ROOT, Path(__file__).with_name("offline_replay_inputs.json"),
                                               initial_batch_size=args.initial_batch_size)
        result = replay_frozen_predictive(pipeline, start=args.start, samples=args.samples, output_path=args.out)
    except (OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))
    print(json.dumps({key: value for key, value in result.items() if key != "records"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
