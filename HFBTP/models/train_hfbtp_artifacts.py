"""Train reproducible full-data HFBTP inference artifacts."""

from pathlib import Path
import argparse
import json

from fabric_perf.models.artifacts import train_and_save_hfbtp


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw/hfbtp"))
    parser.add_argument("--output-dir", type=Path, default=Path("experiments/artifacts/hfbtp"))
    parser.add_argument("--model", default="random_forest", choices=["dummy_median", "linear", "random_forest"])
    args = parser.parse_args()
    print(json.dumps(train_and_save_hfbtp(args.data_dir, args.output_dir, args.model), indent=2))


if __name__ == "__main__":
    main()
