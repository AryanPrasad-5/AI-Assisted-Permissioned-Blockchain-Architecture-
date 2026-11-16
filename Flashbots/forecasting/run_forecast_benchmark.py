"""P2: leakage-safe horizon-mean forecast benchmark.

Commands (offline; read the stored 1-s count series, write under experiments/results/flashbots_horizon_benchmark_v2/):
    .venv/Scripts/python.exe -I -B scripts/replay/run_forecast_benchmark.py select
    .venv/Scripts/python.exe -I -B scripts/replay/run_forecast_benchmark.py final --frozen-sha256 <SHA-256 of selection_frozen.json>

`select` touches only the 21 Sep series. `final` touches only the 12 Oct series and runs once.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.forecasting import horizon_runner  # noqa: E402
from fabric_perf.replay.common import IntegrityError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["select", "final"])
    parser.add_argument("--series-dir", type=Path, default=REPO / "data/processed/flashbots")
    parser.add_argument("--out-dir", type=Path, default=REPO / "experiments/results/flashbots_horizon_benchmark_v2")
    parser.add_argument("--frozen-sha256", help="pre-registered SHA-256 of selection_frozen.json (final only)")
    args = parser.parse_args()
    try:
        if args.mode == "select":
            result = horizon_runner.select_run(args.series_dir, args.out_dir)
            print(f"FROZEN selection_frozen.json sha256={result['frozen_file_sha256']}")
            for horizon, info in result["horizons"].items():
                print(f"  H={horizon}s alpha={info['alpha']} selected={info['selected_model']} "
                      f"V-MAE={info['validation_mae'][info['selected_model']]:.4f}")
        else:
            if not args.frozen_sha256:
                print("REFUSED: --frozen-sha256 is required")
                return 2
            result = horizon_runner.final_run(args.series_dir, args.out_dir / "select" / "selection_frozen.json",
                                              args.frozen_sha256, args.out_dir)
            print(f"F PASS COMPLETE final_report sha256={result['final_report_sha256']}")
            for horizon, info in result["horizons"].items():
                print(f"  H={horizon}s selected={info['selected_model']} F-MAE={info['mae_F_selected']:.4f} "
                      f"(V-MAE {info['mae_V_selected']:.4f}) skill vs persistence_interval={info['skill_vs_persistence_interval_F']:+.3f}")
    except (IntegrityError, FileExistsError) as exc:
        print(f"REFUSED/FAILED: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
