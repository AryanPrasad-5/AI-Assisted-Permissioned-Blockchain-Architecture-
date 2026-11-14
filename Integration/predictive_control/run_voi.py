"""P3: law-implied value of information for the {B=10, B=40} menu, from the frozen forecasts.

Commands (offline arithmetic; writes experiments/results/flashbots_horizon_benchmark_v2/voi/, never overwrites):
    .venv/Scripts/python.exe -I -B scripts/replay/run_voi.py --pool V
    .venv/Scripts/python.exe -I -B scripts/replay/run_voi.py --pool F      # refused until the F pass (final/) exists

Every quantity is a law-implied interval p95 in seconds, volume-weighted. Rates outside 10-40 tx/s are extrapolation.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.forecasting import horizon_benchmark as hb  # noqa: E402
from fabric_perf.forecasting import law_voi  # noqa: E402
from fabric_perf.replay import trace  # noqa: E402

DAY = {"V": "2026-09-21", "F": "2026-10-12"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pool", choices=sorted(DAY), required=True)
    parser.add_argument("--results-dir", type=Path, default=REPO / "experiments/results/flashbots_horizon_benchmark_v2")
    parser.add_argument("--series-dir", type=Path, default=REPO / "data/processed/flashbots")
    args = parser.parse_args()
    source = "select" if args.pool == "V" else "final"
    if args.pool == "F" and not (args.results_dir / "final" / "final_report.json").exists():
        print("REFUSED: the F pass has not been run, so F forecasts do not exist yet.")
        return 2
    out = args.results_dir / "voi"
    tiered_path, summary_path = out / f"voi_{args.pool}_tiered.csv", out / f"voi_{args.pool}_summary.json"
    if tiered_path.exists() or summary_path.exists():
        print(f"REFUSED: {tiered_path.name} already exists (outputs are never overwritten).")
        return 2
    out.mkdir(exist_ok=True)
    frozen = json.loads((args.results_dir / "select" / "selection_frozen.json").read_text(encoding="utf-8"))

    rows, summary = [], {"pool": args.pool, "law": "p95 = W + c; W = min((B-1)/rate, 2 s); c = +25 ms count-cut, -60 ms timer-cut, -24 ms boundary",
                         "validated_rate_tx_per_s": list(law_voi.VALIDATED_RATE), "unit": "seconds, volume-weighted law-implied interval p95",
                         "forecast_source": f"{source}/forecasts_{args.pool}_H<H>.csv", "horizons": {}}
    for horizon in hb.HORIZONS:
        frame = pd.read_csv(args.results_dir / source / f"forecasts_{args.pool}_H{horizon}.csv")
        y = frame["y_observed"].to_numpy()
        selected = frozen["horizons"][str(horizon)]["selected_model"]
        for model in hb.MODELS:
            table = law_voi.tiered(y, horizon, frame[f"yhat_{model}"].to_numpy())
            table.insert(0, "model", model)
            table.insert(0, "horizon_s", horizon)
            table.insert(0, "resolution", "interval_mean")
            table["selected"] = model == selected
            rows.append(table)
        all_row = law_voi.summarize(y, horizon, frame[f"yhat_{selected}"].to_numpy())
        summary["horizons"][str(horizon)] = {"selected_model": selected, "all_intervals": all_row,
                                             "tier_shares": law_voi.tiered(y, horizon)[["tier", "share_intervals", "share_volume"]].to_dict("records")}
    counts = trace.read_series(args.series_dir, DAY[args.pool], 1).astype(float)
    counts = counts[hb.POOL_START["V"]:] if args.pool == "V" else counts
    sensitivity = law_voi.tiered(counts, 1.0)  # oracle only: each second's count taken as a constant rate for one second
    sensitivity.insert(0, "model", "oracle_only")
    sensitivity.insert(0, "horizon_s", 1)
    sensitivity.insert(0, "resolution", "one_second_counts_sensitivity")
    sensitivity["selected"] = False
    pd.concat(rows + [sensitivity], ignore_index=True).to_csv(tiered_path, index=False)
    summary["one_second_sensitivity_all"] = law_voi.summarize(counts, 1.0)
    summary["code_sha256"] = {Path(p).name: trace.sha256_file(p) for p in (law_voi.__file__, __file__)}
    summary_path.write_text(json.dumps(summary, indent=2, default=float), encoding="utf-8")

    for horizon in hb.HORIZONS:
        info = summary["horizons"][str(horizon)]
        a = info["all_intervals"]
        print(f"[{args.pool}] H={horizon}s selected={info['selected_model']}: static B10={a['static_B10']:.3f}s B40={a['static_B40']:.3f}s "
              f"oracle={a['oracle_by_interval']:.3f}s headroom_over_best_static={a['oracle_headroom_over_best_static_s']:.4f}s "
              f"over_B40={a['oracle_headroom_over_B40_s']:.3f}s forecast_regret={a['forecast_regret_vs_oracle_s']:.4f}s "
              f"selection_errors={a['selection_errors']}/{a['intervals_with_traffic']}")
        print("   exposure (share of volume): " + ", ".join(f"{t['tier']}={t['share_volume']:.3f}" for t in info["tier_shares"] if t["tier"] != "all"))
    s1 = summary["one_second_sensitivity_all"]
    print(f"[{args.pool}] 1-s sensitivity (law not validated at 1 s): B10={s1['static_B10']:.3f}s B40={s1['static_B40']:.3f}s "
          f"oracle={s1['oracle_by_interval']:.3f}s headroom_over_best_static={s1['oracle_headroom_over_best_static_s']:.4f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
