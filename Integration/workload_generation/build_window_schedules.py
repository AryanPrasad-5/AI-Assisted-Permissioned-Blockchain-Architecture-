"""P1: tile a pool, pick one window by the registered seeded rule, and build + check its Approach A and BO schedules.

Command:
    .venv/Scripts/python.exe -I scripts/replay/build_window_schedules.py --pool D

Pool F (12 Oct) is refused unless --allow-f is passed, which is done only after the forecast methodology and
model selection are frozen (P2). Output: data/processed/flashbots_ms/windows/<pool>_<start_s>/ (never overwritten).
Offline artifacts only; nothing here starts Docker, WSL, Fabric or Caliper.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.replay import fidelity, schedules, trace, windows  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pool", choices=sorted(windows.POOLS), required=True)
    parser.add_argument("--length-s", type=int, default=windows.DEFAULT_LENGTH_S)
    parser.add_argument("--interval-s", type=int, default=60, help="intensity interval for the BO arm")
    parser.add_argument("--ms-dir", type=Path, default=REPO / "data/processed/flashbots_ms")
    parser.add_argument("--allow-f", action="store_true", help="only after the forecast benchmark is frozen")
    parser.add_argument("--forecast-csv", type=Path, help="forecasts_<pool>_H60.csv: also build the forecast-driven BF arm")
    parser.add_argument("--model", help="forecast column to use for BF (the frozen selected model)")
    args = parser.parse_args()
    if bool(args.forecast_csv) != bool(args.model):
        print("REFUSED: --forecast-csv and --model go together.")
        return 2
    if args.pool == "F" and not args.allow_f:
        print("REFUSED: pool F is opened only after the forecast methodology and model selection are frozen (P2).")
        return 2

    day = windows.POOLS[args.pool][0]
    offsets = trace.load_day_offsets(args.ms_dir, day)  # re-checks the recorded SHA-256
    table = windows.tile_table(offsets, args.pool, args.length_s)
    row, eligible = windows.select_window(table)
    start_s = int(row["start_s"])
    out = args.ms_dir / "windows" / f"{args.pool}_{start_s}"
    if out.exists():
        print(f"REFUSED: {out} already exists (outputs are never overwritten).")
        return 2
    out.mkdir(parents=True)

    window = windows.extract_window(offsets, day, start_s, args.length_s)
    arm_a = schedules.observed_schedule(window)
    rates_bo = schedules.interval_rates_from_counts(window.offsets_ms, args.length_s, args.interval_s)
    arm_bo = schedules.poisson_schedule(rates_bo, args.interval_s, seed=windows.WINDOW_SEED)

    report_a = fidelity.observed_schedule_integrity(window, arm_a)
    report_bo = fidelity.generated_schedule_integrity(arm_bo, rates_bo, args.interval_s)
    print(report_a.render())
    print(report_bo.render())
    arm_bf = rates_bf = report_bf = None
    if args.forecast_csv:
        # Forecast-driven arm: the frozen model's forecast at each interval origin becomes the submission intensity.
        # Each forecast was made from counts before its own origin (leakage tests + lookback table), never from the window itself.
        import pandas as pd

        frame = pd.read_csv(args.forecast_csv)
        origins = list(range(start_s, start_s + args.length_s, args.interval_s))
        picked = frame.set_index("origin_s").loc[origins]
        if len(picked) != args.length_s // args.interval_s or (picked["target_end_s"].to_numpy() - picked.index.to_numpy() != args.interval_s).any():
            raise SystemExit("forecast table does not tile the window")
        rates_bf = picked[f"yhat_{args.model}"].to_numpy(dtype=float)
        arm_bf = schedules.poisson_schedule(rates_bf, args.interval_s, seed=windows.WINDOW_SEED)  # same seed: common random numbers with BO
        report_bf = fidelity.generated_schedule_integrity(arm_bf, rates_bf, args.interval_s)
        print(report_bf.render())

    table.to_csv(out / "tiles.csv", index=False)
    schedules.write_replay_rate(arm_a, out / "schedule_A_replay_rate.txt")
    schedules.write_replay_rate(arm_bo, out / "schedule_BO_replay_rate.txt")
    fidelity.rate_table(window, arm_a).to_csv(out / "rate_table_A.csv", index=False)
    fidelity.rate_table(window, arm_bo).to_csv(out / "rate_table_BO.csv", index=False)  # BO intensity is an ex-post oracle, not a forecast
    descriptors = {
        "A": fidelity.schedule_descriptors(arm_a, args.length_s),
        "BO": fidelity.schedule_descriptors(arm_bo, args.length_s),
        "BO_rate_summary": fidelity.rate_summary(fidelity.rate_table(window, arm_bo)),
    }
    if arm_bf is not None:
        schedules.write_replay_rate(arm_bf, out / "schedule_BF_replay_rate.txt")
        table_bf = fidelity.rate_table(window, arm_bf, rates_bf, args.interval_s)
        table_bf.to_csv(out / "rate_table_BF.csv", index=False)  # observed y, forecast yhat, offered s, delivered d (NaN)
        descriptors["BF"] = fidelity.schedule_descriptors(arm_bf, args.length_s)
        descriptors["BF_rate_summary"] = fidelity.rate_summary(table_bf, args.interval_s)
    record = {
        "pool": args.pool, "day": day, "start_s": start_s, "length_s": args.length_s, "pre_roll_s": window.pre_roll_s,
        "selection_seed": windows.WINDOW_SEED, "tiles": len(table), "eligible_tiles": eligible,
        "selected_tile": {k: (v.item() if hasattr(v, "item") else v) for k, v in row.items()},
        "bo_intensity": {"interval_s": args.interval_s, "source": "observed ex-post interval mean (oracle, not a forecast)",
                         "rates_tx_per_s": rates_bo.tolist()},
        "descriptors": descriptors,
        "integrity": {"A": report_a.as_dicts(), "BO": report_bo.as_dicts(),
                      **({"BF": report_bf.as_dicts()} if report_bf else {})},
        "bf_intensity": None if rates_bf is None else {"interval_s": args.interval_s, "model": args.model, "forecast_file": str(args.forecast_csv),
                                                       "rates_tx_per_s": rates_bf.tolist()},
        "schedule_sha256": {"A": trace.sha256_file(out / "schedule_A_replay_rate.txt"),
                            "BO": trace.sha256_file(out / "schedule_BO_replay_rate.txt"),
                            **({"BF": trace.sha256_file(out / "schedule_BF_replay_rate.txt")} if arm_bf is not None else {})},
        "code_sha256": {Path(p).name: trace.sha256_file(p) for p in
                        (windows.__file__, schedules.__file__, fidelity.__file__, __file__)},
    }
    (out / "window_record.json").write_text(json.dumps(record, indent=2, default=float), encoding="utf-8")
    print(f"pool {args.pool}: {len(table)} tiles, {eligible} eligible, selected start_s={start_s} -> {out}")
    for arm in ("A", "BO", "BF"):
        if arm not in descriptors:
            continue
        d = descriptors[arm]
        print(f"  {arm}: n={d['n_tx']} mean={d['mean_rate']:.2f}/s max_1s={d['max_1s']} p95_1s={d['p95_1s']:.0f} "
              f"gt40_tx_share={d['tx_share_in_seconds_gt40']:.4f} gap<=5ms={d['gap_le5ms_share']:.3f} "
              f"zero_gap={d['zero_gap_share']:.3f} dispersion={d['dispersion_index']:.2f}")
    return 0 if report_a.ok and report_bo.ok and (report_bf is None or report_bf.ok) else 3


if __name__ == "__main__":
    raise SystemExit(main())
