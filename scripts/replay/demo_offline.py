"""P5: offline end-to-end demonstration of Flashbots historical replay + forecast-driven workload.

Command (read-only except for a temporary directory; nothing is written to the repo):
    .venv/Scripts/python.exe -I -B scripts/replay/demo_offline.py --window-dir data/processed/flashbots_ms/windows/F_82800

It (1) re-verifies the derived ms trace against its recorded SHA-256, (2) regenerates the A, BO and BF schedules from the trace
and the frozen forecasts and checks them byte-for-byte against the recorded schedule hashes, (3) re-runs the integrity checks,
(4) prints observed / forecast / offered / delivered rates side by side (delivered is NOT MEASURED offline), and (5) prints the
law-implied predictions per arm and configuration with their exposure outside the validated range.
No Docker, WSL, Fabric or Caliper is started; the law-implied numbers are predictions to falsify, not measurements.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.forecasting import law_voi  # noqa: E402
from fabric_perf.replay import fidelity, schedules, trace, windows  # noqa: E402


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    ok = np.isfinite(values) & (weights > 0)
    order = np.argsort(values[ok])
    v, w = values[ok][order], weights[ok][order]
    return float(v[np.searchsorted(np.cumsum(w), q * w.sum())])


def law_row(arm: str, rates: np.ndarray, interval_s: float, resolution: str) -> list[dict]:
    rows = []
    weights = rates * interval_s
    for batch in law_voi.MENU:
        p = law_voi.law_p95_s(rates, batch)
        rows.append({"arm": arm, "B": batch, "resolution": resolution,
                     "law_mean_p95_ms": 1000 * float(np.nansum(p * weights) / weights[np.isfinite(p)].sum()),
                     "law_p95_of_intervals_ms": 1000 * weighted_quantile(p, weights, 0.95),
                     "share_volume_below_10": float(weights[rates < 10].sum() / weights.sum()),
                     "share_volume_above_40": float(weights[rates > 40].sum() / weights.sum())})
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--window-dir", type=Path, required=True)
    parser.add_argument("--ms-dir", type=Path, default=REPO / "data/processed/flashbots_ms")
    args = parser.parse_args()
    record = json.loads((args.window_dir / "window_record.json").read_text(encoding="utf-8"))
    day, start_s, length_s, interval_s = record["day"], record["start_s"], record["length_s"], record["bo_intensity"]["interval_s"]

    offsets = trace.load_day_offsets(args.ms_dir, day)  # raises if the derived trace differs from its recorded SHA-256
    print(f"[1] derived ms trace {day} verified against its recorded SHA-256 ({len(offsets)} arrivals)")
    window = windows.extract_window(offsets, day, start_s, length_s)
    arms = {"A": schedules.observed_schedule(window)}
    rates_bo = schedules.interval_rates_from_counts(window.offsets_ms, length_s, interval_s)
    arms["BO"] = schedules.poisson_schedule(rates_bo, interval_s, seed=record["selection_seed"])
    rates_bf = np.array(record["bf_intensity"]["rates_tx_per_s"]) if record.get("bf_intensity") else None
    if rates_bf is not None:
        arms["BF"] = schedules.poisson_schedule(rates_bf, interval_s, seed=record["selection_seed"])

    print(f"[2] regenerated schedules vs recorded hashes (window {day} start={start_s}s length={length_s}s)")
    with tempfile.TemporaryDirectory() as scratch:
        for arm, schedule in arms.items():
            path = Path(scratch) / f"{arm}.txt"
            schedules.write_replay_rate(schedule, path)
            same = trace.sha256_file(path) == record["schedule_sha256"][arm]
            print(f"    {arm}: {len(schedule)} arrivals, byte-identical to recorded schedule: {'YES' if same else 'NO  <-- FAIL'}")
            if not same:
                return 3

    print("[3] integrity checks")
    reports = {"A": fidelity.observed_schedule_integrity(window, arms["A"]),
               "BO": fidelity.generated_schedule_integrity(arms["BO"], rates_bo, interval_s)}
    if rates_bf is not None:
        reports["BF"] = fidelity.generated_schedule_integrity(arms["BF"], rates_bf, interval_s)
    for arm, report in reports.items():
        print(f"    {arm}: {'PASS' if report.ok else 'FAIL'} ({len(report.checks)} checks)")
    if not all(r.ok for r in reports.values()):
        return 3

    print("[4] four rates, kept separate (tx/s). Delivered is NOT MEASURED offline.")
    tables = {arm: fidelity.rate_table(window, s, rates_bf if arm == "BF" else None, interval_s if arm == "BF" else None) for arm, s in arms.items()}
    interval = pd.DataFrame({"interval_start_s": np.arange(0, length_s, interval_s),
                             "observed_y": tables["A"]["observed_y"].to_numpy().reshape(-1, interval_s).mean(axis=1)})
    if rates_bf is not None:
        interval["forecast_yhat"] = rates_bf
    for arm, t in tables.items():
        interval[f"offered_s_{arm}"] = t["offered_s"].to_numpy().reshape(-1, interval_s).mean(axis=1)
    interval["delivered_d"] = np.nan
    print(interval.round(2).to_string(index=False))
    summaries = {arm: fidelity.rate_summary(t, interval_s if arm == "BF" else None) for arm, t in tables.items()}
    for arm, s in summaries.items():
        print(f"    {arm}: offered/observed volume={s['offered_over_observed_volume']:.3f} per-second MAE offered vs observed={s['per_second_mae_offered_vs_observed']:.2f}"
              + (f" forecast MAE vs interval mean={s['forecast_mae_vs_interval_mean']:.2f} bias={s['forecast_bias_vs_interval_mean']:+.2f}" if "forecast_mae_vs_interval_mean" in s else ""))

    print("[5] burst structure of what each arm would offer")
    descriptors = {arm: fidelity.schedule_descriptors(s, length_s) for arm, s in arms.items()}
    for arm, d in descriptors.items():
        print(f"    {arm}: n={d['n_tx']} mean={d['mean_rate']:.2f}/s max_1s={d['max_1s']} gt40_tx_share={d['tx_share_in_seconds_gt40']:.4f} "
              f"gap<=5ms={d['gap_le5ms_share']:.3f} zero_gap={d['zero_gap_share']:.3f} dispersion={d['dispersion_index']:.2f}")

    print("[6] LAW-IMPLIED predictions (PROPOSED, to be falsified by a live run; law validated only at constant 10-40 tx/s)")
    rows = []
    for arm, schedule in arms.items():
        rows += law_row(arm, windows.second_counts(schedule, length_s).astype(float), 1.0, "1s_counts_of_the_schedule")
        rows += law_row(arm, windows.second_counts(schedule, length_s).reshape(-1, interval_s).sum(axis=1) / interval_s, interval_s, f"{interval_s}s_means_of_the_schedule")
    law = pd.DataFrame(rows)
    print(law.round(3).to_string(index=False))
    report = {"window": {k: record[k] for k in ("pool", "day", "start_s", "length_s")}, "schedule_sha256": record["schedule_sha256"],
              "integrity_pass": {a: r.ok for a, r in reports.items()}, "descriptors": descriptors,
              "rate_summaries": summaries, "law_implied": law.to_dict("records"),
              "delivered_rate": "NOT_MEASURED_OFFLINE"}
    print("\nDEMO COMPLETE: schedules reproduce byte-for-byte, integrity PASS, delivered rate NOT MEASURED (needs an authorized live run).")
    (Path(tempfile.gettempdir()) / "demo_offline_report.json").write_text(json.dumps(report, indent=2, default=float), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
