"""Build the 1-second multi-target UTC series and verify the six stored arrival-count series.

Two jobs, both from Flashbots/data/derived/tx_extract_<date>.npz (itself a row-complete copy of the raw archive):

1. Write Flashbots/time_series/target_series_<date>_1s.csv.gz with one row per UTC second (86,400 rows), bin
   [t, t+1 s) labelled by its left edge. Columns:
     arrival_count        transactions first seen in the bin (PRIMARY target, tx/s)
     gas_limit_sum        sum of declared gas limits of those transactions (gas/s; a demand proxy, not gas used)
     calldata_bytes_sum   sum of calldata sizes (bytes/s)
     median_fee_cap_gwei  median max-fee-per-gas of the bin's transactions (NaN when the bin is empty)
     median_tip_cap_gwei  median priority-fee cap (NaN when the bin is empty)
   Empty seconds are real zeros for the sums/counts; nothing is interpolated or resampled.
2. Independently re-aggregate arrival counts to 1/5/10 s and compare them value by value with the six stored
   series in data/processed/flashbots/ (built earlier by build_flashbots_series.py). Also check the stored files'
   SHA-256 against the hashes recorded by the frozen P2 benchmark. Result: Flashbots/time_series/series_verification.json.

    .venv/Scripts/python.exe -B Flashbots/time_series/build_target_series.py [--force]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fbcommon as fc  # noqa: E402

FREQUENCIES = (1, 5, 10)


def second_index(timestamp_ms: np.ndarray, date: str) -> np.ndarray:
    """UTC second-of-day for each transaction; raises if any timestamp falls outside the file's UTC date."""
    offsets = np.asarray(timestamp_ms, dtype=np.int64) - fc.day_start_ms(date)
    if (offsets < 0).any() or (offsets >= fc.DAY_MS).any():
        raise ValueError(f"{date}: timestamps outside the UTC day")
    return offsets // 1000


def aggregate(tx: dict[str, np.ndarray], date: str) -> pd.DataFrame:
    sec = second_index(tx["timestamp_ms"], date)
    frame = pd.DataFrame({
        "timestamp_utc": pd.date_range(pd.Timestamp(date, tz="UTC"), periods=fc.DAY_S, freq="1s"),
        "second_of_day": np.arange(fc.DAY_S),
        "arrival_count": np.bincount(sec, minlength=fc.DAY_S),
        "gas_limit_sum": np.bincount(sec, weights=tx["gas"].astype(float), minlength=fc.DAY_S).astype(np.int64),
        "calldata_bytes_sum": np.bincount(sec, weights=tx["data_size"].astype(float), minlength=fc.DAY_S).astype(np.int64),
    })
    per_tx = pd.DataFrame({"sec": sec, "fee": tx["gas_fee_cap_gwei"], "tip": tx["gas_tip_cap_gwei"]})
    medians = per_tx.groupby("sec")[["fee", "tip"]].median()
    frame["median_fee_cap_gwei"] = medians["fee"].reindex(frame["second_of_day"]).to_numpy()
    frame["median_tip_cap_gwei"] = medians["tip"].reindex(frame["second_of_day"]).to_numpy()
    frame.insert(1, "date_utc", date)
    return frame


def verify_against_stored(counts_1s: np.ndarray, date: str) -> list[dict]:
    rows = []
    for frequency in FREQUENCIES:
        path = fc.STORED_SERIES_DIR / f"series_{date}_{frequency}s.csv.gz"
        stored = pd.read_csv(path)
        mine = counts_1s.reshape(-1, frequency).sum(axis=1)
        expected_start = pd.date_range(pd.Timestamp(date, tz="UTC"), periods=len(mine), freq=f"{frequency}s")
        stamps_ok = len(stored) == len(mine) and (pd.to_datetime(stored["timestamp_utc"], utc=True).to_numpy() == expected_start.to_numpy()).all()
        diff = np.abs(stored["arrival_count"].to_numpy() - mine) if len(stored) == len(mine) else None
        rows.append({
            "date_utc": date, "frequency_s": frequency, "stored_file": str(path.relative_to(fc.REPO)),
            "stored_sha256": fc.sha256_file(path), "bins_stored": len(stored), "bins_expected": len(mine),
            "timestamps_regular_utc_grid": bool(stamps_ok), "bins_differing": None if diff is None else int((diff > 0).sum()),
            "total_arrivals_stored": int(stored["arrival_count"].sum()), "total_arrivals_rebuilt": int(mine.sum()),
            "zero_bins": int((stored["arrival_count"] == 0).sum()),
            "identical": bool(diff is not None and (diff == 0).all() and stamps_ok),
        })
    return rows


def p2_recorded_hashes() -> dict[str, str]:
    """Series hashes recorded by the frozen P2 selection (21 Sep) and final report (12 Oct)."""
    out = {}
    for rel in ("select/selection_frozen.json", "final/final_report.json"):
        path = fc.P2_DIR / rel
        if path.exists():
            out.update(json.loads(path.read_text(encoding="utf-8")).get("series_sha256", {}))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    outputs = [fc.TS_DIR / f"target_series_{d}_1s.csv.gz" for d in fc.DATES] + [fc.TS_DIR / "series_verification.json"]
    fc.refuse_overwrite(outputs, args.force)
    recorded = p2_recorded_hashes()
    checks, files = [], []
    for date in fc.DATES:
        tx = fc.load_extract(date)
        frame = aggregate(tx, date)
        assert frame["arrival_count"].sum() == len(tx["timestamp_ms"]), "every transaction lands in exactly one bin"
        out = fc.TS_DIR / f"target_series_{date}_1s.csv.gz"
        frame.to_csv(out, index=False, compression="gzip")
        files.append({"date_utc": date, "file": str(out.relative_to(fc.REPO)), "rows": len(frame), "sha256": fc.sha256_file(out)})
        day_checks = verify_against_stored(frame["arrival_count"].to_numpy(), date)
        for row in day_checks:
            if row["frequency_s"] == 1 and date in recorded:
                row["sha256_recorded_by_P2"] = recorded[date]
                row["sha256_matches_P2_record"] = row["stored_sha256"] == recorded[date]
        checks.extend(day_checks)
        print(f"{date}: " + ", ".join(f"{r['frequency_s']}s identical={r['identical']}" for r in day_checks))
    fc.write_json(fc.TS_DIR / "series_verification.json", {
        "generated_utc": fc.utc_now(), "script": "Flashbots/time_series/build_target_series.py",
        "method": "Arrival counts re-aggregated from the row-complete extract (independent of fabric_perf.forecasting.flashbots) "
                  "and compared bin by bin with the stored series; stored-file SHA-256 compared with the P2 records.",
        "all_six_stored_series_identical": all(c["identical"] for c in checks),
        "stored_series_checks": checks, "target_series_files": files, "environment": fc.environment(),
    })


if __name__ == "__main__":
    main()
