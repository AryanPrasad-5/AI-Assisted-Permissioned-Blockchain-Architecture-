"""Write a compact, identity-free numeric extract of each raw archive (one row per transaction).

Why: every later stage (EDA, target-series construction) needs a few numeric columns of ~2M rows.
Streaming the 180 MB of ZIPs each time is slow, and copying the archives would duplicate them. The extract
keeps the original row order, keeps every row (no filtering, no deduplication, no imputation), and drops
identity fields (hash, from, to, data_4bytes values), which no stage needs.

Units in the extract: timestamps in Unix ms (UTC); gas in gas units; prices in gwei (wei / 1e9, float64);
value in ETH (wei / 1e18, float64 - lossy above 2^53 wei, descriptive use only).

    .venv/Scripts/python.exe -B Flashbots/preprocessing/extract_transactions.py [--force]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fbcommon as fc  # noqa: E402

USECOLS = ["timestamp_ms", "to", "value", "gas", "gas_price", "gas_tip_cap", "gas_fee_cap", "data_size",
           "data_4bytes", "included_at_block_height", "included_block_timestamp_ms", "inclusion_delay_ms", "tx_type"]


def extract(archive: Path) -> dict[str, np.ndarray]:
    parts: dict[str, list[np.ndarray]] = {k: [] for k in (
        "timestamp_ms", "gas", "gas_price_gwei", "gas_tip_cap_gwei", "gas_fee_cap_gwei", "data_size", "tx_type",
        "value_eth", "to_is_null", "has_selector", "included_block_height", "included_block_timestamp_ms", "inclusion_delay_ms")}
    for chunk in fc.iter_raw_chunks(archive, usecols=USECOLS, as_text=True):
        num = {c: pd.to_numeric(chunk[c], errors="raise") for c in USECOLS if c not in ("to", "data_4bytes")}
        parts["timestamp_ms"].append(num["timestamp_ms"].to_numpy(np.int64))
        parts["gas"].append(num["gas"].to_numpy(np.int64))
        parts["gas_price_gwei"].append(num["gas_price"].to_numpy(float) / 1e9)
        parts["gas_tip_cap_gwei"].append(num["gas_tip_cap"].to_numpy(float) / 1e9)
        parts["gas_fee_cap_gwei"].append(num["gas_fee_cap"].to_numpy(float) / 1e9)
        parts["data_size"].append(num["data_size"].to_numpy(np.int64))
        parts["tx_type"].append(num["tx_type"].to_numpy(np.int8))
        parts["value_eth"].append(num["value"].to_numpy(float) / 1e18)
        parts["to_is_null"].append(chunk["to"].isna().to_numpy())
        parts["has_selector"].append(chunk["data_4bytes"].notna().to_numpy())
        parts["included_block_height"].append(num["included_at_block_height"].to_numpy(np.int64))
        parts["included_block_timestamp_ms"].append(num["included_block_timestamp_ms"].to_numpy(np.int64))
        parts["inclusion_delay_ms"].append(num["inclusion_delay_ms"].to_numpy(np.int64))
    return {k: np.concatenate(v) for k, v in parts.items()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    manifest = fc.load_manifest()
    outputs = [fc.DERIVED_DIR / f"tx_extract_{d}.npz" for d in fc.DATES]
    fc.refuse_overwrite(outputs, args.force)
    record = {"generated_utc": fc.utc_now(), "script": "Flashbots/preprocessing/extract_transactions.py", "files": []}
    for date, out in zip(fc.DATES, outputs):
        entry = manifest[date]
        sha = fc.verify_archive(entry)
        data = extract(Path(entry["source_path"]))
        rows = len(data["timestamp_ms"])
        if rows != entry["rows"]:
            raise ValueError(f"{date}: extracted {rows} rows, manifest says {entry['rows']}")
        out.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(out, **data)
        record["files"].append({"date_utc": date, "source_archive": entry["source_path"], "source_sha256": sha,
                                "rows": rows, "columns": list(data), "output": str(out.relative_to(fc.REPO)),
                                "output_sha256": fc.sha256_file(out), "output_bytes": out.stat().st_size})
        print(f"{date}: {rows} rows -> {out.name} ({out.stat().st_size / 1e6:.1f} MB)")
    record["environment"] = fc.environment()
    fc.write_json(fc.DERIVED_DIR / "extract_record.json", record)


if __name__ == "__main__":
    main()
