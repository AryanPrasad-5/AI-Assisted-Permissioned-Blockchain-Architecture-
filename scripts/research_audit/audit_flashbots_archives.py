"""Audit Flashbots Mempool Dumpster ZIP archives without extracting them."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


def audit_archive(path: Path, seen_db: sqlite3.Connection, chunk_rows: int = 100_000) -> dict:
    digest = hashlib.sha256(path.read_bytes()).hexdigest().upper()
    with zipfile.ZipFile(path) as archive:
        corrupt_entry = archive.testzip()
        if corrupt_entry is not None:
            raise ValueError(f"CRC failure in {path}: {corrupt_entry}")
        entries = [info for info in archive.infolist() if not info.is_dir()]
        csv_entries = [info for info in entries if info.filename.lower().endswith(".csv")]
        if len(csv_entries) != 1:
            raise ValueError(f"Expected exactly one CSV entry in {path}, found {len(csv_entries)}")
        info = csv_entries[0]
        with archive.open(info) as raw:
            columns = list(pd.read_csv(raw, nrows=0, encoding="utf-8-sig").columns)

        counts_1s: dict[int, int] = defaultdict(int)
        counts_10s: dict[int, int] = defaultdict(int)
        null_counts = dict.fromkeys(columns, 0)
        rows = 0
        decreases = 0
        previous = None
        interarrivals: list[np.ndarray] = []
        min_timestamp = None
        max_timestamp = None
        invalid_timestamps = 0
        negative_timestamps = 0
        duplicate_hashes = 0
        with archive.open(info) as raw:
            for chunk in pd.read_csv(raw, chunksize=chunk_rows, encoding="utf-8-sig", low_memory=False):
                rows += len(chunk)
                for column in columns:
                    null_counts[column] += int(chunk[column].isna().sum())
                numeric_timestamp = pd.to_numeric(chunk["timestamp_ms"], errors="coerce")
                invalid_timestamps += int(numeric_timestamp.isna().sum())
                numeric_timestamp = numeric_timestamp.dropna()
                invalid_timestamps += int((numeric_timestamp % 1 != 0).sum())
                timestamp = numeric_timestamp.astype("int64").to_numpy()
                negative_timestamps += int((timestamp < 0).sum())
                if len(timestamp) == 0:
                    continue
                min_timestamp = int(timestamp.min()) if min_timestamp is None else min(min_timestamp, int(timestamp.min()))
                max_timestamp = int(timestamp.max()) if max_timestamp is None else max(max_timestamp, int(timestamp.max()))
                if previous is not None:
                    decreases += int(timestamp[0] < previous)
                    interarrivals.append(np.asarray([timestamp[0] - previous], dtype="int64"))
                if len(timestamp) > 1:
                    delta = np.diff(timestamp)
                    decreases += int((delta < 0).sum())
                    interarrivals.append(delta)
                previous = int(timestamp[-1])
                for value in timestamp:
                    counts_1s[int(value // 1_000)] += 1
                    counts_10s[int(value // 10_000)] += 1
                hashes = chunk["hash"].dropna().astype(str).tolist()
                if hashes:
                    inserted = seen_db.executemany(
                        "INSERT OR IGNORE INTO transaction_hashes(hash) VALUES (?)",
                        ((value,) for value in hashes),
                    ).rowcount
                    duplicate_hashes += len(hashes) - inserted
            seen_db.commit()

        deltas = np.concatenate(interarrivals) if interarrivals else np.array([], dtype="int64")
        one_second = np.asarray(list(counts_1s.values()), dtype="int64")
        ten_second = np.asarray(list(counts_10s.values()), dtype="int64")
        return {
            "archive": str(path),
            "sha256": digest,
            "entries": [{"name": x.filename, "bytes": x.file_size, "compressed_bytes": x.compress_size} for x in entries],
            "csv_entry": info.filename,
            "archive_crc_valid": True,
            "columns": columns,
            "rows": rows,
            "null_counts": null_counts,
            "timestamp_field": "timestamp_ms",
            "invalid_timestamp_values": invalid_timestamps,
            "negative_timestamp_values": negative_timestamps,
            "utc_start": pd.to_datetime(min_timestamp, unit="ms", utc=True).isoformat(),
            "utc_end": pd.to_datetime(max_timestamp, unit="ms", utc=True).isoformat(),
            "chronological_decreases": decreases,
            "duplicate_transaction_hashes": duplicate_hashes,
            "interarrival_ms": {
                "count": int(len(deltas)),
                "min": int(deltas.min()),
                "median": float(np.median(deltas)),
                "p95": float(np.percentile(deltas, 95)),
                "max": int(deltas.max()),
                "nonpositive": int((deltas <= 0).sum()),
            },
            "tps_1s": {"bins": int(len(one_second)), "mean": float(one_second.mean()), "p95": float(np.percentile(one_second, 95)), "max": int(one_second.max())},
            "tps_10s": {"bins": int(len(ten_second)), "mean": float(ten_second.mean() / 10), "p95": float(np.percentile(ten_second, 95) / 10), "max_per_second_equiv": float(ten_second.max() / 10)},
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archives", nargs="+", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="flashbots-audit-") as temp_dir:
        db = sqlite3.connect(str(Path(temp_dir) / "hashes.sqlite"))
        db.execute("CREATE TABLE transaction_hashes(hash TEXT PRIMARY KEY)")
        report = {"archives": [audit_archive(path, db) for path in args.archives]}
        report["combined_duplicate_transaction_hashes"] = sum(
            item["duplicate_transaction_hashes"] for item in report["archives"]
        )
        db.close()
    report["schemas_identical"] = len({tuple(x["columns"]) for x in report["archives"]}) == 1
    report["can_combine"] = report["schemas_identical"] and all(
        item["invalid_timestamp_values"] == 0
        and item["negative_timestamp_values"] == 0
        and item["chronological_decreases"] == 0
        for item in report["archives"]
    )
    text = json.dumps(report, indent=2)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
