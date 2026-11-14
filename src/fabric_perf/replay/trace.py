"""Millisecond-resolution Flashbots arrival traces.

Rules (owner-approved, 2026-11-14):
- Source hashes are verified against the manifest before any row is read.
- Timestamps are never smoothed, clipped, deduplicated or re-sorted. The archive is already ordered
  (0 chronological decreases in the stored audit); a decrease is an IntegrityError, not something to repair.
- Exact ties (zero gaps) are kept as repeated values.
- Derived data is written only after every reconciliation check against the stored audit passes.
"""

from __future__ import annotations

import hashlib
import json
import platform
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from .common import DAY_MS, DAY_S, FLOAT_TOL, CheckReport, IntegrityError

# Exact-tie (zero-gap) counts registered in the stored archive audit. Hard acceptance values.
REGISTERED_TIE_GAPS = {"2026-09-21": 46_198, "2026-10-12": 27_802}


def sha256_file(path: str | Path, chunk_bytes: int = 1 << 22) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk_bytes)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest().upper()


def load_manifest(path: str | Path) -> dict[str, dict]:
    """Raw-archive manifest entries keyed by UTC date (YYYY-MM-DD)."""
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return {entry["filename"].split(".")[0]: entry for entry in data["raw_archives"]}


def verify_archive(path: str | Path, entry: dict) -> str:
    """Check size and SHA-256 against the manifest entry. Returns the digest."""
    size = Path(path).stat().st_size
    if size != int(entry["compressed_bytes"]):
        raise IntegrityError(f"{path}: size {size} != manifest {entry['compressed_bytes']}")
    digest = sha256_file(path)
    if digest != str(entry["sha256"]).upper():
        raise IntegrityError(f"{path}: SHA-256 {digest} != manifest {entry['sha256']}")
    return digest


def read_timestamps_ms(archive_path: str | Path, chunk_rows: int = 250_000) -> np.ndarray:
    """Stream only the timestamp_ms column of the archive's single CSV entry, in file order."""
    parts: list[np.ndarray] = []
    with zipfile.ZipFile(archive_path) as archive:
        names = [i.filename for i in archive.infolist() if not i.is_dir() and i.filename.lower().endswith(".csv")]
        if len(names) != 1:
            raise IntegrityError(f"{archive_path}: expected exactly one CSV entry, found {names}")
        with archive.open(names[0]) as raw:
            try:
                reader = pd.read_csv(
                    raw, usecols=["timestamp_ms"], dtype={"timestamp_ms": "float64"},
                    chunksize=chunk_rows, encoding="utf-8-sig",
                )
                for chunk in reader:
                    column = chunk["timestamp_ms"].to_numpy()
                    if np.isnan(column).any():
                        raise IntegrityError(f"{archive_path}: missing timestamp_ms value")
                    if (column != np.floor(column)).any():
                        raise IntegrityError(f"{archive_path}: non-integer timestamp_ms value")
                    parts.append(column.astype(np.int64))
            except (ValueError, KeyError) as exc:
                raise IntegrityError(f"{archive_path}: cannot parse timestamp_ms ({exc})") from exc
    if not parts:
        raise IntegrityError(f"{archive_path}: no rows")
    return np.concatenate(parts)


@dataclass
class DayTrace:
    date: str
    offsets_ms: np.ndarray  # int64 ms since 00:00:00.000 UTC of `date`, file order, ties preserved
    day_start_ms: int
    source_sha256: str
    source_bytes: int
    rows: int


def _offset_ms(iso: str, date: str) -> int:
    return int((pd.Timestamp(iso) - pd.Timestamp(date, tz="UTC")) // pd.Timedelta(milliseconds=1))


def build_day_trace(archive_path: str | Path, entry: dict) -> DayTrace:
    date = entry["filename"].split(".")[0]
    digest = verify_archive(archive_path, entry)
    timestamps = read_timestamps_ms(archive_path)
    if len(timestamps) != int(entry["rows"]):
        raise IntegrityError(f"{date}: {len(timestamps)} rows != manifest {entry['rows']}")
    day_start_ms = int((pd.Timestamp(date, tz="UTC") - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(milliseconds=1))
    offsets = timestamps - day_start_ms
    if offsets.min() < 0 or offsets.max() >= DAY_MS:
        raise IntegrityError(f"{date}: timestamps outside the UTC day (min {offsets.min()}, max {offsets.max()})")
    decreases = int((np.diff(offsets) < 0).sum())
    if decreases:
        raise IntegrityError(f"{date}: {decreases} chronological decreases; refusing to re-sort silently")
    if int(offsets[0]) != _offset_ms(entry["utc_start"], date) or int(offsets[-1]) != _offset_ms(entry["utc_end"], date):
        raise IntegrityError(f"{date}: first/last timestamp disagree with manifest utc_start/utc_end")
    return DayTrace(date, offsets, day_start_ms, digest, Path(archive_path).stat().st_size, len(offsets))


def read_series(series_dir: str | Path, date: str, frequency_s: int) -> np.ndarray:
    """Stored count series (arrival_count), with its timestamp grid checked."""
    frame = pd.read_csv(Path(series_dir) / f"series_{date}_{frequency_s}s.csv.gz")
    expected_bins = DAY_S // frequency_s
    if len(frame) != expected_bins:
        raise IntegrityError(f"series {date} {frequency_s}s: {len(frame)} bins != {expected_bins}")
    grid = pd.date_range(pd.Timestamp(date, tz="UTC"), periods=expected_bins, freq=f"{frequency_s}s")
    if not (pd.to_datetime(frame["timestamp_utc"], utc=True).to_numpy() == grid.to_numpy()).all():
        raise IntegrityError(f"series {date} {frequency_s}s: timestamp grid is not the regular UTC grid")
    return frame["arrival_count"].to_numpy(dtype=np.int64)


def reconcile_day(trace: DayTrace, audit: dict, quality: dict, series_dir: str | Path) -> CheckReport:
    """Compare the ms trace with the stored audit, data-quality audit and stored count series."""
    date = trace.date
    report = CheckReport(f"reconcile {date}")
    entry = next(e for e in audit["archives"] if e["csv_entry"].startswith(date))
    quality_by_freq = {e["frequency_seconds"]: e for e in quality["archives"] if e["date_utc"] == date}
    offsets = trace.offsets_ms
    counts1 = np.bincount(offsets // 1000, minlength=DAY_S).astype(np.int64)
    counts5 = counts1.reshape(-1, 5).sum(axis=1)
    counts10 = counts1.reshape(-1, 10).sum(axis=1)

    report.add("rows", entry["rows"], trace.rows)
    report.add("source_sha256", entry["sha256"].upper(), trace.source_sha256)
    report.add("total_arrivals_vs_quality_audit", quality_by_freq[1]["total_arrivals"], int(counts1.sum()))
    for freq, counts in ((1, counts1), (5, counts5), (10, counts10)):
        stored = read_series(series_dir, date, freq)
        report.add(f"series_{freq}s_bins_differing_from_stored", 0, int((stored != counts).sum()))
        report.add(f"zero_arrival_bins_{freq}s", quality_by_freq[freq]["zero_arrival_bins"], int((counts == 0).sum()))

    gaps = np.diff(offsets)
    ia = entry["interarrival_ms"]
    report.add("interarrival_count", ia["count"], len(gaps))
    report.add("interarrival_min_ms", ia["min"], int(gaps.min()))
    report.add("interarrival_median_ms", ia["median"], float(np.median(gaps)), FLOAT_TOL)
    report.add("interarrival_p95_ms", ia["p95"], float(np.quantile(gaps, 0.95)), FLOAT_TOL)
    report.add("interarrival_max_ms", ia["max"], int(gaps.max()))
    report.add("exact_tie_gaps_vs_audit", ia["nonpositive"], int((gaps <= 0).sum()))
    report.add("exact_tie_gaps_vs_registered", REGISTERED_TIE_GAPS[date], int((gaps == 0).sum()))
    report.add("negative_gaps", 0, int((gaps < 0).sum()))

    nonempty = counts1[counts1 > 0]
    t1 = entry["tps_1s"]
    report.add("tps_1s_nonempty_bins", t1["bins"], len(nonempty))
    report.add("tps_1s_mean_nonempty", t1["mean"], float(nonempty.mean()), FLOAT_TOL)
    report.add("tps_1s_p95_nonempty", t1["p95"], float(np.quantile(nonempty, 0.95)), FLOAT_TOL)
    report.add("tps_1s_max", t1["max"], int(nonempty.max()))
    t10 = entry["tps_10s"]
    per_second_10 = counts10 / 10.0
    report.add("tps_10s_bins", t10["bins"], len(counts10))
    report.add("tps_10s_mean", t10["mean"], float(per_second_10.mean()), FLOAT_TOL)
    report.add("tps_10s_p95", t10["p95"], float(np.quantile(per_second_10, 0.95)), 1e-6)
    report.add("tps_10s_max_per_second_equiv", t10["max_per_second_equiv"], float(per_second_10.max()), 1e-6)
    return report


def write_traces(traces: list[DayTrace], reports: list[CheckReport], out_dir: str | Path, code_files: list[str | Path]) -> dict:
    """Write arrays and provenance. Refuses to run unless every report passed; never overwrites differing data."""
    if not reports or not all(r.ok for r in reports):
        raise IntegrityError("reconciliation did not pass; refusing to write derived data")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summary = {
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "code_sha256": {Path(p).name: sha256_file(p) for p in code_files},
        "transform": "timestamp_ms - UTC day start; file order; ties preserved; no smoothing, clipping, dedup or re-sorting",
        "days": {},
    }
    for trace, report in zip(traces, reports):
        array_path = out / f"arrivals_{trace.date}_ms.npy"
        if array_path.exists():
            if not np.array_equal(np.load(array_path), trace.offsets_ms):
                raise IntegrityError(f"{array_path} exists with different content; refusing to overwrite")
        else:
            np.save(array_path, trace.offsets_ms)
        summary["days"][trace.date] = {
            "array": array_path.name,
            "array_sha256": sha256_file(array_path),
            "rows": trace.rows,
            "day_start_ms": trace.day_start_ms,
            "source_sha256": trace.source_sha256,
            "source_bytes": trace.source_bytes,
            "exact_tie_gaps": int((np.diff(trace.offsets_ms) == 0).sum()),
            "reconciliation": report.as_dicts(),
        }
    manifest_path = out / "flashbots_ms_manifest.json"
    manifest_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["manifest_sha256"] = sha256_file(manifest_path)
    return summary


def load_day_offsets(out_dir: str | Path, date: str) -> np.ndarray:
    """Load a derived ms trace and re-check it against its recorded hash."""
    out = Path(out_dir)
    manifest = json.loads((out / "flashbots_ms_manifest.json").read_text(encoding="utf-8"))
    info = manifest["days"][date]
    path = out / info["array"]
    if sha256_file(path) != info["array_sha256"]:
        raise IntegrityError(f"{path}: SHA-256 differs from flashbots_ms_manifest.json")
    return np.load(path)
