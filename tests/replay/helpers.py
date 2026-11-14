"""Synthetic fixtures for the replay tests. No raw Flashbots data is read here."""

from __future__ import annotations

import calendar
import hashlib
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

DATE = "2026-09-21"
DAY_START_MS = calendar.timegm((2026, 9, 21, 0, 0, 0)) * 1000


def iso_ms(epoch_ms: int) -> str:
    seconds, ms = divmod(int(epoch_ms), 1000)
    return pd.Timestamp(seconds, unit="s", tz="UTC").strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"


def make_zip(path: Path, offsets_ms, csv_name: str = f"{DATE}.csv", extra_entry: str | None = None,
             timestamp_text: list[str] | None = None) -> Path:
    stamps = timestamp_text if timestamp_text is not None else [str(DAY_START_MS + int(o)) for o in offsets_ms]
    lines = ["timestamp_ms,hash,chain_id"] + [f"{t},0x{i:064x},1" for i, t in enumerate(stamps)]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(csv_name, "\n".join(lines) + "\n")
        if extra_entry:
            archive.writestr(extra_entry, "timestamp_ms\n1\n")
    return path


def manifest_entry(zip_path: Path, offsets_ms, date: str = DATE, **overrides) -> dict:
    data = Path(zip_path).read_bytes()
    entry = {
        "filename": f"{date}.csv.zip",
        "sha256": hashlib.sha256(data).hexdigest().upper(),
        "compressed_bytes": len(data),
        "rows": len(offsets_ms),
        "utc_start": iso_ms(DAY_START_MS + int(offsets_ms[0])),
        "utc_end": iso_ms(DAY_START_MS + int(offsets_ms[-1])),
    }
    entry.update(overrides)
    return entry


def synthetic_offsets(seed: int = 7, n: int = 4000) -> np.ndarray:
    """Sorted day offsets with deliberate exact ties and sub-5 ms gaps."""
    rng = np.random.default_rng(seed)
    base = np.sort(rng.integers(100, 86_399_000, size=n))
    ties = base[rng.integers(0, n, size=300)]
    return np.sort(np.concatenate([base, ties])).astype(np.int64)


def write_series(series_dir: Path, offsets_ms: np.ndarray, date: str = DATE) -> None:
    counts1 = np.bincount(offsets_ms // 1000, minlength=86_400)
    for freq in (1, 5, 10):
        counts = counts1.reshape(-1, freq).sum(axis=1)
        grid = pd.date_range(pd.Timestamp(date, tz="UTC"), periods=len(counts), freq=f"{freq}s")
        frame = pd.DataFrame({"timestamp_utc": grid, "arrival_count": counts, "date_utc": date, "frequency_seconds": freq})
        frame.to_csv(series_dir / f"series_{date}_{freq}s.csv.gz", index=False)


def pandas_audit(offsets_ms: np.ndarray, date: str, sha: str, rows: int) -> tuple[dict, dict]:
    """Audit and quality-audit dictionaries computed with pandas, independently of the code under test."""
    series = pd.Series(offsets_ms)
    gaps = series.diff().dropna()
    by_second = pd.Series(offsets_ms // 1000).value_counts().reindex(range(86_400), fill_value=0).sort_index()
    nonempty = by_second[by_second > 0]
    ten = by_second.groupby(by_second.index // 10).sum() / 10.0
    audit_entry = {
        "csv_entry": f"{date}.csv", "sha256": sha, "rows": rows,
        "interarrival_ms": {"count": len(gaps), "min": float(gaps.min()), "median": float(gaps.median()),
                            "p95": float(gaps.quantile(0.95)), "max": float(gaps.max()), "nonpositive": int((gaps <= 0).sum())},
        "tps_1s": {"bins": len(nonempty), "mean": float(nonempty.mean()), "p95": float(nonempty.quantile(0.95)), "max": int(nonempty.max())},
        "tps_10s": {"bins": len(ten), "mean": float(ten.mean()), "p95": float(ten.quantile(0.95)), "max_per_second_equiv": float(ten.max())},
    }
    quality_entries = []
    for freq in (1, 5, 10):
        counts = by_second.to_numpy().reshape(-1, freq).sum(axis=1)
        quality_entries.append({"date_utc": date, "frequency_seconds": freq, "total_arrivals": int(counts.sum()),
                                "zero_arrival_bins": int((counts == 0).sum()), "output_bins": len(counts)})
    return {"archives": [audit_entry]}, {"archives": quality_entries}
