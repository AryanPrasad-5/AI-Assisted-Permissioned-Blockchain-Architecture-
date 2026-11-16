"""Build complete daily Flashbots arrival-count series (1/5/10 s) from the ZIP archives.

This is the ORIGINAL builder of the six stored series in data/processed/flashbots/. They have been verified
bin-for-bin by Flashbots/time_series/build_target_series.py (see series_verification.json), so there is no need to
rerun this. It refuses to overwrite existing series unless --force is given (P2 recorded their SHA-256).

    .venv/Scripts/python.exe -B Flashbots/time_series/build_flashbots_series.py --out-dir <new folder>
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import sys

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from fabric_perf.forecasting.flashbots import build_daily_series


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path("data/manifests/flashbots_mempool_manifest.yaml"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/processed/flashbots"))
    parser.add_argument("--force", action="store_true", help="overwrite existing series (they are hashed by the frozen P2 benchmark)")
    args = parser.parse_args()
    existing = sorted(args.out_dir.glob("series_*.csv.gz")) if args.out_dir.exists() else []
    if existing and not args.force:
        raise SystemExit(f"REFUSED: {len(existing)} series already exist in {args.out_dir}; use a new --out-dir or --force")
    manifest = yaml.safe_load(args.manifest.read_text(encoding="utf-8"))
    args.out_dir.mkdir(parents=True, exist_ok=True)
    source_audit_path = Path("experiments/results/flashbots_archive_audit.json")
    source_audit = json.loads(source_audit_path.read_text(encoding="utf-8")) if source_audit_path.exists() else {"archives": []}
    source_by_path = {str(Path(item["archive"]).resolve()): item for item in source_audit.get("archives", [])}
    audits = []
    for archive in manifest["raw_archives"]:
        path = Path(archive["source_path"])
        actual_hash = sha256(path)
        if actual_hash != archive["sha256"]:
            raise ValueError(f"Checksum mismatch for {path}: {actual_hash}")
        for frequency in (1, 5, 10):
            frame, audit = build_daily_series(path, frequency)
            output = args.out_dir / f"series_{audit['date_utc']}_{frequency}s.csv.gz"
            frame.to_csv(output, index=False, compression="gzip")
            source = source_by_path.get(str(path.resolve()), {})
            audit.update({
                "archive_crc_valid": source.get("archive_crc_valid"),
                "duplicate_transaction_hashes": source.get("duplicate_transaction_hashes"),
                "null_counts": source.get("null_counts"),
            })
            audit.update({"frequency_seconds": frequency, "output": str(output), "archive_sha256": actual_hash})
            audits.append(audit)
    (args.out_dir / "data_quality_audit.json").write_text(json.dumps({"source_archive_audit": str(source_audit_path), "archives": audits}, indent=2) + "\n", encoding="utf-8")
    (args.out_dir / "series_config.json").write_text(json.dumps({"timestamp_field": "timestamp_ms", "frequencies_seconds": [1, 5, 10], "dates_are_independent_sequences": True, "source_manifest": str(args.manifest), "source_archive_audit": str(source_audit_path)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"series": len(audits), "output_dir": str(args.out_dir)}, indent=2))


if __name__ == "__main__":
    main()
