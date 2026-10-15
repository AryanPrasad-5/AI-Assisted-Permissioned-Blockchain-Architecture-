"""Shared utilities: project paths, checksums, and result serialization."""

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Union


def project_root() -> Path:
    """Repository root (src/fabric_perf/utils/helpers.py <- utils <- fabric_perf <- src <- root)."""
    return Path(__file__).resolve().parents[3]


def default_hfbtp_raw_dir() -> Path:
    """Default location of the raw HFBTP dataset."""
    return project_root() / "data" / "raw" / "hfbtp"


def default_manifest_dir() -> Path:
    """Default location of dataset manifests."""
    return project_root() / "data" / "manifests"


def experiments_dir() -> Path:
    """Directory holding all generated experiment artefacts."""
    return project_root() / "experiments"


def results_dir() -> Path:
    d = experiments_dir() / "results"
    d.mkdir(parents=True, exist_ok=True)
    return d


def figures_dir() -> Path:
    d = experiments_dir() / "figures"
    d.mkdir(parents=True, exist_ok=True)
    return d


def logs_dir() -> Path:
    d = experiments_dir() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def sha256_of_file(path: Union[str, Path]) -> str:
    """SHA-256 of a file, streamed (works for large raw datasets)."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def md5_of_file(path: Union[str, Path]) -> str:
    """MD5 of a file, streamed."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def save_json(data: Dict[str, Any], path: Union[str, Path]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, default=str)
    return path


def save_csv_rows(rows: list, path: Union[str, Path]) -> Path:
    """Write a list of flat dicts as CSV with a stable column order."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return path
    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def utc_timestamp() -> str:
    """Current UTC time as an ISO-8601 string (for run manifests only — never
    presented as a dataset property, since HFBTP itself has no timestamps)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def import_json(path: Union[str, Path]) -> Dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
