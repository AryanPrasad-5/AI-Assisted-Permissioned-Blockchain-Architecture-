"""Shared helpers for the LA-3 telemetry tools: block fetch, metrics scraper, resource sampler, clock
handshake and run orchestrator. Standard library only (also runs under the WSL system python3).

Status vocabulary (the same as e1_log_collector.py):
  COMPLETE                no anomaly                                        exit 0
  COMPLETE_WITH_WARNINGS  warnings only                                     exit 0
  INCOMPLETE              a completeness error (missing or malformed data)  exit 1
  INCONSISTENT            no completeness error, but a consistency error    exit 1
  invalid request, unreadable input or an existing output directory         exit 2

Every anomaly kind must be declared by the tool as completeness, consistency or warning. An
undeclared kind raises, so nothing can be recorded without a severity.
"""
from __future__ import annotations

import csv
import datetime
import hashlib
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable

COMPLETE, COMPLETE_WITH_WARNINGS = "COMPLETE", "COMPLETE_WITH_WARNINGS"
INCOMPLETE, INCONSISTENT = "INCOMPLETE", "INCONSISTENT"
EXIT = {COMPLETE: 0, COMPLETE_WITH_WARNINGS: 0, INCOMPLETE: 1, INCONSISTENT: 1}
USABLE = (COMPLETE, COMPLETE_WITH_WARNINGS)

# Channel batch symbols. One table for every E1 tool, test, record and document (owner ruling
# 2026-11-09): P is PreferredMaxBytes in bytes, never a message count.
SYMBOLS = {
    "B": ("Orderer.BatchSize.MaxMessageCount", "transactions per block (count)"),
    "tau": ("Orderer.BatchTimeout", "Go duration"),
    "P": ("Orderer.BatchSize.PreferredMaxBytes", "bytes"),
    "A": ("Orderer.BatchSize.AbsoluteMaxBytes", "bytes"),
}

Runner = Callable[[list[str]], "tuple[int | None, bytes, bytes]"]


def subprocess_runner(argv: list[str], timeout: float = 120.0) -> tuple[int | None, bytes, bytes]:
    proc = subprocess.run(argv, capture_output=True, timeout=timeout, check=False)
    return proc.returncode, proc.stdout, proc.stderr


@dataclass
class Anomaly:
    severity: str          # error | warning
    category: str          # completeness | consistency | warning
    kind: str
    subject: str = ""      # container, target, block or file the anomaly is about
    detail: str = ""
    source: str = ""       # file:line, tick or round reference
    raw: str = ""          # offending text, kept verbatim


class Anomalies:
    """Ordered anomaly list with declared kinds."""

    def __init__(self, completeness: Iterable[str], consistency: Iterable[str], warnings: Iterable[str]):
        self.kinds = {k: "completeness" for k in completeness}
        for k in consistency:
            self.kinds[k] = "consistency"
        for k in warnings:
            self.kinds[k] = "warning"
        self.items: list[Anomaly] = []

    def add(self, kind: str, subject: str = "", detail: str = "", source: str = "", raw: str = "") -> Anomaly:
        if kind not in self.kinds:
            raise ValueError(f"undeclared anomaly kind {kind!r}")
        cat = self.kinds[kind]
        a = Anomaly("warning" if cat == "warning" else "error", cat, kind, str(subject), detail, source, raw)
        self.items.append(a)
        return a

    def of(self, kind: str) -> list[Anomaly]:
        return [a for a in self.items if a.kind == kind]

    def status(self) -> str:
        cats = {a.category for a in self.items}
        if "completeness" in cats:
            return INCOMPLETE
        if "consistency" in cats:
            return INCONSISTENT
        return COMPLETE_WITH_WARNINGS if cats else COMPLETE

    def counts(self) -> dict:
        out: dict[str, dict[str, int]] = {"error": {}, "warning": {}}
        for a in self.items:
            out[a.severity][a.kind] = out[a.severity].get(a.kind, 0) + 1
        return out

    def rows(self) -> list[dict]:
        return [asdict(a) for a in self.items]


ANOMALY_COLS = ["severity", "category", "kind", "subject", "detail", "source", "raw"]
_ORDER = [COMPLETE, COMPLETE_WITH_WARNINGS, INCONSISTENT, INCOMPLETE]


def worst(statuses: Iterable[str]) -> str:
    """The least usable status (INCOMPLETE > INCONSISTENT > COMPLETE_WITH_WARNINGS > COMPLETE)."""
    return max(statuses, key=_ORDER.index, default=COMPLETE)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str | None:
    return sha256_bytes(path.read_bytes()) if path.exists() else None


def utc_iso(ns: int | None) -> str:
    if ns is None:
        return ""
    dt = datetime.datetime.fromtimestamp(ns // 1_000_000_000, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns % 1_000_000_000:09d}Z"


def _cell(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (list, tuple)):
        return ";".join(str(x) for x in v)
    return v


def write_csv_x(path: Path, cols: list[str], rows: list[dict]) -> str:
    """Write a CSV with LF line ends; refuses to overwrite. Returns its SHA-256.
    A row key that is not a column raises: no field is ever dropped silently."""
    for r in rows:
        extra = set(r) - set(cols)
        if extra:
            raise ValueError(f"{path.name}: row keys not in the column list: {sorted(extra)}")
    with path.open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n", extrasaction="raise")
        w.writeheader()
        for r in rows:
            w.writerow({c: _cell(r.get(c)) for c in cols})
    return sha256_file(path)


def write_json_x(path: Path, obj) -> str:
    with path.open("x", encoding="utf-8", newline="\n") as fh:
        json.dump(obj, fh, indent=2, default=str)
        fh.write("\n")
    return sha256_file(path)


def write_bytes_x(path: Path, data: bytes) -> str:
    with path.open("xb") as fh:
        fh.write(data)
    return sha256_bytes(data)


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


_WIN_PATH = re.compile(r"^[A-Za-z]:[\\/]|\\")


def windows_style_paths(obj, where: str = "") -> list[str]:
    """Keys whose string values look like Windows paths (drive letter or backslash). A Linux process in WSL
    cannot open them; translate with `wslpath -u`."""
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out += windows_style_paths(v, f"{where}/{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out += windows_style_paths(v, f"{where}/{i}")
    elif isinstance(obj, str) and _WIN_PATH.search(obj):
        out.append(f"{where}={obj[:80]}")
    return out


def provenance(script: str) -> dict:
    p = Path(script).resolve()
    return {"path": p.name, "sha256": sha256_file(p)}
