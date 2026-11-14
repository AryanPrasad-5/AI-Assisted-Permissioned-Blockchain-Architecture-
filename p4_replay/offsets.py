"""Replay schedule construction, validation and export. Pure and offline.

Rules enforced here (Phase 1):
  R1  Every source arrival in the window maps to exactly one scheduled CreateAsset transaction (1:1, source file order,
      exact ties kept as repeated offsets). Nothing is dropped, added, merged, deduplicated or re-sorted.
  R2  Absolute millisecond offsets are preserved. One-second counts are for analysis only and are never used to dispatch.
      Zero-arrival intervals and burst structure survive because they are simply absent from, or repeated in, the offsets.
  R3  The 60 s pre-roll is exported as its own file and replayed as its own unmeasured round. The measured replay is the
      half-open window [0, 600 s) in window-relative milliseconds, exactly.
  R4  Transaction identity survives: the CreateAsset key of every scheduled transaction is recorded together with the
      source row index, source hash and source timestamp, so a ledger can be reconciled against the archive.
"""

from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from . import ensure_paths

ensure_paths()

from fabric_perf.replay.common import IntegrityError  # noqa: E402  (existing component)
from fabric_perf.replay.windows import extract_window, second_counts  # noqa: E402  (existing component)

MEASURED_S = 600
PRE_ROLL_S = 60
PRE_ROUND_INDEX = 0          # Caliper round 0: unmeasured pre-roll, label wu-<run_id> (the slot P1b uses for its warm-up round)
MEASURED_ROUND_INDEX = 1     # Caliper round 1: measured replay, label m-<run_id>
KEY_RE = re.compile(r"^[A-Za-z0-9._-]+$")
RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,40}$")
MAX_KEY, MAX_SEQ = 128, 9_999_999
TX_MAP_HEADER = ["run_id", "round", "round_index", "seq", "create_asset_key", "source_day", "source_row_index", "source_hash",
                 "source_timestamp_ms", "day_offset_ms", "window_offset_ms", "intended_offset_ms"]


class ScheduleError(ValueError):
    """A schedule violates a replay rule. Never repaired silently."""


def tx_key(run_id: str, round_index: int, seq: int) -> str:
    """The CreateAsset key. Mirrors keyFor() in fabric-testbed/caliper/workloads/e1_workload_core.js exactly."""
    if not isinstance(run_id, str) or not RUN_ID_RE.match(run_id):
        raise ScheduleError(f"run id {run_id!r} must match {RUN_ID_RE.pattern}")
    if not isinstance(round_index, int) or isinstance(round_index, bool) or round_index < 0:
        raise ScheduleError(f"round index {round_index!r}")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0 or seq > MAX_SEQ:
        raise ScheduleError(f"seq {seq!r} outside 0..{MAX_SEQ}")
    key = f"{run_id}_{round_index}_{seq:07d}"
    if not KEY_RE.match(key) or len(key) > MAX_KEY:
        raise ScheduleError(f"key {key!r} violates the chaincode key rule")
    return key


def validate_offsets(offsets, length_ms: int, name: str = "schedule") -> np.ndarray:
    """int64, 1-D, non-empty, non-negative, non-decreasing (ties allowed), strictly below length_ms."""
    array = np.asarray(offsets)
    if array.ndim != 1 or array.size == 0:
        raise ScheduleError(f"{name}: must be a non-empty 1-D array")
    if array.dtype.kind not in "iu":
        if array.dtype.kind == "f" and np.array_equal(array, np.floor(array)) and np.isfinite(array).all():
            array = array.astype(np.int64)
        else:
            raise ScheduleError(f"{name}: offsets must be whole milliseconds")
    array = array.astype(np.int64, copy=False)
    if (array < 0).any():
        raise ScheduleError(f"{name}: negative offset")
    if (np.diff(array) < 0).any():
        raise ScheduleError(f"{name}: offsets must be non-decreasing (no re-sorting is done)")
    if int(array[-1]) >= length_ms:
        raise ScheduleError(f"{name}: last offset {int(array[-1])} ms is outside [0, {length_ms}) ms")
    return array


def schedule_bytes(offsets, newline: bytes = b"\n") -> bytes:
    """Caliper replay-rate TEXT body: one integer ms offset per line, explicit separator, no trailing newline.

    LF is the default (the P4 draft's blocker 5: Path.write_text writes CRLF on Windows). The separator is a parameter only so that
    the legacy CRLF form can be re-derived to prove lineage to the P1-P5 schedule hashes.
    """
    return newline.join(str(int(v)).encode("ascii") for v in np.asarray(offsets, dtype=np.int64))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class ReplayWindow:
    window_id: str
    day: str
    start_s: int
    length_s: int
    pre_roll_s: int
    measured_ms: np.ndarray        # int64, window-relative, in [0, length_s * 1000)
    pre_ms_rel: np.ndarray         # int64, window-relative, in [-pre_roll_s * 1000, 0)
    measured_src_index: np.ndarray  # int64, row positions in the day file (file order)
    pre_src_index: np.ndarray

    @property
    def length_ms(self) -> int:
        return self.length_s * 1000

    @property
    def pre_ms(self) -> np.ndarray:
        """Pre-roll offsets relative to the START OF THE PRE-ROLL ROUND, in [0, pre_roll_s * 1000)."""
        return self.pre_ms_rel + self.pre_roll_s * 1000

    @property
    def day_start_offset_ms(self) -> int:
        return self.start_s * 1000


def build_window(day_offsets_ms: np.ndarray, window_id: str, day: str, start_s: int, length_s: int = MEASURED_S,
                 pre_roll_s: int = PRE_ROLL_S) -> ReplayWindow:
    """Slice a day trace with the existing extract_window (reused) and add the source row positions. Both must agree."""
    window = extract_window(day_offsets_ms, day, start_s, length_s, pre_roll_s)
    lo = start_s * 1000
    a = int(np.searchsorted(day_offsets_ms, lo - pre_roll_s * 1000, side="left"))
    b = int(np.searchsorted(day_offsets_ms, lo, side="left"))
    c = int(np.searchsorted(day_offsets_ms, lo + length_s * 1000, side="left"))
    measured_idx = np.arange(b, c, dtype=np.int64)
    pre_idx = np.arange(a, b, dtype=np.int64)
    if not (np.array_equal(day_offsets_ms[b:c] - lo, window.offsets_ms) and np.array_equal(day_offsets_ms[a:b] - lo, window.pre_offsets_ms)):
        raise IntegrityError("source row positions disagree with extract_window")
    measured = validate_offsets(window.offsets_ms, length_s * 1000, "measured window")
    pre = np.asarray(window.pre_offsets_ms, dtype=np.int64)
    if pre.size and (pre.min() < -pre_roll_s * 1000 or pre.max() >= 0 or (np.diff(pre) < 0).any()):
        raise ScheduleError("pre-roll offsets must be non-decreasing and inside [-pre_roll, 0)")
    return ReplayWindow(window_id, day, start_s, length_s, pre_roll_s, measured, pre, measured_idx, pre_idx)


def pre_roll_schedule(window: ReplayWindow) -> np.ndarray:
    """Pre-roll offsets for the pre-roll round. May be empty only if the source has no arrival in the 60 s before the window."""
    if window.pre_ms.size == 0:
        raise ScheduleError("the pre-roll holds no arrivals; an empty round cannot be replayed (owner decision)")
    return validate_offsets(window.pre_ms, window.pre_roll_s * 1000, "pre-roll")


def rounds(window: ReplayWindow) -> list[dict]:
    """The two unmeasured/measured rounds in execution order, each with its own offsets."""
    return [
        {"name": "pre_roll", "round": PRE_ROUND_INDEX, "measured": False, "offsets": pre_roll_schedule(window),
         "length_ms": window.pre_roll_s * 1000, "src_index": window.pre_src_index, "window_offsets": window.pre_ms_rel},
        {"name": "measured", "round": MEASURED_ROUND_INDEX, "measured": True,
         "offsets": validate_offsets(window.measured_ms, window.length_ms, "measured window"),
         "length_ms": window.length_ms, "src_index": window.measured_src_index, "window_offsets": window.measured_ms},
    ]


def second_count_profile(offsets, length_s: int) -> np.ndarray:
    """Per-second counts for ANALYSIS (existing second_counts, reused). Never used to dispatch."""
    return second_counts(np.asarray(offsets, dtype=np.int64), length_s)


# ----------------------------------------------------------------------------------------------- identity and the tx map
def build_tx_map(run_id: str, window: ReplayWindow, *, day_start_ms: int | None = None, identifiers: dict | None = None) -> list[dict]:
    """One row per scheduled transaction, both rounds. `identifiers` = {'hash': array-like of str, 'timestamp_ms': array-like},
    indexed by the DAY-FILE row position (so hashes line up with source_row_index); None leaves source_hash empty."""
    rows: list[dict] = []
    for rnd in rounds(window):
        for seq, (src, win_off, intended) in enumerate(zip(rnd["src_index"], rnd["window_offsets"], rnd["offsets"])):
            src = int(src)
            day_off = window.start_s * 1000 + int(win_off)
            row = {"run_id": run_id, "round": rnd["name"], "round_index": rnd["round"], "seq": seq,
                   "create_asset_key": tx_key(run_id, rnd["round"], seq), "source_day": window.day, "source_row_index": src,
                   "source_hash": "", "source_timestamp_ms": "", "day_offset_ms": day_off, "window_offset_ms": int(win_off),
                   "intended_offset_ms": int(intended)}
            if day_start_ms is not None:
                row["source_timestamp_ms"] = day_start_ms + day_off
            if identifiers is not None:
                row["source_hash"] = str(identifiers["hash"][src])
                stamp = int(identifiers["timestamp_ms"][src])
                if day_start_ms is not None and stamp != day_start_ms + day_off:
                    raise IntegrityError(f"source row {src}: archive timestamp {stamp} != trace-derived {day_start_ms + day_off}")
                row["source_timestamp_ms"] = stamp
            rows.append(row)
    keys = [r["create_asset_key"] for r in rows]
    if len(set(keys)) != len(keys):
        raise ScheduleError("duplicate CreateAsset keys in the transaction map")
    return rows


def write_tx_map(path: Path, rows: list[dict]) -> str:
    """Create-exclusive CSV with LF line endings. Returns the SHA-256."""
    with Path(path).open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TX_MAP_HEADER, lineterminator="\n", extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)
    return sha256_hex(Path(path).read_bytes())


def read_tx_map(path: Path) -> list[dict]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


# ----------------------------------------------------------------------------------------------- source identifiers (archive)
def load_archive_identifiers(archive_path: str | Path, entry: dict, a: int, c: int, chunk_rows: int = 250_000) -> dict:
    """Read hash and timestamp_ms for day-file rows [a, c) from the manifest-verified archive. Read-only.
    The archive is verified (size and SHA-256) against its manifest entry before any row is read."""
    import pandas as pd
    from fabric_perf.replay.trace import verify_archive
    import zipfile

    verify_archive(archive_path, entry)
    hashes: dict[int, str] = {}
    stamps: dict[int, int] = {}
    with zipfile.ZipFile(archive_path) as archive:
        names = [i.filename for i in archive.infolist() if not i.is_dir() and i.filename.lower().endswith(".csv")]
        if len(names) != 1:
            raise IntegrityError(f"{archive_path}: expected exactly one CSV entry, found {names}")
        with archive.open(names[0]) as raw:
            position = 0
            for chunk in pd.read_csv(raw, usecols=["timestamp_ms", "hash"], dtype={"timestamp_ms": "int64", "hash": "string"},
                                     chunksize=chunk_rows, encoding="utf-8-sig"):
                lo, hi = max(a, position), min(c, position + len(chunk))
                if lo < hi:
                    part = chunk.iloc[lo - position:hi - position]
                    for idx, h, t in zip(range(lo, hi), part["hash"].tolist(), part["timestamp_ms"].tolist()):
                        hashes[idx], stamps[idx] = h, int(t)
                position += len(chunk)
                if position >= c:
                    break
    if len(hashes) != c - a:
        raise IntegrityError(f"archive holds fewer rows than requested ({len(hashes)} of {c - a})")
    return {"hash": hashes, "timestamp_ms": stamps}


# ----------------------------------------------------------------------------------------------- registered windows (P1-P5 records)
def load_registered_window(processed_dir: str | Path, window_id: str) -> tuple[ReplayWindow, dict]:
    """Rebuild a registered window from the stored derived trace and prove it equals what P1-P5 recorded.

    Reads (never writes) `<processed>/windows/<window_id>/window_record.json` and the hash-checked day array. Raises
    IntegrityError unless: the selected tile's n_tx equals the rebuilt count, and the measured offsets, written in the LEGACY
    CRLF form, hash to the window record's approach-A schedule hash (so the schedule is byte-for-byte the one P1-P5 built).
    """
    import json
    from fabric_perf.replay.trace import load_day_offsets

    processed = Path(processed_dir)
    record = json.loads((processed / "windows" / window_id / "window_record.json").read_text(encoding="utf-8"))
    day_offsets = load_day_offsets(processed, record["day"])
    window = build_window(day_offsets, window_id, record["day"], int(record["start_s"]), int(record["length_s"]), int(record["pre_roll_s"]))
    legacy = sha256_hex(schedule_bytes(window.measured_ms, b"\r\n")).upper()
    expected = str(record["schedule_sha256"]["A"]).upper()
    lineage = {"window_record": str(processed / "windows" / window_id / "window_record.json"),
               "n_tx_expected": int(record["selected_tile"]["n_tx"]), "n_tx_rebuilt": int(window.measured_ms.size),
               "legacy_crlf_sha256_rebuilt": legacy, "legacy_crlf_sha256_recorded": expected,
               "equal_to_p1_p5_schedule": legacy == expected,
               "pre_roll_n_tx": int(window.pre_ms.size)}
    if lineage["n_tx_expected"] != lineage["n_tx_rebuilt"]:
        raise IntegrityError(f"{window_id}: rebuilt {lineage['n_tx_rebuilt']} arrivals, window record says {lineage['n_tx_expected']}")
    if legacy != expected:
        raise IntegrityError(f"{window_id}: rebuilt schedule hash {legacy} != window_record approach-A hash {expected}")
    return window, lineage


# ----------------------------------------------------------------------------------------------- export (pre-roll and measured files)
def export_schedule_files(window: ReplayWindow, out_dir: Path) -> dict:
    """Write schedule_pre_roll.txt and schedule_measured.txt (create-exclusive, LF, no trailing newline). Returns the hashes.
    Includes the legacy CRLF-form hash of the measured file so it can be compared with the P1-P5 window_record."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=False)
    info: dict = {}
    for rnd in rounds(window):
        body = schedule_bytes(rnd["offsets"])
        path = out / f"schedule_{rnd['name']}.txt"
        with path.open("xb") as handle:
            handle.write(body)
        offsets = rnd["offsets"]
        info[rnd["name"]] = {"file": path.name, "sha256": sha256_hex(body), "n_tx": int(offsets.size), "bytes": len(body),
                             "first_offset_ms": int(offsets[0]), "last_offset_ms": int(offsets[-1]),
                             "zero_gaps": int((np.diff(offsets) == 0).sum()),
                             "legacy_crlf_sha256": sha256_hex(schedule_bytes(offsets, b"\r\n"))}
    return info
