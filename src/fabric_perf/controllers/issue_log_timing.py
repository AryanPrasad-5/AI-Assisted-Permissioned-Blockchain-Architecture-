"""Verifier for the issue-log write-timing sidecar (task A0p, 2026-11-22). OFFLINE ONLY: it reads two finished files and returns a document.

What it reads
-------------
* the issue log (``e1_issue_log/1``, written by ``e1_issue_log.js`` or by its timed successor ``e1_issue_log_timed.js``; the log format is identical), validated by the UNMODIFIED
  strict reader ``issue_log_reader.IssueLogTail`` (hash chain, sequence, state machine, clocks);
* the timing sidecar (``e1_issue_log_timing/1``, written only by the timed writer): an append-only, hash-chained companion whose ``write`` rows say when each log record's
  write returned and whose ``sync`` rows say when a sync of the log file returned.

What it establishes, and what it does not
-----------------------------------------
* A ``write`` row names a log record by rseq AND by that record's sha256 and creation time. The verifier checks the link against the validated log: a row that names a record the
  log does not hold, or whose digest differs, makes the evidence INVALID. The completion stamp is therefore never attached to the wrong event.
* ``write_end`` is the time ``fs.writeSync`` returned. It is NOT a durability claim. ``sync_end`` is the time ``fs.fsyncSync`` returned without an exception: the operating system
  accepted the flush request. It is not an absolute guarantee against every storage failure and the word "durable" is never used for it here.
* A log record with no ``write`` row is UNATTESTED. That is the signature of a crash between the log write and the sidecar write; it is allowed only for the final record, it is
  reported (never repaired, never back-filled) and the status is ``UNRESOLVED_TAIL``. An unattested record anywhere else, or after a clean sidecar stop, is INVALID.
* A log record not covered by any ``sync`` row has NO sync evidence. It is listed in ``unsynced_rseqs`` and is never described as persisted.
* A ``failure`` row is kept and reported; it never becomes a success.
* The sidecar's clock hooks are independent of the log's ``t_ms`` / ``mono_ms``. Differences are formed only between stamps of the SAME process monotonic clock
  (``*_mono_us``). Wall stamps are used for provenance and for an ordering plausibility check against the record's own creation time, never for a duration.

Nothing here writes, repairs or truncates a file, and nothing imports a process, network, container or model API.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from . import issue_log_reader as ilr
from .workload_coverage import GENESIS_SHA256, canonical, event_digest

TIMING_SCHEMA = "e1_issue_log_timing/1"
LOG_SCHEMA = ilr.ISSUE_LOG_SCHEMA
VERIFIED, UNRESOLVED_TAIL, INVALID = "VERIFIED", "UNRESOLVED_TAIL", "INVALID"
T_KINDS = ("tstart", "write", "sync", "failure", "tstop")
MAX_ROW_BYTES = 8192


class TimingSidecarError(ValueError):
    """The sidecar or the log cannot be verified. ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _fail(code: str, message: str) -> TimingSidecarError:
    return TimingSidecarError(code, message)


def _int(row: Mapping[str, Any], key: str, minimum: int = 0) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _fail("timing_field_invalid", f"sidecar row {row.get('tseq')!r} ({row.get('kind')}): {key} must be an integer >= {minimum}")
    return value


@dataclass
class TimingEvidence:
    status: str
    error: dict[str, Any] | None
    run_id: str | None = None
    worker_id: str | None = None
    round_index: int | None = None
    clock_domain: str | None = None
    log_records: int = 0
    log_partial_tail_bytes: int = 0
    sidecar_rows: int = 0
    sidecar_torn_tail_bytes: int = 0
    writes: dict[int, dict[str, Any]] = field(default_factory=dict)
    syncs: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    unattested_rseqs: list[int] = field(default_factory=list)
    unsynced_rseqs: list[int] = field(default_factory=list)
    has_stop: bool = False
    declarations: dict[str, Any] = field(default_factory=dict)
    inputs: list[dict[str, Any]] = field(default_factory=list)

    def usable(self) -> bool:
        """Timing may be computed from this evidence. ``UNRESOLVED_TAIL`` is usable for the attested records only; ``INVALID`` is not usable at all."""
        return self.status != INVALID

    def as_dict(self) -> dict[str, Any]:
        return {"schema": "e1_issue_log_timing_verification/1", "status": self.status, "error": self.error, "run_id": self.run_id, "worker_id": self.worker_id,
                "round_index": self.round_index, "clock_domain": self.clock_domain, "log_records": self.log_records, "log_partial_tail_bytes": self.log_partial_tail_bytes,
                "sidecar_rows": self.sidecar_rows, "sidecar_torn_tail_bytes": self.sidecar_torn_tail_bytes, "attested_records": len(self.writes), "syncs": len(self.syncs),
                "failures": list(self.failures), "unattested_rseqs": list(self.unattested_rseqs), "unsynced_rseqs_count": len(self.unsynced_rseqs),
                "unsynced_rseqs_tail": self.unsynced_rseqs[-5:], "has_stop": self.has_stop, "declarations": self.declarations, "inputs": self.inputs,
                "meaning": {"write_end": "fs.writeSync returned having accepted every byte; not a storage guarantee",
                            "sync_end": "fs.fsyncSync returned without an exception; the OS accepted the flush request; not a guarantee against every storage failure",
                            "unattested": "no write row exists for the record (a crash between the log write and the sidecar write, or a failed sidecar)",
                            "unsynced": "no sync row covers the record: no persistence is claimed for it"}}


def _sha(path: Path, data: bytes, role: str) -> dict[str, Any]:
    return {"role": role, "name": Path(path).name, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def load_validated_log(path: Path, *, allow_partial_tail: bool = True, max_clock_skew_ms: int = 1000) -> dict[str, Any]:
    """Validate a log with the unmodified strict reader and parse its records. A partial trailing record (a torn final write) is allowed only when ``allow_partial_tail``; it is
    reported, never consumed. Raises ``TimingSidecarError`` if the reader refuses the log: nothing is taken from a log that failed integrity."""
    path = Path(path)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise _fail("issue_log_unreadable", f"{path}: {exc}") from exc
    try:
        start = json.loads(raw.split(b"\n", 1)[0].decode("ascii"))
        policy = ilr.IssueLogPolicy(run_id=start["run_id"], worker_id=start["worker_id"], clock_domain=start["clock_domain"], max_tick_gap_ms=600_000,
                                    max_clock_skew_ms=max_clock_skew_ms)
    except (ValueError, KeyError, UnicodeDecodeError, ilr.IssueLogError) as exc:
        raise _fail("issue_log_start_unreadable", f"the first record of {path.name} cannot be read as a start record: {exc}") from exc
    tail = ilr.IssueLogTail(path, policy)
    stamp = ilr.ms_to_utc(int(start["t_ms"]))
    for _ in range(100_000):
        result = tail.poll(stamp)
        if tail.error is not None or result.records == 0 or tail.offset >= len(raw):
            break
    if tail.error is not None:
        raise _fail("issue_log_invalid", f"{tail.error.code}: {tail.error}")
    if tail.offset != len(raw) and not (tail.partial_tail_bytes and tail.offset + tail.partial_tail_bytes == len(raw)):
        raise _fail("issue_log_not_read_to_end", "the log was not read to its end")
    if tail.partial_tail_bytes and not allow_partial_tail:
        raise _fail("issue_log_partial_tail", f"{tail.partial_tail_bytes} bytes follow the last complete record")
    records = [json.loads(line) for line in raw[:tail.offset].decode("ascii").split("\n")[:-1]]
    return {"records": records, "run_id": policy.run_id, "worker_id": policy.worker_id, "round_index": start.get("round_index"), "clock_domain": policy.clock_domain,
            "partial_tail_bytes": tail.partial_tail_bytes, "input": _sha(path, raw, "issue_log"), "start": start}


def _read_sidecar_rows(raw: bytes) -> tuple[list[dict[str, Any]], int]:
    """Strictly parse the sidecar: canonical ASCII JSON, contiguous tseq, hash chain. Returns the rows and the byte length of an unterminated final line (a torn write)."""
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise _fail("sidecar_not_ascii", "the sidecar is not ASCII") from exc
    lines = text.split("\n")
    torn = len(lines[-1])
    rows: list[dict[str, Any]] = []
    prev = GENESIS_SHA256
    for number, line in enumerate(lines[:-1], 1):
        if not line.strip():
            raise _fail("sidecar_blank_line", f"sidecar line {number} is blank")
        if len(line) > MAX_ROW_BYTES:
            raise _fail("sidecar_row_too_long", f"sidecar line {number} is {len(line)} bytes")
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise _fail("sidecar_row_malformed", f"sidecar line {number} is not JSON") from exc
        if not isinstance(row, dict) or canonical(row) != line:
            raise _fail("sidecar_row_not_canonical", f"sidecar line {number} is not in canonical form (edited or re-serialised)")
        if row.get("schema") != TIMING_SCHEMA:
            raise _fail("sidecar_schema_mismatch", f"sidecar line {number} schema is {row.get('schema')!r}; a mixed writer/reader schema is refused")
        if row.get("kind") not in T_KINDS:
            raise _fail("sidecar_kind_unknown", f"sidecar line {number} has kind {row.get('kind')!r}")
        tseq = row.get("tseq")
        if isinstance(tseq, bool) or not isinstance(tseq, int) or tseq != number:
            raise _fail("sidecar_sequence_gap" if isinstance(tseq, int) and not isinstance(tseq, bool) and tseq > number else "sidecar_sequence_invalid",
                        f"sidecar line {number} carries tseq {tseq!r}")
        if row.get("prev") != prev:
            raise _fail("sidecar_chain_broken", f"sidecar row {number} does not follow row {number - 1}")
        if row.get("sha256") != event_digest(row, prev):
            raise _fail("sidecar_hash_mismatch", f"sidecar row {number} was altered after it was written")
        prev = row["sha256"]
        rows.append(row)
    return rows, torn


def verify_timing(log_path: Path, sidecar_path: Path, *, run_id: str | None = None, worker_id: str | None = None, max_clock_skew_ms: int = 1000) -> TimingEvidence:
    """Verify a timing sidecar against its issue log. Never raises: every evidence problem, including an unreadable input, is an INVALID result carrying the error code."""
    ev = TimingEvidence(status=INVALID, error=None)
    try:
        return _verify(ev, Path(log_path), Path(sidecar_path), run_id, worker_id, max_clock_skew_ms)
    except TimingSidecarError as exc:
        ev.status, ev.error = INVALID, {"code": exc.code, "message": str(exc)}
        return ev


def _verify(ev: TimingEvidence, log_path: Path, sidecar_path: Path, run_id: str | None, worker_id: str | None, skew: int) -> TimingEvidence:
    log = load_validated_log(log_path, allow_partial_tail=True, max_clock_skew_ms=skew)
    ev.inputs.append(log["input"])
    ev.run_id, ev.worker_id, ev.round_index, ev.clock_domain = log["run_id"], log["worker_id"], log["round_index"], log["clock_domain"]
    ev.log_records, ev.log_partial_tail_bytes = len(log["records"]), log["partial_tail_bytes"]
    if run_id is not None and run_id != log["run_id"]:
        raise _fail("run_identity_mismatch", f"the log is for run {log['run_id']!r}, not {run_id!r}")
    if worker_id is not None and worker_id != log["worker_id"]:
        raise _fail("worker_identity_mismatch", f"the log is for worker {log['worker_id']!r}, not {worker_id!r}")
    try:
        raw = sidecar_path.read_bytes()
    except OSError as exc:
        raise _fail("sidecar_unreadable", f"{sidecar_path}: {exc}") from exc
    ev.inputs.append(_sha(sidecar_path, raw, "timing_sidecar"))
    rows, torn = _read_sidecar_rows(raw)
    ev.sidecar_rows, ev.sidecar_torn_tail_bytes = len(rows), torn
    if not rows:
        raise _fail("sidecar_empty", "the sidecar holds no complete row")
    head = rows[0]
    if head["kind"] != "tstart":
        raise _fail("sidecar_not_started", "the first sidecar row must be tstart")
    for key, want in (("run_id", log["run_id"]), ("worker_id", log["worker_id"]), ("log_name", log_path.name), ("log_schema", LOG_SCHEMA), ("round_index", log["round_index"]),
                      ("clock_domain", log["clock_domain"])):
        if head.get(key) != want:
            raise _fail("sidecar_identity_mismatch", f"sidecar tstart {key} is {head.get(key)!r}, the log has {want!r}")
    if head.get("mono_unit") != "us" or head.get("wall_precision") != "ms":
        raise _fail("sidecar_units_invalid", "the sidecar must declare microsecond monotonic and millisecond wall stamps")
    ev.declarations = {k: head.get(k) for k in ("timing_writer_version", "wall_clock_source", "wall_precision", "mono_clock_source", "mono_unit", "mono_origin", "fsync_policy",
                                                "write_semantics", "sync_semantics")}
    records = {r["rseq"]: r for r in log["records"]}
    last_write: dict[str, Any] | None = None
    synced_through = 0
    stop_row: dict[str, Any] | None = None
    sync_for: dict[int, int] = {}
    for row in rows[1:]:
        if row.get("run_id") != log["run_id"] or row.get("worker_id") != log["worker_id"]:
            raise _fail("sidecar_identity_mismatch", f"sidecar row {row['tseq']} carries another run or worker")
        if ev.has_stop:
            raise _fail("sidecar_row_after_stop", f"sidecar row {row['tseq']} follows tstop")
        kind = row["kind"]
        if kind == "tstart":
            raise _fail("sidecar_start_repeated", f"sidecar row {row['tseq']} is a second tstart")
        if kind == "write":
            rseq = _int(row, "target_rseq", 1)
            expected = (max(ev.writes) + 1) if ev.writes else 1
            if rseq != expected:
                raise _fail("attestation_out_of_order", f"write row {row['tseq']} attests record {rseq}, expected {expected}")
            rec = records.get(rseq)
            if rec is None:
                raise _fail("attestation_for_missing_record", f"write row {row['tseq']} attests record {rseq}, which the log does not hold")
            for key, field_name in (("target_sha256", "sha256"), ("target_kind", "kind"), ("target_t_ms", "t_ms"), ("target_mono_ms", "mono_ms")):
                if row.get(key) != rec.get(field_name):
                    raise _fail("attestation_link_mismatch", f"write row {row['tseq']}: {key} {row.get(key)!r} differs from log record {rseq} ({rec.get(field_name)!r})")
            if row.get("write_status") != "returned":
                raise _fail("write_status_invalid", f"write row {row['tseq']} has status {row.get('write_status')!r}")
            ws_w, we_w = _int(row, "write_start_wall_ms", 1), _int(row, "write_end_wall_ms", 1)
            ws_m, we_m = _int(row, "write_start_mono_us"), _int(row, "write_end_mono_us")
            if we_m < ws_m or we_w < ws_w:
                raise _fail("timestamp_order_invalid", f"write row {row['tseq']}: the write ended before it started")
            if ws_w < rec["t_ms"] - skew:
                raise _fail("timestamp_order_invalid", f"write row {row['tseq']}: the write started {rec['t_ms'] - ws_w} ms before the record's own creation time")
            if last_write is not None:
                if ws_m < last_write["write_end_mono_us"]:
                    raise _fail("monotonic_regression", f"write row {row['tseq']} starts before the previous write ended")
                if ws_w < last_write["write_start_wall_ms"]:
                    raise _fail("clock_regression", f"write row {row['tseq']}: the wall clock moved back")
                if abs((ws_w - last_write["write_start_wall_ms"]) - (ws_m - last_write["write_start_mono_us"]) / 1000.0) > skew:
                    raise _fail("wall_clock_step", f"write row {row['tseq']}: wall and monotonic clocks disagree by more than {skew} ms (a clock step)")
            entry = {"rseq": rseq, "kind": rec["kind"], "sha256": rec["sha256"], "t_ms": rec["t_ms"], "write_start_wall_ms": ws_w, "write_end_wall_ms": we_w,
                     "write_start_mono_us": ws_m, "write_end_mono_us": we_m}
            ev.writes[rseq] = entry
            last_write = entry
        elif kind == "sync":
            frm, thr = _int(row, "covered_from_rseq", 1), _int(row, "covered_through_rseq", 0)
            if frm != synced_through + 1 or thr < frm - 1:
                raise _fail("sync_coverage_invalid", f"sync row {row['tseq']} covers {frm}..{thr}; records up to {synced_through} were already covered")
            if thr >= frm and thr not in ev.writes:                                 # thr == frm - 1: a sync that returned with no new record to cover (e.g. policy 'every' + tick)
                raise _fail("sync_before_attestation", f"sync row {row['tseq']} covers record {thr}, which has no earlier write row")
            if row.get("sync_status") != "returned" or row.get("call") != "fsyncSync":
                raise _fail("sync_status_invalid", f"sync row {row['tseq']} has status {row.get('sync_status')!r} / call {row.get('call')!r}")
            ss_w, se_w = _int(row, "sync_start_wall_ms", 1), _int(row, "sync_end_wall_ms", 1)
            ss_m, se_m = _int(row, "sync_start_mono_us"), _int(row, "sync_end_mono_us")
            if se_m < ss_m or se_w < ss_w:
                raise _fail("timestamp_order_invalid", f"sync row {row['tseq']}: the sync ended before it started")
            if thr >= 1 and ss_m < ev.writes[thr]["write_end_mono_us"]:
                raise _fail("timestamp_order_invalid", f"sync row {row['tseq']} started before the write of record {thr} returned, so it cannot cover it")
            index = len(ev.syncs)
            ev.syncs.append({"covered_from_rseq": frm, "covered_through_rseq": thr, "sync_start_wall_ms": ss_w, "sync_end_wall_ms": se_w, "sync_start_mono_us": ss_m,
                             "sync_end_mono_us": se_m})
            for rs in range(frm, thr + 1):
                sync_for[rs] = index
            synced_through = max(synced_through, thr)
        elif kind == "failure":
            if row.get("op") not in ("write", "sync"):
                raise _fail("failure_op_invalid", f"failure row {row['tseq']} has op {row.get('op')!r}")
            ev.failures.append({"op": row["op"], "target_rseq": row.get("target_rseq"), "target_kind": row.get("target_kind"), "error": row.get("error"),
                                "failure_wall_ms": row.get("failure_wall_ms")})
        else:                                                                        # tstop
            ev.has_stop, stop_row = True, row
            if _int(row, "writes") != len(ev.writes) or _int(row, "syncs") != len(ev.syncs) or _int(row, "failures") != len(ev.failures):
                raise _fail("stop_count_mismatch", "tstop counts differ from the rows before it")
            if _int(row, "synced_through_rseq") != synced_through:
                raise _fail("stop_count_mismatch", "tstop synced_through_rseq differs from the sync rows")
    attested = set(ev.writes)
    ev.unattested_rseqs = [r for r in range(1, ev.log_records + 1) if r not in attested]
    ev.unsynced_rseqs = [r for r in sorted(attested) if r not in sync_for]
    if stop_row is not None and _int(stop_row, "log_records") != ev.log_records:
        raise _fail("stop_count_mismatch", "tstop log_records differs from the number of records in the log")
    if ev.unattested_rseqs:
        tail_only = ev.unattested_rseqs == list(range(ev.log_records - len(ev.unattested_rseqs) + 1, ev.log_records + 1)) and len(ev.unattested_rseqs) <= 1
        if ev.has_stop or not tail_only:
            raise _fail("attestation_gap", f"log records {ev.unattested_rseqs[:5]} have no write row but the sidecar continues or was closed: not a crash tail")
    ev.status = UNRESOLVED_TAIL if (ev.unattested_rseqs or ev.sidecar_torn_tail_bytes or ev.log_partial_tail_bytes or not ev.has_stop) else VERIFIED
    ev.error = None
    return ev
