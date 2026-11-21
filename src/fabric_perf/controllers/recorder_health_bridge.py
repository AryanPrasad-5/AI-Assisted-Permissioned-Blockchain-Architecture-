"""Recorder-to-journal bridge (task A0j, 2026-11-21): connects the arrival recorder's health spool to the ONE collector-health journal.

Why a bridge and not a second journal writer
---------------------------------------------
``collector_health.CollectorHealthJournal`` is the only implementation of the hash chain, and ``workload_coverage`` the only validator.
The Node recorder (``scripts/e1/caliper/e1_per_tx_recorder_v3.js``) therefore writes a plain, sequence-numbered SPOOL
(``recorder_health_spool/1``) and this module replays it, row by row, through that journal. The spool is not tamper-evident by itself; the
journal it produces is. Replay is deterministic (no ingest-time field enters a row), so re-ingesting the same spool gives the same journal.

What the evidence can and cannot establish (stated here so no caller has to guess)
----------------------------------------------------------------------------------
Every journal this module writes declares ``coverage_scope = "recorder_callback"``: the recorder was running, its TxSubmitted callback fed
it events and they were durably appended to the live submits file before each heartbeat. That is RECORDER OPERATIONAL COVERAGE. It is NOT
proof that no source event was missed. ``recorder_coverage`` therefore refuses to certify a zero-arrival second from it unless an
independent source count agrees. Nothing here back-fills, repairs or retroactively certifies time.

Fail-closed behaviour
---------------------
A torn final spool line is reported and dropped (never repaired). A gap in the spool sequence ends ingestion at the gap. A missing ``stop`` is
reported and NOT synthesised (a killed recorder leaves none). A wall-clock step (wall delta and monotonic delta disagree) opens a journal
``error`` that is never recovered by the bridge. A refused journal write raises ``BridgeError``; nothing healthy is implied.

No network, process, Fabric, Docker, model or forecaster is used. Files are only read, and only the new journal file is written.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from .collector_health import CollectorHealthJournal
from .workload_coverage import CoverageEvidenceError, CoveragePolicy, parse_utc, read_journal_file, sha256_text

SPOOL_SCHEMA = "recorder_health_spool/1"
BRIDGE_REPORT_SCHEMA = "recorder_health_ingest_report/1"
COVERAGE_SCOPE_RECORDER = "recorder_callback"
ORIGIN_LIVE_RECORDER = "live_recorder"
ORIGIN_SYNTHETIC = "synthetic_fixture"
EVIDENCE_ORIGINS = (ORIGIN_LIVE_RECORDER, ORIGIN_SYNTHETIC)
SPOOL_KINDS = ("start", "heartbeat", "error", "recovered", "stop")
HEARTBEAT_PHASES = ("heartbeat", "checkpoint", "window_close")
CORE_ROW_KEYS = frozenset({"schema", "seq", "kind", "t_utc", "source_id", "collector_id", "collector_instance", "run_id", "channel_id",
                           "dataset_id", "prev", "sha256"})
RECORDER_START_EXTRAS = ("coverage_scope", "evidence_origin", "recorder_version", "options_sha256", "lifecycle", "declared_policy",
                         "source_count_declared", "source_counter", "worker", "round", "label")
RECORDER_BEAT_EXTRAS = ("phase", "progress", "source_events_expected", "source_basis", "source_independence_claim", "final_files_present",
                        "live_stream_ok", "health_anomaly_codes")

# ENGINEERING PROPOSAL, NOT A VALIDATED SETTING. Nothing in this repository applies these automatically; the operator must pass a policy.
CANDIDATE_PROPOSAL = {
    "label": "engineering proposal requiring later empirical verification; not scientifically validated",
    "heartbeat_interval_s": 1,
    "max_heartbeat_gap_s": 5.0,
    "freshness_limit_s": 10.0,
    "clock_tolerance_ms": 250.0,
    "basis": ("interval = the 1 s bin of the arrival series (watermarks are whole seconds); gap = 5 intervals so one late timer tick or a "
              "brief event-loop stall does not manufacture a gap but a multi-second stall does; freshness = 2 gaps so a collector that is "
              "merely on schedule is never called stale; tolerance = a quarter of a bin. None of this was measured on real Caliper "
              "scheduling or on Windows-WSL timing."),
}


class BridgeError(ValueError):
    """The spool, the journal or the policy cannot be bridged. ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _positive(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise BridgeError("policy_invalid", f"{name} must be a finite positive number")
    return float(value)


@dataclass(frozen=True)
class RecorderHealthPolicy:
    """Operator-supplied timing contract. There are NO defaults: every value must be stated and justified by the experiment.

    ``heartbeat_interval_s`` how often the recorder writes a heartbeat (a whole number of seconds: watermarks are whole seconds).
    ``max_heartbeat_gap_s``  longest silence after which the collector can no longer certify the time in between (a coverage gap).
    ``freshness_limit_s``    oldest history end the Stage 3 store accepts as current.
    ``clock_tolerance_ms``   tolerated disagreement between a wall-clock delta and a monotonic delta before a clock step is declared.
    """

    heartbeat_interval_s: int
    max_heartbeat_gap_s: float
    freshness_limit_s: float
    clock_tolerance_ms: float

    def __post_init__(self) -> None:
        interval = _positive(self.heartbeat_interval_s, "heartbeat_interval_s")
        if isinstance(self.heartbeat_interval_s, bool) or not float(interval).is_integer():
            raise BridgeError("policy_invalid", "heartbeat_interval_s must be a whole number of seconds")
        gap, fresh = _positive(self.max_heartbeat_gap_s, "max_heartbeat_gap_s"), _positive(self.freshness_limit_s, "freshness_limit_s")
        _positive(self.clock_tolerance_ms, "clock_tolerance_ms")
        if gap < 2 * interval:
            raise BridgeError("policy_inconsistent", "max_heartbeat_gap_s must be at least twice heartbeat_interval_s, or one late tick "
                                                     "makes a coverage gap")
        if fresh < gap:
            raise BridgeError("policy_inconsistent", "freshness_limit_s must be at least max_heartbeat_gap_s, or an on-schedule collector "
                                                     "would be called stale")
        if self.clock_tolerance_ms >= 1000 * interval:
            raise BridgeError("policy_inconsistent", "clock_tolerance_ms must be smaller than one heartbeat interval")

    def to_coverage_policy(self) -> CoveragePolicy:
        return CoveragePolicy(max_heartbeat_gap_s=self.max_heartbeat_gap_s)

    def as_dict(self) -> dict[str, Any]:
        return {"heartbeat_interval_s": self.heartbeat_interval_s, "max_heartbeat_gap_s": self.max_heartbeat_gap_s,
                "freshness_limit_s": self.freshness_limit_s, "clock_tolerance_ms": self.clock_tolerance_ms,
                "basis": "operator-supplied; no scientific justification is claimed by the code"}


# ------------------------------------------------------------------------------------------------------------------ journal
class _RowClock:
    """The journal clock for one replayed row: the collector's own time stamp, never the ingest time."""

    def __init__(self) -> None:
        self.now: datetime | None = None

    def __call__(self) -> datetime:
        if self.now is None:
            raise BridgeError("clock_unset", "no row time has been set")
        return self.now


class RecorderHealthJournal(CollectorHealthJournal):
    """``CollectorHealthJournal`` plus a validated pass-through of recorder fields. Chain, ordering and state machine are inherited."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._pending_extra: dict[str, Any] = {}

    def with_extra(self, extra: Mapping[str, Any]) -> "RecorderHealthJournal":
        clash = CORE_ROW_KEYS & set(extra)
        if clash:
            raise BridgeError("extra_field_clash", f"recorder fields may not override journal fields: {sorted(clash)}")
        self._pending_extra = {k: v for k, v in extra.items() if v is not None}
        return self

    def _append(self, kind: str, **fields: Any) -> dict[str, Any]:
        extra, self._pending_extra = self._pending_extra, {}
        overlap = set(extra) & set(fields)
        if overlap:
            raise BridgeError("extra_field_clash", f"recorder fields repeat journal fields: {sorted(overlap)}")
        return super()._append(kind, **fields, **extra)


# -------------------------------------------------------------------------------------------------------------------- spool
@dataclass
class SpoolRead:
    rows: list
    torn_tail: bool
    sha256: str
    duplicates_dropped: int
    break_at: int | None            # the spool sequence number that was expected next when the sequence broke
    break_reason: str | None


def read_spool(path: Path) -> SpoolRead:
    """Read a ``recorder_health_spool/1`` file. A last line without a newline is a torn write: reported, dropped, never repaired."""
    raw = Path(path).read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BridgeError("spool_not_utf8", f"{Path(path).name} is not UTF-8") from exc
    lines = text.split("\n")
    torn = bool(lines[-1])
    rows: list = []
    duplicates = 0
    break_at = break_reason = None
    for number, line in enumerate(lines[:-1], 1):
        if not line.strip():
            raise BridgeError("spool_blank_line", f"spool line {number} is blank")
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BridgeError("spool_row_malformed", f"spool line {number} is not JSON") from exc
        if not isinstance(row, dict) or row.get("schema") != SPOOL_SCHEMA:
            raise BridgeError("spool_schema_mismatch", f"spool line {number} is not a {SPOOL_SCHEMA} row")
        if row.get("kind") not in SPOOL_KINDS:
            raise BridgeError("spool_kind_unknown", f"spool line {number} has kind {row.get('kind')!r}")
        n = row.get("n")
        if isinstance(n, bool) or not isinstance(n, int):
            raise BridgeError("spool_row_malformed", f"spool line {number} has no integer n")
        if break_at is not None:
            continue                                        # after a break nothing is trusted
        if rows and n == rows[-1]["n"]:
            if row != rows[-1]:
                raise BridgeError("spool_row_conflict", f"spool rows with n={n} disagree: a duplicate must be identical")
            duplicates += 1
            continue
        if n != len(rows) + 1:
            break_at, break_reason = len(rows) + 1, f"expected n={len(rows) + 1}, found n={n}"
            continue
        rows.append(row)
    return SpoolRead(rows, torn, hashlib.sha256(raw).hexdigest(), duplicates, break_at, break_reason)


def _t(row: Mapping[str, Any], key: str = "t_utc") -> datetime:
    try:
        return parse_utc(row[key], f"spool n={row.get('n')} {key}")
    except (KeyError, CoverageEvidenceError) as exc:
        raise BridgeError("spool_timestamp_invalid", f"spool row n={row.get('n')} has no valid {key}") from exc


def _need(row: Mapping[str, Any], *keys: str) -> None:
    for key in keys:
        if key not in row or row[key] is None:
            raise BridgeError("spool_field_missing", f"spool row n={row.get('n')} ({row.get('kind')}) lacks {key!r}")


# ------------------------------------------------------------------------------------------------------------------ ingest
def ingest_spool(spool_path: Path, journal_path: Path, *, source_id: str, policy: RecorderHealthPolicy, channel_id: str | None = None,
                 resume: bool = False) -> dict[str, Any]:
    """Replay one round's spool into a collector-health journal and return an ingest report (never raises for evidence problems).

    Raises ``BridgeError`` only when nothing trustworthy can be written at all: identity or policy mismatch (checked BEFORE any file is
    created), an existing journal that is not being resumed, or a journal write that fails.
    """
    spool = read_spool(spool_path)
    problems: list[dict[str, Any]] = []
    rows = spool.rows
    if not rows:
        raise BridgeError("spool_empty", "the spool holds no complete row")
    first = rows[0]
    if first["kind"] != "start":
        raise BridgeError("spool_not_started", "the first spool row must be a start")
    for row in rows:                                         # identity: one source, one collector instance, one run, one worker
        for key, expected in (("source_id", source_id), ("collector_instance", first.get("collector_instance")),
                              ("run_id", first.get("run_id")), ("worker", first.get("worker"))):
            if row.get(key) != expected:
                raise BridgeError("source_identity_mismatch" if key == "source_id" else "spool_identity_mismatch",
                                  f"spool row n={row['n']} has {key}={row.get(key)!r}, expected {expected!r}")
    _need(first, "observation_start_utc", "coverage_scope", "evidence_origin", "collector_id", "run_id", "collector_instance", "declared_policy")
    if first["coverage_scope"] != COVERAGE_SCOPE_RECORDER:
        raise BridgeError("coverage_scope_invalid", f"a recorder spool must declare coverage_scope={COVERAGE_SCOPE_RECORDER!r}")
    if first["evidence_origin"] not in EVIDENCE_ORIGINS:
        raise BridgeError("evidence_origin_invalid", f"evidence_origin must be one of {EVIDENCE_ORIGINS}")
    declared = first["declared_policy"].get("heartbeat_interval_ms") if isinstance(first["declared_policy"], dict) else None
    if declared != policy.heartbeat_interval_s * 1000:
        raise BridgeError("policy_declared_mismatch", f"the recorder declared a {declared} ms heartbeat interval, the policy says "
                                                      f"{policy.heartbeat_interval_s * 1000} ms")
    clock = _RowClock()
    try:
        journal = RecorderHealthJournal(Path(journal_path), source_id=source_id, collector_id=first["collector_id"], run_id=first["run_id"],
                                        channel_id=channel_id, clock=clock, resume=resume, collector_instance=first["collector_instance"])
    except FileExistsError as exc:
        raise BridgeError("journal_exists", f"{journal_path} already exists; evidence is never overwritten (use resume for a later round)") from exc
    except CoverageEvidenceError as exc:
        raise BridgeError(exc.code, str(exc)) from exc
    ingested, last_t, last_mono = 0, None, None
    last_wm = None
    open_error = False
    has_stop, stop_reason = False, None
    tolerance = policy.clock_tolerance_ms
    try:
        for row in rows:
            t = _t(row)
            clock.now = t
            mono = row.get("mono_ms")
            if last_t is not None and isinstance(mono, (int, float)) and not isinstance(mono, bool) and last_mono is not None \
                    and row["kind"] != "start":
                wall_delta = (t - last_t).total_seconds() * 1000.0
                if abs(wall_delta - (mono - last_mono)) > tolerance and not open_error and last_wm is not None:
                    journal.with_extra({"spool_n": row["n"]}).error(
                        "clock_discontinuity", f"wall delta {wall_delta:.1f} ms vs monotonic delta {mono - last_mono:.1f} ms", affects_from_utc=last_wm)
                    open_error = True
                    problems.append({"code": "clock_discontinuity", "spool_n": row["n"]})
            last_t, last_mono = t, mono if isinstance(mono, (int, float)) and not isinstance(mono, bool) else None
            kind = row["kind"]
            if kind == "start":
                extra = {k: row.get(k) for k in RECORDER_START_EXTRAS}
                journal.with_extra({**extra, "spool_n": row["n"]}).start(
                    observation_start_utc=row["observation_start_utc"], timestamp_provenance=str(row.get("timestamp_provenance") or "recorder_wall_clock_utc"))
                last_wm = parse_utc(row["observation_start_utc"], "observation_start_utc")
            elif kind == "heartbeat":
                _need(row, "observed_through_utc", "events_received", "health", "errors_since_last")
                if row.get("phase") not in (None, *HEARTBEAT_PHASES):
                    raise BridgeError("spool_field_invalid", f"spool row n={row['n']} has phase {row.get('phase')!r}")
                extra = {k: row.get(k) for k in RECORDER_BEAT_EXTRAS}
                journal.with_extra({**extra, "spool_n": row["n"]}).heartbeat(
                    row["observed_through_utc"], events_received=row["events_received"], last_event_seq=row.get("last_event_seq"),
                    health=row["health"], errors_since_last=row["errors_since_last"])
                last_wm = parse_utc(row["observed_through_utc"], "observed_through_utc")
            elif kind == "error":
                _need(row, "error_code", "message")
                if open_error:
                    problems.append({"code": "error_already_open_row_not_journalled", "spool_n": row["n"], "error_code": row["error_code"]})
                else:
                    journal.with_extra({"spool_n": row["n"]}).error(str(row["error_code"]), str(row["message"]),
                                                                     affects_from_utc=row.get("affects_from_utc"))
                    open_error = True
            elif kind == "recovered":
                _need(row, "coverage_resumes_at_utc")
                journal.with_extra({"spool_n": row["n"]}).recovered(row["coverage_resumes_at_utc"])
                open_error = False
            else:                                            # stop
                _need(row, "observed_through_utc", "events_received", "reason")
                extra = {k: row.get(k) for k in RECORDER_BEAT_EXTRAS}
                journal.with_extra({**extra, "spool_n": row["n"]}).stop(
                    row["observed_through_utc"], events_received=row["events_received"], last_event_seq=row.get("last_event_seq"),
                    reason=row["reason"])
                has_stop, stop_reason = True, row["reason"]
            ingested += 1
    except CoverageEvidenceError as exc:                      # the journal refused a row: the evidence is inconsistent, so ingestion ends here
        problems.append({"code": exc.code, "message": str(exc), "spool_n": rows[ingested]["n"] if ingested < len(rows) else None})
    except OSError as exc:
        raise BridgeError("journal_write_failed", f"the journal could not be written after {ingested} row(s): {exc}") from exc
    if spool.torn_tail:
        problems.append({"code": "spool_torn_tail", "message": "an interrupted final write was dropped, not repaired"})
    if spool.break_at is not None:
        problems.append({"code": "spool_sequence_gap", "message": spool.break_reason, "next_expected_n": spool.break_at})
    if not has_stop:
        problems.append({"code": "no_stop_row", "message": "no stop was written; no stop is synthesised, so time after the last watermark stays uncovered"})
    events, torn, digest = read_journal_file(Path(journal_path))
    return {"schema": BRIDGE_REPORT_SCHEMA, "spool_path": str(spool_path), "spool_sha256": spool.sha256, "journal_path": str(journal_path),
            "journal_sha256": digest, "journal_records": len(events), "journal_torn_tail": torn, "collector_instance": first["collector_instance"],
            "source_id": source_id, "run_id": first["run_id"], "worker": first.get("worker"), "round": first.get("round"),
            "coverage_scope": first["coverage_scope"], "evidence_origin": first["evidence_origin"], "policy": policy.as_dict(),
            "spool_rows_read": len(rows), "spool_rows_ingested": ingested, "spool_duplicates_dropped": spool.duplicates_dropped,
            "has_stop": has_stop, "stop_reason": stop_reason, "problems": problems, "complete": not problems,
            "note": ("operational coverage of the recorder only; source completeness is a separate, independent piece of evidence"
                     if not problems else "the evidence is incomplete or inconsistent; see problems")}


def canonical_report_digest(report: Mapping[str, Any]) -> str:
    return sha256_text(json.dumps(report, sort_keys=True, separators=(",", ":")))
