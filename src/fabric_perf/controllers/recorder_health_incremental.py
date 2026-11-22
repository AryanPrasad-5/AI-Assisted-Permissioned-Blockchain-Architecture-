"""Incremental, restart-safe recorder-health bridge with explicit ingestion stamps (task A0p, 2026-11-22). OFFLINE ONLY: it reads a spool file and appends to files it owns.

Relationship to the whole-file bridge
-------------------------------------
``recorder_health_bridge.ingest_spool`` replays a FINISHED spool through ``CollectorHealthJournal`` and is unchanged: it stays the historical route and is never selected by this
module. ``IncrementalRecorderBridge`` is a separate, explicitly selected route that consumes the same ``recorder_health_spool/1`` rows a few at a time and produces the SAME journal
(row for row and byte for byte: a test replays one spool both ways). It reuses ``CollectorHealthJournal`` / ``RecorderHealthJournal`` for the state machine and the one hash-chain
format; it writes no second chain format into the journal.

Three files, three jobs (nothing in the journal depends on when the bridge ran)
----------------------------------------------------------------------------------
* the JOURNAL (``collector_health_event/1``) keeps the source's own time stamps (``t_utc`` is the spool row's time). No ingestion time enters it, so replaying the same spool always
  gives the same journal and a restart cannot change a row;
* the INGEST LEDGER (``recorder_health_ingest/1``, hash-chained) records, per spool row, WHEN the bridge handled it: ``read_*`` (the poll that read the bytes), ``journal_append_start_*``
  and ``journal_append_return_*`` (the journal append, which flushes and calls ``os.fsync`` before returning), each as an aware UTC wall time and a monotonic microsecond count, plus the
  source's own ``spool_t_utc``. Source event time, spool write time, bridge read time and bridge append time are separate fields and are never merged;
* the CHECKPOINT (``recorder_health_bridge_checkpoint/1``, atomically replaced) is an ADVISORY resume hint. It is never trusted over the journal.

What "durable" means here: ``journal_append_return`` is the moment ``CollectorHealthJournal._append`` returned, i.e. write, flush and ``os.fsync`` all completed without an exception. It
is the operating system's acknowledgement, not an absolute guarantee against every storage failure, and the module never claims more. If the sync raised, the row is recorded as
``not_confirmed``.

Clock domains. The spool's ``t_utc`` comes from the recorder's host clock; the bridge stamps come from the bridge host. The operator DECLARES both domain labels. Equal labels are an
operator declaration, not a measurement; the timing sampler differences the two only when the labels are equal or a clock offset interval is supplied, and otherwise reports the
quantity as not measurable. Only differences between two monotonic stamps of the bridge process are formed here without such a declaration.

Restart and crash consistency
-----------------------------
Per spool row the bridge does: read -> validate -> journal append(s) (fsynced) -> ledger append -> (end of poll) ledger fsync -> checkpoint. ``resume`` does not trust the checkpoint:
it re-verifies the journal chain, re-derives (in memory) the journal rows the spool deterministically produces, and requires the existing journal rows to be an exact prefix of them
(``journal_diverges_from_spool`` otherwise). From that it learns, from the data itself, which spool rows were consumed, whether a spool row's journal unit was only half written, and
what is missing from the ledger and the checkpoint:

* crash before a read, or after a read but before any append: nothing was written; the next poll re-reads the same rows. No duplicate, no skip.
* crash after the journal append but before the ledger append: the journal row exists, its ingest time is UNKNOWN. A ``recovery`` ledger row says so; the unknown time is never
  filled with the recovery time.
* crash after the ledger append but before the checkpoint: the checkpoint is stale; it is rebuilt from the journal.
* crash during a journal append: a torn final journal line is removed ONLY when it is provably a prefix of the exact line the bridge would write next (so no evidence is lost); any
  other torn tail is refused (``journal_torn_tail_not_provable``). ``torn_tail_policy="refuse"`` refuses every torn tail.
* a spool or journal that was replaced, truncated or that belongs to another run, source, instance or worker fails closed with a machine-readable code.
Nothing is back-filled and nothing certifies coverage the spool does not show: a recovery never writes a ``recovered`` or ``stop`` row the recorder did not write.

Failures are sticky: after one, ``poll`` returns the error and writes nothing.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from .channel_selection import select_experiment_channel
from .collector_health import CollectorHealthJournal
from .recorder_health_bridge import (BRIDGE_REPORT_SCHEMA, COVERAGE_SCOPE_RECORDER, CORE_ROW_KEYS, EVIDENCE_ORIGINS, HEARTBEAT_PHASES, RECORDER_BEAT_EXTRAS,
                                     RECORDER_START_EXTRAS, SPOOL_KINDS, SPOOL_SCHEMA, BridgeError, RecorderHealthPolicy, _need, _t)
from .workload_coverage import (GENESIS_SHA256, JOURNAL_EVENT_SCHEMA, CoverageEvidenceError, canonical, event_digest, iso, parse_utc, read_journal_file)

INCREMENTAL_VERSION = "recorder_health_incremental 0.1 (A0p, 2026-11-22)"
INGEST_SCHEMA = "recorder_health_ingest/1"
CHECKPOINT_SCHEMA = "recorder_health_bridge_checkpoint/1"
REPORT_SCHEMA = "recorder_health_incremental_report/1"
LEDGER_KINDS = ("ledger_start", "ingest", "recovery", "ledger_stop")
TORN_POLICIES = ("refuse", "truncate_if_provable_prefix")
TAIL_CHECK_BYTES = 4096
APPEND_SEMANTICS = ("CollectorHealthJournal._append returned: the row was written, flushed and os.fsync returned without an exception; the operating system accepted the flush "
                    "request; not a guarantee against every storage failure")

Clock = Callable[[], datetime]


def _wall_now() -> datetime:
    return datetime.now(timezone.utc)


def _mono_now_us() -> int:
    return time.monotonic_ns() // 1000


def iso_us(moment: datetime) -> str:
    """An aware datetime as UTC text with microseconds. A naive datetime is refused: a time zone is never assumed."""
    if not isinstance(moment, datetime) or moment.tzinfo is None:
        raise BridgeError("timestamp_naive", "a bridge clock must return an aware UTC time")
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass(frozen=True)
class BridgeConfig:
    """Everything the bridge insists on. No default is applied to an identity or a clock-domain label."""

    spool_path: Path
    journal_path: Path
    ledger_path: Path
    checkpoint_path: Path
    source_id: str
    run_id: str
    policy: RecorderHealthPolicy
    spool_clock_domain: str
    bridge_clock_domain: str
    worker: Any = None                       # the recorder's worker identity as written in its spool rows; None = the first row's, then fixed
    collector_instance: str | None = None    # the expected recorder instance; None = the first row's, then fixed
    channel_id: str | None = None
    max_rows_per_poll: int = 500
    max_bytes_per_poll: int = 1 << 20
    max_line_bytes: int = 16_384
    torn_tail_policy: str = "truncate_if_provable_prefix"

    def __post_init__(self) -> None:
        for name in ("source_id", "run_id", "spool_clock_domain", "bridge_clock_domain"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise BridgeError("config_identity_missing", f"{name} must be a non-empty string")
        for name in ("max_rows_per_poll", "max_bytes_per_poll", "max_line_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise BridgeError("config_invalid", f"{name} must be a positive integer")
        if self.max_bytes_per_poll < self.max_line_bytes:
            raise BridgeError("config_invalid", "max_bytes_per_poll must be at least max_line_bytes")
        if self.torn_tail_policy not in TORN_POLICIES:
            raise BridgeError("config_invalid", f"torn_tail_policy must be one of {TORN_POLICIES}")
        if self.channel_id is not None:
            select_experiment_channel(self.channel_id, purpose="incremental recorder-health bridging", option="channel_id")
        for name in ("spool_path", "journal_path", "ledger_path", "checkpoint_path"):
            object.__setattr__(self, name, Path(getattr(self, name)))
        if len({str(getattr(self, n)) for n in ("spool_path", "journal_path", "ledger_path", "checkpoint_path")}) != 4:
            raise BridgeError("config_invalid", "spool, journal, ledger and checkpoint must be four different files")

    def binding(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "run_id": self.run_id, "worker": self.worker, "collector_instance": self.collector_instance, "channel_id": self.channel_id,
                "spool_name": self.spool_path.name, "journal_name": self.journal_path.name, "ledger_name": self.ledger_path.name,
                "spool_clock_domain": self.spool_clock_domain, "bridge_clock_domain": self.bridge_clock_domain, "policy": self.policy.as_dict(),
                "schemas": [SPOOL_SCHEMA, JOURNAL_EVENT_SCHEMA, INGEST_SCHEMA, CHECKPOINT_SCHEMA], "bridge_version": INCREMENTAL_VERSION}

    def binding_sha256(self) -> str:
        return hashlib.sha256(canonical(self.binding()).encode("utf-8")).hexdigest()


# ------------------------------------------------------------------------------------------------------------------ journal (the ONE journal, driven row by row)
class _RowJournal(CollectorHealthJournal):
    """``CollectorHealthJournal`` state machine with recorder-field pass-through (as ``RecorderHealthJournal``), driven without its constructor so one instance can be
    created fresh, restored mid-stream, or run as an in-memory shadow. ``_row`` builds a row exactly as the base class does; subclasses decide what to do with it."""

    def _init_state(self, *, path: Path, source_id: str, collector_id: str, run_id: str, channel_id: str | None, instance: str, clock: Clock) -> None:
        self.path, self.source_id, self.collector_id, self.run_id = Path(path), source_id, collector_id, run_id
        self.channel_id, self.dataset_id = channel_id, None
        self.instance, self._clock = instance, clock
        self._seq, self._prev = 0, GENESIS_SHA256
        self._started = self._stopped = self._open_error = self._resumed = False
        self._last_watermark = None
        self._pending_extra: dict[str, Any] = {}

    def with_extra(self, extra: Mapping[str, Any]) -> "_RowJournal":
        clash = CORE_ROW_KEYS & set(extra)
        if clash:
            raise BridgeError("extra_field_clash", f"recorder fields may not override journal fields: {sorted(clash)}")
        self._pending_extra = {k: v for k, v in extra.items() if v is not None}
        return self

    def _row(self, kind: str, **fields: Any) -> tuple[dict[str, Any], bytes]:
        extra, self._pending_extra = self._pending_extra, {}
        overlap = set(extra) & set(fields)
        if overlap:
            raise BridgeError("extra_field_clash", f"recorder fields repeat journal fields: {sorted(overlap)}")
        fields = {**fields, **extra}
        now = self._clock()
        if now.tzinfo is None:
            raise CoverageEvidenceError("timestamp_naive", "the collector clock must return an aware UTC time")
        body = {"schema": JOURNAL_EVENT_SCHEMA, "seq": self._seq + 1, "kind": kind, "t_utc": iso(now), "source_id": self.source_id,
                "collector_id": self.collector_id, "collector_instance": self.instance, "run_id": self.run_id,
                "channel_id": self.channel_id, "dataset_id": self.dataset_id, **fields}
        body["prev"] = self._prev
        body["sha256"] = event_digest(body, self._prev)
        return body, (canonical(body) + "\n").encode("utf-8")


class _ShadowJournal(_RowJournal):
    """The journal rows the spool deterministically produces, kept in memory. Nothing is written."""

    def __init__(self, **kw: Any) -> None:
        self._init_state(**kw)
        self.rows: list[dict[str, Any]] = []
        self.lines: list[bytes] = []

    def _append(self, kind: str, **fields: Any) -> dict[str, Any]:
        body, line = self._row(kind, **fields)
        self._seq, self._prev = body["seq"], body["sha256"]
        self.rows.append(body)
        self.lines.append(line)
        return body


class _FileJournal(_RowJournal):
    """The real journal file. ``_append`` is the base class's write, flush and fsync, with the bridge's hooks around it."""

    def __init__(self, bridge: "IncrementalRecorderBridge", **kw: Any) -> None:
        self._init_state(**kw)
        self._bridge = bridge

    def _append(self, kind: str, **fields: Any) -> dict[str, Any]:
        body, line = self._row(kind, **fields)
        self._bridge._journal_write(self, body, line)
        self._seq, self._prev = body["seq"], body["sha256"]
        return body


# ------------------------------------------------------------------------------------------------------------------ the spool row state machine
class _Machine:
    """Maps one validated spool row to journal operations. It is the body of ``recorder_health_bridge.ingest_spool``'s loop, one row at a time, with the loop's state kept in
    attributes so it can be re-derived from a replay (parity with ``ingest_spool`` is a tested property)."""

    def __init__(self, cfg: BridgeConfig) -> None:
        self.cfg = cfg
        self.first: dict[str, Any] | None = None
        self.last_t: datetime | None = None
        self.last_mono: Any = None
        self.last_wm: datetime | None = None
        self.open_error = False
        self.has_stop = False
        self.stop_reason: str | None = None
        self.rows = 0
        self.problems: list[dict[str, Any]] = []

    def check_identity(self, row: Mapping[str, Any]) -> None:
        if self.first is None:
            if row["kind"] != "start":
                raise BridgeError("spool_not_started", "the first spool row must be a start")
            _need(row, "observation_start_utc", "coverage_scope", "evidence_origin", "collector_id", "run_id", "collector_instance", "declared_policy")
            if row["coverage_scope"] != COVERAGE_SCOPE_RECORDER:
                raise BridgeError("coverage_scope_invalid", f"a recorder spool must declare coverage_scope={COVERAGE_SCOPE_RECORDER!r}")
            if row["evidence_origin"] not in EVIDENCE_ORIGINS:
                raise BridgeError("evidence_origin_invalid", f"evidence_origin must be one of {EVIDENCE_ORIGINS}")
            declared = row["declared_policy"].get("heartbeat_interval_ms") if isinstance(row["declared_policy"], dict) else None
            if declared != self.cfg.policy.heartbeat_interval_s * 1000:
                raise BridgeError("policy_declared_mismatch", f"the recorder declared a {declared} ms heartbeat interval, the policy says "
                                                              f"{self.cfg.policy.heartbeat_interval_s * 1000} ms")
            if row.get("run_id") != self.cfg.run_id:
                raise BridgeError("run_identity_mismatch", f"the spool is for run {row.get('run_id')!r}, the bridge for {self.cfg.run_id!r}")
            if self.cfg.collector_instance is not None and row["collector_instance"] != self.cfg.collector_instance:
                raise BridgeError("spool_identity_mismatch", f"the spool's recorder instance is {row['collector_instance']!r}, expected {self.cfg.collector_instance!r}")
            if self.cfg.worker is not None and row.get("worker") != self.cfg.worker:
                raise BridgeError("spool_identity_mismatch", f"the spool's worker is {row.get('worker')!r}, expected {self.cfg.worker!r}")
            self.first = dict(row)
        for key, expected in (("source_id", self.cfg.source_id), ("collector_instance", self.first["collector_instance"]), ("run_id", self.first["run_id"]),
                              ("worker", self.first.get("worker"))):
            if row.get(key) != expected:
                raise BridgeError("source_identity_mismatch" if key == "source_id" else "spool_identity_mismatch",
                                  f"spool row n={row['n']} has {key}={row.get(key)!r}, expected {expected!r}")

    def step(self, row: Mapping[str, Any], journal: _RowJournal, clock: "_RowClock") -> None:
        self.check_identity(row)
        t = _t(row)
        clock.now = t
        mono = row.get("mono_ms")
        mono_ok = isinstance(mono, (int, float)) and not isinstance(mono, bool)
        if self.last_t is not None and mono_ok and self.last_mono is not None and row["kind"] != "start":
            wall_delta = (t - self.last_t).total_seconds() * 1000.0
            if abs(wall_delta - (mono - self.last_mono)) > self.cfg.policy.clock_tolerance_ms and not self.open_error and self.last_wm is not None:
                journal.with_extra({"spool_n": row["n"]}).error(
                    "clock_discontinuity", f"wall delta {wall_delta:.1f} ms vs monotonic delta {mono - self.last_mono:.1f} ms", affects_from_utc=self.last_wm)
                self.open_error = True
                self.problems.append({"code": "clock_discontinuity", "spool_n": row["n"]})
        self.last_t, self.last_mono = t, mono if mono_ok else None
        kind = row["kind"]
        if kind == "start":
            extra = {k: row.get(k) for k in RECORDER_START_EXTRAS}
            journal.with_extra({**extra, "spool_n": row["n"]}).start(
                observation_start_utc=row["observation_start_utc"], timestamp_provenance=str(row.get("timestamp_provenance") or "recorder_wall_clock_utc"))
            self.last_wm = parse_utc(row["observation_start_utc"], "observation_start_utc")
        elif kind == "heartbeat":
            _need(row, "observed_through_utc", "events_received", "health", "errors_since_last")
            if row.get("phase") not in (None, *HEARTBEAT_PHASES):
                raise BridgeError("spool_field_invalid", f"spool row n={row['n']} has phase {row.get('phase')!r}")
            extra = {k: row.get(k) for k in RECORDER_BEAT_EXTRAS}
            journal.with_extra({**extra, "spool_n": row["n"]}).heartbeat(
                row["observed_through_utc"], events_received=row["events_received"], last_event_seq=row.get("last_event_seq"), health=row["health"],
                errors_since_last=row["errors_since_last"])
            self.last_wm = parse_utc(row["observed_through_utc"], "observed_through_utc")
        elif kind == "error":
            _need(row, "error_code", "message")
            if self.open_error:
                self.problems.append({"code": "error_already_open_row_not_journalled", "spool_n": row["n"], "error_code": row["error_code"]})
            else:
                journal.with_extra({"spool_n": row["n"]}).error(str(row["error_code"]), str(row["message"]), affects_from_utc=row.get("affects_from_utc"))
                self.open_error = True
        elif kind == "recovered":
            _need(row, "coverage_resumes_at_utc")
            journal.with_extra({"spool_n": row["n"]}).recovered(row["coverage_resumes_at_utc"])
            self.open_error = False
        else:
            _need(row, "observed_through_utc", "events_received", "reason")
            extra = {k: row.get(k) for k in RECORDER_BEAT_EXTRAS}
            journal.with_extra({**extra, "spool_n": row["n"]}).stop(
                row["observed_through_utc"], events_received=row["events_received"], last_event_seq=row.get("last_event_seq"), reason=row["reason"])
            self.has_stop, self.stop_reason = True, row["reason"]
        self.rows += 1


class _RowClock:
    def __init__(self) -> None:
        self.now: datetime | None = None

    def __call__(self) -> datetime:
        if self.now is None:
            raise BridgeError("clock_unset", "no row time has been set")
        return self.now


def _parse_spool_line(line: bytes, number: int, max_line: int) -> dict[str, Any]:
    if len(line) > max_line:
        raise BridgeError("spool_line_too_long", f"spool line {number} is {len(line)} bytes, over {max_line}")
    if not line.strip():
        raise BridgeError("spool_blank_line", f"spool line {number} is blank")
    try:
        row = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BridgeError("spool_row_malformed", f"spool line {number} is not UTF-8 JSON") from exc
    if not isinstance(row, dict) or row.get("schema") != SPOOL_SCHEMA:
        raise BridgeError("spool_schema_mismatch", f"spool line {number} is not a {SPOOL_SCHEMA} row")
    if row.get("kind") not in SPOOL_KINDS:
        raise BridgeError("spool_kind_unknown", f"spool line {number} has kind {row.get('kind')!r}")
    n = row.get("n")
    if isinstance(n, bool) or not isinstance(n, int):
        raise BridgeError("spool_row_malformed", f"spool line {number} has no integer n")
    return row


def _atomic_write(path: Path, data: bytes, fsync: Callable[[int], None]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as handle:
        handle.write(data)
        handle.flush()
        fsync(handle.fileno())
    os.replace(tmp, path)


def _file_id(path: Path) -> int | None:
    try:
        return getattr(path.stat(), "st_ino", 0) or None
    except OSError:
        return None


class IncrementalRecorderBridge:
    """Consumes a recorder health spool incrementally into the one collector-health journal, with an ingest ledger and a reconciled checkpoint. Use ``create`` for a new
    round and ``resume`` after a restart; the constructor is not public."""

    def __init__(self, cfg: BridgeConfig, *, wall_clock: Clock | None, mono_us: Callable[[], int] | None, fsync_fn: Callable[[int], None] | None,
                 fault: Callable[[str], None] | None, _token: object) -> None:
        if _token is not _CREATE_TOKEN:
            raise BridgeError("use_create_or_resume", "construct the bridge with IncrementalRecorderBridge.create or .resume")
        self.cfg = cfg
        self._wall, self._mono = wall_clock or _wall_now, mono_us or _mono_now_us
        self._fsync = fsync_fn or os.fsync
        self._fault = fault or (lambda point: None)
        self.error: BridgeError | None = None
        self._machine = _Machine(cfg)
        self._clock = _RowClock()
        self._journal: _FileJournal | None = None
        self._offset = 0
        self._last_n = 0
        self._sha = hashlib.sha256()
        self._tail = b""
        self._file_id: int | None = None
        self._journal_rows = 0
        self._journal_last = GENESIS_SHA256
        self._journal_bytes = 0
        self._lseq = 0
        self._lprev = GENESIS_SHA256
        self._ledger_bytes = 0
        self._polls = 0
        self._duplicates = 0
        self._partial_tail = 0
        self._torn_journal: bytes = b""            # an unterminated journal tail found at restart, not yet proven or removed
        self._pending_lines: list[tuple[bytes, dict[str, Any]]] = []
        self.recovery: list[dict[str, Any]] = []
        self.problems: list[dict[str, Any]] = []
        self._ended = False
        self._current: dict[str, Any] = {}
        self._last_row: dict[str, Any] = {}

    # ---------------------------------------------------------------------------------------------------------------- construction
    @classmethod
    def create(cls, cfg: BridgeConfig, *, wall_clock: Clock | None = None, mono_us: Callable[[], int] | None = None, fsync_fn: Callable[[int], None] | None = None,
               fault: Callable[[str], None] | None = None) -> "IncrementalRecorderBridge":
        """Start a NEW round: the journal, the ledger and the checkpoint must not exist (evidence is never overwritten). The journal file is created at the first spool row,
        because the recorder's own identity in its ``start`` row names the collector."""
        for path in (cfg.journal_path, cfg.ledger_path, cfg.checkpoint_path):
            if path.exists():
                raise BridgeError("output_exists", f"{path} already exists; evidence is never overwritten (use resume to continue a round)")
        self = cls(cfg, wall_clock=wall_clock, mono_us=mono_us, fsync_fn=fsync_fn, fault=fault, _token=_CREATE_TOKEN)
        with cfg.ledger_path.open("xb"):
            pass
        self._ledger_append("ledger_start", bridge_version=INCREMENTAL_VERSION, binding_sha256=cfg.binding_sha256(), spool_clock_domain=cfg.spool_clock_domain,
                            bridge_clock_domain=cfg.bridge_clock_domain, wall_clock_source="bridge host wall clock (aware UTC, microseconds)",
                            mono_clock_source="bridge process monotonic clock (microseconds, arbitrary origin)", append_semantics=APPEND_SEMANTICS, policy=cfg.policy.as_dict(),
                            spool_name=cfg.spool_path.name, journal_name=cfg.journal_path.name, evidence_note="a ledger row describes WHEN the bridge handled a spool row; "
                            "it is not source event time")
        self._ledger_sync()
        self._save_checkpoint()
        return self

    @classmethod
    def resume(cls, cfg: BridgeConfig, *, wall_clock: Clock | None = None, mono_us: Callable[[], int] | None = None, fsync_fn: Callable[[int], None] | None = None,
               fault: Callable[[str], None] | None = None) -> "IncrementalRecorderBridge":
        """Rebuild the bridge after a restart. Raises ``BridgeError`` when no trustworthy recovery point can be established; nothing is written in that case."""
        self = cls(cfg, wall_clock=wall_clock, mono_us=mono_us, fsync_fn=fsync_fn, fault=fault, _token=_CREATE_TOKEN)
        self._recover()
        return self

    # ---------------------------------------------------------------------------------------------------------------- ledger
    def _ledger_append(self, kind: str, **fields: Any) -> dict[str, Any]:
        body = {"schema": INGEST_SCHEMA, "lseq": self._lseq + 1, "kind": kind, "run_id": self.cfg.run_id, "source_id": self.cfg.source_id, **fields}
        body["prev"] = self._lprev
        body["sha256"] = event_digest(body, self._lprev)
        line = (canonical(body) + "\n").encode("utf-8")
        with self.cfg.ledger_path.open("ab") as handle:
            handle.write(line)
            handle.flush()
        self._lseq, self._lprev = body["lseq"], body["sha256"]
        self._ledger_bytes += len(line)
        return body

    def _ledger_sync(self) -> None:
        with self.cfg.ledger_path.open("ab") as handle:
            handle.flush()
            self._fsync(handle.fileno())

    # ---------------------------------------------------------------------------------------------------------------- journal
    def _journal_kwargs(self) -> dict[str, Any]:
        first = self._machine.first
        return {"path": self.cfg.journal_path, "source_id": self.cfg.source_id, "collector_id": first["collector_id"], "run_id": first["run_id"],
                "channel_id": self.cfg.channel_id, "instance": first["collector_instance"], "clock": self._clock}

    def _open_journal(self) -> _FileJournal:
        if self._journal is None:
            journal = _FileJournal(self, **self._journal_kwargs())
            if not self.cfg.journal_path.exists():
                with self.cfg.journal_path.open("xb"):
                    pass
            journal._seq, journal._prev = self._journal_rows, self._journal_last
            journal._started = self._machine.rows > 0 and self._machine.first is not None and self._journal_rows > 0
            journal._stopped, journal._open_error, journal._last_watermark = self._machine.has_stop, self._machine.open_error, self._machine.last_wm
            self._journal = journal
        return self._journal

    def _journal_write(self, journal: _FileJournal, body: dict[str, Any], line: bytes) -> None:
        """The base class's append: write, flush, ``os.fsync``. A torn tail left by an earlier crash is removed first, only if it is provably a prefix of ``line``."""
        self._settle_torn_journal(line)
        self._fault("journal_before_write")
        start_wall, start_mono = self._wall(), self._mono()
        confirmed = False
        try:
            with self.cfg.journal_path.open("ab") as handle:
                self._fault_write(handle, line)
                handle.flush()
                self._fsync(handle.fileno())
                confirmed = True
        finally:
            self._current.setdefault("appends", []).append({"seq": body["seq"], "sha256": body["sha256"], "start_wall": start_wall, "start_mono": start_mono,
                                                             "return_wall": self._wall() if confirmed else None, "return_mono": self._mono() if confirmed else None,
                                                             "confirmed": confirmed, "bytes": len(line)})
        self._journal_rows, self._journal_last = body["seq"], body["sha256"]
        self._journal_bytes += len(line)
        self._fault("journal_after_write")

    def _fault_write(self, handle: Any, line: bytes) -> None:
        """Write the line; a fault hook may interrupt after a prefix has been written (a torn write)."""
        try:
            self._fault("journal_mid_write")
        except _TornWrite as torn:
            handle.write(line[: max(1, min(len(line) - 1, torn.keep))])
            handle.flush()
            raise
        handle.write(line)

    def _settle_torn_journal(self, next_line: bytes) -> None:
        if not self._torn_journal:
            return
        if not next_line.startswith(self._torn_journal):
            raise BridgeError("journal_torn_tail_not_provable", "the journal ends with an unterminated fragment that is not a prefix of the next row the bridge would write; "
                                                                "it is not removed (review the journal)")
        size = self.cfg.journal_path.stat().st_size
        keep = size - len(self._torn_journal)
        with self.cfg.journal_path.open("r+b") as handle:
            handle.truncate(keep)
            handle.flush()
            self._fsync(handle.fileno())
        self.recovery.append({"action": "journal_torn_tail_removed", "bytes": len(self._torn_journal), "fragment_sha256": hashlib.sha256(self._torn_journal).hexdigest(),
                              "proof": "the fragment is a strict prefix of the exact row the bridge writes next (deterministic replay)"})
        self._torn_journal = b""

    # ---------------------------------------------------------------------------------------------------------------- checkpoint
    def _checkpoint_body(self) -> dict[str, Any]:
        first = self._machine.first or {}
        return {"schema": CHECKPOINT_SCHEMA, "bridge_version": INCREMENTAL_VERSION, "binding_sha256": self.cfg.binding_sha256(), "run_id": self.cfg.run_id,
                "source_id": self.cfg.source_id, "collector_instance": first.get("collector_instance"), "worker": first.get("worker"), "round": first.get("round"),
                "spool_name": self.cfg.spool_path.name, "spool_offset": self._offset, "spool_prefix_sha256": self._sha.hexdigest(), "spool_last_n": self._last_n,
                "spool_file_id": self._file_id, "spool_tail_hex": self._tail.hex(), "journal_name": self.cfg.journal_path.name, "journal_records": self._journal_rows,
                "journal_last_sha256": self._journal_last, "journal_bytes": self._journal_bytes, "ledger_name": self.cfg.ledger_path.name, "ledger_rows": self._lseq,
                "ledger_last_sha256": self._lprev, "ledger_bytes": self._ledger_bytes, "has_stop": self._machine.has_stop, "open_error": self._machine.open_error,
                "advisory": "a resume hint only: resume reconciles it with the journal and the spool and never trusts it over them"}

    def _save_checkpoint(self) -> None:
        body = self._checkpoint_body()
        doc = {**body, "self_sha256": hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()}
        _atomic_write(self.cfg.checkpoint_path, (canonical(doc) + "\n").encode("utf-8"), self._fsync)

    # ---------------------------------------------------------------------------------------------------------------- polling
    def _fail(self, exc: BridgeError) -> dict[str, Any]:
        if self.error is None:
            self.error = exc
        return self._status("error", 0)

    def _status(self, status: str, rows: int) -> dict[str, Any]:
        return {"status": status, "rows_ingested": rows, "spool_offset": self._offset, "spool_last_n": self._last_n, "partial_tail_bytes": self._partial_tail,
                "journal_records": self._journal_rows, "ledger_rows": self._lseq, "error": None if self.error is None else {"code": self.error.code, "message": str(self.error)}}

    def _check_spool_file(self) -> os.stat_result | None:
        path = self.cfg.spool_path
        try:
            info = path.stat()
        except FileNotFoundError:
            if self._offset or self._last_n:
                raise BridgeError("spool_missing_after_start", f"{path.name} existed and has been removed")
            return None
        fid = getattr(info, "st_ino", 0) or None
        if self._file_id is None:
            self._file_id = fid
        elif fid is not None and fid != self._file_id:
            raise BridgeError("spool_replaced", f"{path.name} is a different file from the one being read (file identity changed)")
        if info.st_size < self._offset:
            raise BridgeError("spool_truncated", f"{path.name} is {info.st_size} bytes, shorter than the {self._offset} already consumed")
        if self._tail:
            with path.open("rb") as stream:
                stream.seek(self._offset - len(self._tail))
                if stream.read(len(self._tail)) != self._tail:
                    raise BridgeError("spool_replaced", f"the bytes of {path.name} before offset {self._offset} no longer match what was read")
        return info

    def poll(self, max_rows: int | None = None) -> dict[str, Any]:
        """Consume the complete spool rows appended since the last poll: at most ``max_rows`` (default ``cfg.max_rows_per_poll``) rows and ``cfg.max_bytes_per_poll`` bytes.
        Returns a status dict; it never raises for an evidence problem (the failure is sticky and returned)."""
        if self.error is not None:
            return self._status("error", 0)
        limit = self.cfg.max_rows_per_poll if max_rows is None else max_rows
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            return self._fail(BridgeError("config_invalid", "max_rows must be a positive integer"))
        self._polls += 1
        try:
            self._fault("poll_start")
            return self._poll(limit)
        except BridgeError as exc:
            return self._fail(exc)
        except CoverageEvidenceError as exc:
            self.problems.append({"code": exc.code, "message": str(exc)})
            return self._fail(BridgeError(exc.code, str(exc)))
        except OSError as exc:
            return self._fail(BridgeError("journal_write_failed", f"a bridge file could not be written after {self._journal_rows} journal row(s): {exc}"))

    def _poll(self, limit: int) -> dict[str, Any]:
        if self._pending_lines:
            self._flush_pending()
        info = self._check_spool_file()
        if info is None:
            return self._status("spool_missing", 0)
        with self.cfg.spool_path.open("rb") as stream:
            stream.seek(self._offset)
            data = stream.read(self.cfg.max_bytes_per_poll)
        read_wall, read_mono = self._wall(), self._mono()
        self._fault("after_read")
        if not data:
            self._partial_tail = 0
            return self._status("no_new_data", 0)
        cut = data.rfind(b"\n")
        if cut < 0:
            if len(data) > self.cfg.max_line_bytes:
                raise BridgeError("spool_line_too_long", f"a row in {self.cfg.spool_path.name} exceeds {self.cfg.max_line_bytes} bytes without a newline")
            self._partial_tail = len(data)
            return self._status("partial_tail", 0)
        rows = 0
        for line in data[:cut].split(b"\n"):
            if rows >= limit:
                break
            row = _parse_spool_line(line, self._last_n + 1, self.cfg.max_line_bytes)
            n = row["n"]
            if self._last_n and n == self._last_n:
                prior = self._last_row
                if row != prior:
                    raise BridgeError("spool_row_conflict", f"spool rows with n={n} disagree: a duplicate must be identical")
                self._advance_spool(line)
                self._duplicates += 1
                self.problems.append({"code": "spool_duplicate_dropped", "spool_n": n})
                continue
            if n != self._last_n + 1:
                raise BridgeError("spool_sequence_gap" if n > self._last_n else "spool_row_conflict", f"expected n={self._last_n + 1}, found n={n}")
            self._ingest_row(row, line, read_wall, read_mono)
            rows += 1
        reached_end = len(data) < self.cfg.max_bytes_per_poll and rows < limit
        self._partial_tail = (len(data) - (cut + 1)) if reached_end else 0
        if self._partial_tail > self.cfg.max_line_bytes:
            raise BridgeError("spool_line_too_long", "an unterminated spool row already exceeds the line limit")
        if rows:
            self._ledger_sync()
            self._fault("before_checkpoint")
            self._save_checkpoint()
            self._fault("after_checkpoint")
        return self._status("progress", rows)

    def _advance_spool(self, line: bytes) -> None:
        self._offset += len(line) + 1
        self._sha.update(line + b"\n")
        self._tail = (self._tail + line + b"\n")[-TAIL_CHECK_BYTES:]

    def _ingest_row(self, row: dict[str, Any], line: bytes, read_wall: datetime, read_mono: int) -> None:
        machine = self._machine
        if machine.first is None:
            machine.check_identity(row)                                   # fixes the identity before a journal exists
        journal_before = self._journal_rows
        self._current = {"appends": []}
        try:
            machine.step(row, self._open_journal(), self._clock)
        finally:                                                       # on a refusal this instance is failed; a restart re-derives everything from the files
            appends = self._current.get("appends", [])
        self._fault("after_journal_append")
        self._advance_spool(line)
        self._last_n, self._last_row = row["n"], dict(row)
        first_seq = appends[0]["seq"] if appends else None
        last = appends[-1] if appends else None
        confirmed = bool(appends) and all(a["confirmed"] for a in appends)
        self._ledger_append(
            "ingest", spool_n=row["n"], spool_kind=row["kind"], spool_t_utc=row.get("t_utc"), spool_mono_ms=row.get("mono_ms"),
            journal_seq_first=first_seq, journal_seq_last=None if last is None else last["seq"], journal_last_sha256=None if last is None else last["sha256"],
            poll_index=self._polls, read_wall_utc=iso_us(read_wall), read_mono_us=read_mono,
            journal_append_start_wall_utc=None if not appends else iso_us(appends[0]["start_wall"]),
            journal_append_start_mono_us=None if not appends else appends[0]["start_mono"],
            journal_append_return_wall_utc=None if last is None or last["return_wall"] is None else iso_us(last["return_wall"]),
            journal_append_return_mono_us=None if last is None else last["return_mono"],
            journal_append_confirmation="os_fsync_returned" if confirmed else ("not_applicable_no_journal_row" if not appends else "not_confirmed"),
            journal_rows_appended=len(appends), spool_clock_domain=self.cfg.spool_clock_domain, bridge_clock_domain=self.cfg.bridge_clock_domain,
            completed_after_restart=bool(row.get("_completed_after_restart")))
        self._fault("after_ledger_append")

    def _flush_pending(self) -> None:
        """Append the journal rows a restart found missing from a half-written unit (they are exactly what the spool deterministically produces)."""
        pending, self._pending_lines = self._pending_lines, []
        journal = self._open_journal()
        recovery_wall, recovery_mono = self._wall(), self._mono()
        for line, body in pending:
            self._settle_torn_journal(line)
            start_wall, start_mono = self._wall(), self._mono()
            with self.cfg.journal_path.open("ab") as handle:
                handle.write(line)
                handle.flush()
                self._fsync(handle.fileno())
            self._journal_rows, self._journal_last = body["seq"], body["sha256"]
            self._journal_bytes += len(line)
            journal._seq, journal._prev = body["seq"], body["sha256"]
            self._ledger_append("ingest", spool_n=body.get("spool_n"), spool_kind=body["kind"], spool_t_utc=None, spool_mono_ms=None, journal_seq_first=body["seq"],
                                journal_seq_last=body["seq"], journal_last_sha256=body["sha256"], poll_index=self._polls, read_wall_utc=iso_us(recovery_wall),
                                read_mono_us=recovery_mono, journal_append_start_wall_utc=iso_us(start_wall), journal_append_start_mono_us=start_mono,
                                journal_append_return_wall_utc=iso_us(self._wall()), journal_append_return_mono_us=self._mono(),
                                journal_append_confirmation="os_fsync_returned", journal_rows_appended=1, spool_clock_domain=self.cfg.spool_clock_domain,
                                bridge_clock_domain=self.cfg.bridge_clock_domain, completed_after_restart=True)
            self.recovery.append({"action": "half_written_unit_completed", "journal_seq": body["seq"], "spool_n": body.get("spool_n")})
        self._ledger_sync()
        self._save_checkpoint()

    def poll_until_drained(self, max_polls: int = 10_000) -> dict[str, Any]:
        last: dict[str, Any] = {}
        for _ in range(max_polls):
            last = self.poll()
            if last["status"] != "progress":
                break
        return last

    # ---------------------------------------------------------------------------------------------------------------- recovery
    def _recover(self) -> None:
        cfg = self.cfg
        # 1. the checkpoint: readable and bound to this configuration (advisory afterwards)
        try:
            doc = json.loads(cfg.checkpoint_path.read_text(encoding="ascii"))
        except (OSError, ValueError) as exc:
            raise BridgeError("checkpoint_missing" if not cfg.checkpoint_path.exists() else "checkpoint_unreadable", f"{cfg.checkpoint_path.name} cannot be read: {exc}") from exc
        body = {k: v for k, v in doc.items() if k != "self_sha256"}
        if doc.get("self_sha256") != hashlib.sha256(canonical(body).encode("utf-8")).hexdigest():
            raise BridgeError("checkpoint_corrupt", f"{cfg.checkpoint_path.name} fails its own digest")
        if body.get("schema") != CHECKPOINT_SCHEMA:
            raise BridgeError("checkpoint_schema_mismatch", f"checkpoint schema is {body.get('schema')!r}; a mixed bridge version is refused")
        if body.get("bridge_version") != INCREMENTAL_VERSION:
            raise BridgeError("bridge_version_mismatch", f"the checkpoint was written by {body.get('bridge_version')!r}")
        if body.get("binding_sha256") != cfg.binding_sha256() or body.get("run_id") != cfg.run_id or body.get("source_id") != cfg.source_id:
            raise BridgeError("checkpoint_binding_mismatch", "the checkpoint belongs to another run, source, policy, clock-domain declaration or file set")
        # 2. the journal: chain-verified, one run, one source, one instance
        events, journal_bytes = self._read_journal()
        if len(events) < body["journal_records"]:
            raise BridgeError("journal_behind_checkpoint", f"the journal holds {len(events)} row(s), the checkpoint recorded {body['journal_records']}")
        if body["journal_records"] and events[body["journal_records"] - 1]["sha256"] != body["journal_last_sha256"]:
            raise BridgeError("journal_checkpoint_conflict", "the journal row at the checkpoint's position differs from the one the checkpoint recorded")
        # 3. the spool: re-derive the journal rows it deterministically produces and require the journal to be a prefix of them
        try:
            raw = cfg.spool_path.read_bytes()
        except OSError as exc:
            raise BridgeError("spool_unreadable", f"{cfg.spool_path}: {exc}") from exc
        if len(raw) < body["spool_offset"]:
            raise BridgeError("spool_truncated", f"{cfg.spool_path.name} is {len(raw)} bytes, shorter than the checkpoint's {body['spool_offset']}")
        if hashlib.sha256(raw[: body["spool_offset"]]).hexdigest() != body["spool_prefix_sha256"]:
            raise BridgeError("spool_replaced", "the spool prefix no longer matches the digest the checkpoint recorded")
        shadow, machine, consumed, offset, last_n, sha, last_row, at_ckpt = self._derive(raw, events, body)
        if not at_ckpt:
            raise BridgeError("spool_replaced", "the spool does not reach the checkpoint's offset by whole rows")
        if len(shadow.rows) < len(events):
            raise BridgeError("journal_ahead_of_spool", f"the journal holds {len(events)} row(s) but the spool produces only {len(shadow.rows)}")
        # 4. adopt the derived state; the journal's own bytes are the truth
        self._machine, self._offset, self._last_n, self._last_row = machine, offset, last_n, last_row
        self._sha = sha
        self._tail = raw[:offset][-TAIL_CHECK_BYTES:]
        self._file_id = _file_id(cfg.spool_path)
        self._journal_rows = len(events)
        self._journal_last = events[-1]["sha256"] if events else GENESIS_SHA256
        self._journal_bytes = journal_bytes
        self._clock.now = machine.last_t
        extra = shadow.lines[len(events):]
        self._pending_lines = [(line, shadow.rows[len(events) + i]) for i, line in enumerate(extra)]
        # 5. the ledger, reconciled with the journal
        self._recover_ledger(events)
        self.recovery.append({"action": "resumed", "spool_rows_replayed": consumed, "journal_rows": len(events), "pending_unit_rows": len(extra),
                              "checkpoint_was_stale": body["journal_records"] != len(events) or body["ledger_rows"] != self._lseq or body["spool_offset"] != offset})
        if self._pending_lines:
            self._flush_pending()
        else:
            self._save_checkpoint()

    def _read_journal(self) -> tuple[list[dict[str, Any]], int]:
        cfg = self.cfg
        if not cfg.journal_path.exists():
            return [], 0
        raw = cfg.journal_path.read_bytes()
        complete = raw[: raw.rfind(b"\n") + 1] if b"\n" in raw else b""
        torn = raw[len(complete):]
        events, torn_flag, _ = read_journal_file(cfg.journal_path)
        if bool(torn) != torn_flag:
            raise BridgeError("journal_unreadable", "the journal's tail could not be classified")
        prev = GENESIS_SHA256
        for index, ev in enumerate(events, 1):
            if ev.get("schema") != JOURNAL_EVENT_SCHEMA:
                raise BridgeError("journal_schema_mismatch", f"journal row {index} schema is {ev.get('schema')!r}; a mixed journal version is refused")
            if ev.get("seq") != index or ev.get("prev") != prev or ev.get("sha256") != event_digest(ev, prev):
                raise BridgeError("journal_not_verified", f"journal row {index} fails verification; the journal is not extended")
            if ev.get("run_id") != cfg.run_id or ev.get("source_id") != cfg.source_id:
                raise BridgeError("journal_identity_mismatch", f"journal row {index} is for run {ev.get('run_id')!r} / source {ev.get('source_id')!r}: cross-run contamination")
            if cfg.channel_id is not None and ev.get("channel_id") != cfg.channel_id:
                raise BridgeError("journal_identity_mismatch", f"journal row {index} is for channel {ev.get('channel_id')!r}")
            prev = ev["sha256"]
        if events and len({ev["collector_instance"] for ev in events}) != 1:
            raise BridgeError("journal_identity_mismatch", "the journal mixes recorder instances")
        if torn:
            if cfg.torn_tail_policy == "refuse":
                raise BridgeError("journal_torn_tail_unreviewed", "the journal ends with an unterminated fragment and torn_tail_policy is 'refuse'")
            self._torn_journal = torn
        return events, len(complete)

    def _derive(self, raw: bytes, events: list[dict[str, Any]], ckpt: Mapping[str, Any]):
        cfg = self.cfg
        machine = _Machine(cfg)
        shadow: _ShadowJournal | None = None
        clock = _RowClock()
        offset = last_n = consumed = 0
        sha = hashlib.sha256()
        last_row: dict[str, Any] = {}
        at_ckpt = ckpt["spool_offset"] == 0
        ckpt_sha = None
        produced = 0
        lines = raw.split(b"\n")[:-1]
        for number, line in enumerate(lines, 1):
            if produced >= len(events) and offset >= ckpt["spool_offset"]:
                break                                              # the journal is exhausted and the checkpoint's prefix has been verified
            row = _parse_spool_line(line, number, cfg.max_line_bytes)
            n = row["n"]
            if last_n and n == last_n:
                if row != last_row:
                    raise BridgeError("spool_row_conflict", f"spool rows with n={n} disagree: a duplicate must be identical")
            else:
                if n != last_n + 1:
                    raise BridgeError("spool_sequence_gap" if n > last_n else "spool_row_conflict", f"expected n={last_n + 1}, found n={n}")
                if machine.first is None:
                    machine.check_identity(row)
                    shadow = _ShadowJournal(path=cfg.journal_path, source_id=cfg.source_id, collector_id=machine.first["collector_id"], run_id=machine.first["run_id"],
                                            channel_id=cfg.channel_id, instance=machine.first["collector_instance"], clock=clock)
                machine.step(row, shadow, clock)
                last_n, last_row = n, dict(row)
                for index in range(produced, len(shadow.rows)):
                    if index < len(events) and shadow.rows[index] != events[index]:
                        raise BridgeError("journal_diverges_from_spool", f"journal row {index + 1} is not the row the spool produces (the journal or the spool was altered)")
                produced = len(shadow.rows)
                consumed += 1
            offset += len(line) + 1
            sha.update(line + b"\n")
            if offset == ckpt["spool_offset"]:
                at_ckpt, ckpt_sha = True, sha.hexdigest()
        if shadow is None:
            shadow = _ShadowJournal(path=cfg.journal_path, source_id=cfg.source_id, collector_id="", run_id=cfg.run_id, channel_id=cfg.channel_id, instance="", clock=clock)
        if ckpt_sha is not None and ckpt_sha != ckpt["spool_prefix_sha256"]:
            raise BridgeError("spool_replaced", "the spool prefix differs from the checkpoint's digest")
        return shadow, machine, consumed, offset, last_n, sha, last_row, at_ckpt

    def _recover_ledger(self, events: list[dict[str, Any]]) -> None:
        cfg = self.cfg
        try:
            raw = cfg.ledger_path.read_bytes()
        except OSError as exc:
            raise BridgeError("ledger_missing", f"{cfg.ledger_path}: {exc}") from exc
        complete = raw[: raw.rfind(b"\n") + 1] if b"\n" in raw else b""
        torn = raw[len(complete):]
        prev, rows, covered = GENESIS_SHA256, 0, 0
        for number, line in enumerate(complete.split(b"\n")[:-1], 1):
            try:
                row = json.loads(line.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BridgeError("ledger_row_malformed", f"ledger line {number} is not ASCII JSON") from exc
            if (not isinstance(row, dict) or canonical(row) != line.decode("ascii") or row.get("schema") != INGEST_SCHEMA or row.get("lseq") != number
                    or row.get("prev") != prev or row.get("sha256") != event_digest(row, prev) or row.get("kind") not in LEDGER_KINDS):
                raise BridgeError("ledger_not_verified", f"ledger row {number} fails verification")
            if row.get("run_id") != cfg.run_id or row.get("source_id") != cfg.source_id:
                raise BridgeError("ledger_identity_mismatch", f"ledger row {number} is for another run or source")
            if row["kind"] == "ledger_start" and number != 1 or (number == 1 and row["kind"] != "ledger_start"):
                raise BridgeError("ledger_not_verified", "the ledger must begin with exactly one ledger_start")
            if row["kind"] in ("ingest", "recovery") and row.get("journal_seq_last") is not None:
                last = row["journal_seq_last"]
                if last > len(events) or events[last - 1]["sha256"] != row.get("journal_last_sha256"):
                    raise BridgeError("ledger_ahead_of_journal", f"ledger row {number} names journal row {last}, which the journal does not hold or holds differently")
                covered = max(covered, last)
            prev, rows = row["sha256"], number
        if rows == 0:
            raise BridgeError("ledger_not_verified", "the ledger is empty")
        self._lseq, self._lprev, self._ledger_bytes = rows, prev, len(complete)
        if torn:
            digest = hashlib.sha256(torn).hexdigest()
            with cfg.ledger_path.open("r+b") as handle:
                handle.truncate(len(complete))
                handle.flush()
                self._fsync(handle.fileno())
            self.recovery.append({"action": "ledger_torn_tail_removed", "bytes": len(torn), "fragment_sha256": digest})
        missing = len(events) - covered
        if missing > 0:
            self._ledger_append("recovery", journal_seq_first=covered + 1, journal_seq_last=len(events), journal_last_sha256=events[-1]["sha256"],
                                reason="journal rows exist without an ingest stamp (a crash between the journal append and the ledger append, or a discarded torn ledger row)",
                                ingest_time="unknown", recovery_wall_utc=iso_us(self._wall()), recovery_mono_us=self._mono(),
                                note="the recovery time is NOT the ingest time of these rows and is not used as one")
            self._ledger_sync()
            self.recovery.append({"action": "ledger_recovery_row_written", "journal_rows_without_stamp": missing})

    # ---------------------------------------------------------------------------------------------------------------- finish and report
    def close(self, *, source_ended: bool = False) -> dict[str, Any]:
        """Finish. ``source_ended`` is the CALLER's statement that the recorder will write no more; without a ``stop`` row that is an interrupted collection (reported, never
        synthesised). Writes a ``ledger_stop`` row unless the bridge failed."""
        self._ended = bool(source_ended)
        if self.error is None:
            self._ledger_append("ledger_stop", journal_rows=self._journal_rows, spool_rows=self._machine.rows, has_stop=self._machine.has_stop, source_ended=self._ended)
            self._ledger_sync()
            self._save_checkpoint()
        return self.report()

    def report(self) -> dict[str, Any]:
        m = self._machine
        problems = list(m.problems) + list(self.problems)
        if self.error is not None:
            problems.append({"code": self.error.code, "message": str(self.error)})
        if self._partial_tail:
            problems.append({"code": "spool_partial_trailing_row", "message": f"{self._partial_tail} unterminated bytes follow the last complete row; not consumed, not repaired"})
        if self._torn_journal:
            problems.append({"code": "journal_torn_tail_pending", "message": "an unterminated journal fragment awaits the next row (provable prefix) or review"})
        if self._ended and not m.has_stop and self.error is None:
            problems.append({"code": "interrupted_collection", "message": "the source ended without a stop row; no stop is synthesised, so time after the last watermark stays uncovered"})
        elif not m.has_stop:
            problems.append({"code": "no_stop_row", "message": "no stop row has been seen"})
        events, torn, digest = read_journal_file(self.cfg.journal_path) if self.cfg.journal_path.exists() else ([], False, hashlib.sha256(b"").hexdigest())
        first = m.first or {}
        return {"schema": REPORT_SCHEMA, "compatible_with": BRIDGE_REPORT_SCHEMA, "bridge_version": INCREMENTAL_VERSION, "spool_path": str(self.cfg.spool_path),
                "journal_path": str(self.cfg.journal_path), "ledger_path": str(self.cfg.ledger_path), "journal_sha256": digest, "journal_records": len(events),
                "journal_torn_tail": torn, "collector_instance": first.get("collector_instance"), "source_id": self.cfg.source_id, "run_id": self.cfg.run_id,
                "worker": first.get("worker"), "round": first.get("round"), "coverage_scope": first.get("coverage_scope"), "evidence_origin": first.get("evidence_origin"),
                "spool_clock_domain": self.cfg.spool_clock_domain, "bridge_clock_domain": self.cfg.bridge_clock_domain, "policy": self.cfg.policy.as_dict(),
                "spool_rows_ingested": m.rows, "spool_last_n": self._last_n, "spool_duplicates_dropped": self._duplicates, "spool_offset": self._offset,
                "has_stop": m.has_stop, "stop_reason": m.stop_reason, "ledger_rows": self._lseq, "recovery_actions": list(self.recovery), "problems": problems,
                "complete": not problems, "ingest_time_semantics": APPEND_SEMANTICS,
                "note": ("operational coverage of the recorder only; source completeness is a separate, independent piece of evidence"
                         if not problems else "the evidence is incomplete or inconsistent; see problems")}


class _TornWrite(BaseException):
    """Raised by a fault hook to simulate a crash after part of a journal row was written. ``keep`` is how many bytes of the row reach the file."""

    def __init__(self, keep: int):
        super().__init__(f"torn write after {keep} bytes")
        self.keep = keep


_CREATE_TOKEN = object()
