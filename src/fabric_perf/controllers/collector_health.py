"""Collector-health journal writer (task A0i, 2026-11-21): the smallest offline-testable interface that lets a FUTURE authorized experiment
produce the evidence ``workload_coverage`` needs.

What exists today, and what does not (VERIFIED by inspection, A0i)
-------------------------------------------------------------------
* The Flashbots archives were collected by a third party; their ``sources`` column is empty and they carry no heartbeat, lifecycle or
  sequence record. No coverage can ever be certified for them.
* ``ledger_evidence.LedgerCollector`` has a durable, resumable observation journal with contiguous sequence numbers, but it records
  COMMITTED LEDGER blocks, not workload arrivals, and has no heartbeat. It is not reused as arrival-coverage evidence.
* The Caliper client recorder (``recorder.csv``, ``submits.csv``) has a submit sequence number but writes no start, heartbeat, error or
  stop record. A recorder that crashes leaves no mark.

So no running collector currently emits collector-health evidence. THIS MODULE IS NOT WIRED INTO ANY RECORDER, and it must not be
described as doing so: wiring it into the arrival recorder is future, separately authorized instrumentation (see the A0i report,
section 7). Until then every live interval is certified by nothing, and ``VerifiedCoverage`` fails closed.

Contract
--------
A journal is a NEW file (never overwritten), one JSON object per line, flushed and fsynced per row, hash-chained (each row carries the
digest of the previous one) so an altered, dropped or reordered row is detected. A collector must write ``start`` before anything else,
``heartbeat`` rows at least every ``CoveragePolicy.max_heartbeat_gap_s`` seconds, ``error`` when it loses events, ``recovered`` when it
can capture again, and ``stop`` when it ends cleanly. A process that dies writes none of these: the missing ``stop`` and the next
``start`` (a new instance in the same journal, opened with ``resume=True``) are the restart boundary, and the time in between is
NOT covered. Nothing here repairs, back-fills or retroactively certifies an interval.

This module imports no network, process, model or Fabric API. It writes only the journal file it is given.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .channel_selection import select_experiment_channel
from .workload_coverage import (COVERAGE_SCOPES, GENESIS_SHA256, JOURNAL_EVENT_SCHEMA, KIND_ERROR, KIND_HEARTBEAT, KIND_RECOVERED, KIND_START,
                                KIND_STOP, CoverageEvidenceError, canonical, event_digest, floor_second, iso, parse_utc, read_journal_file,
                                whole_second)

Clock = Callable[[], datetime]


def _system_clock() -> datetime:
    return datetime.now(timezone.utc)


class CollectorHealthJournal:
    """One collector instance's append-only health journal.

    ``clock`` is injected (tests pass a scripted one). Every public write raises ``CoverageEvidenceError`` on misuse (a heartbeat before
    ``start``, a second ``start`` without a restart, a write after ``stop``), and a refused write appends nothing.
    """

    def __init__(self, path: Path, *, source_id: str, collector_id: str, run_id: str, channel_id: str | None = None,
                 dataset_id: str | None = None, clock: Clock | None = None, resume: bool = False,
                 collector_instance: str | None = None):
        for name, value in (("source_id", source_id), ("collector_id", collector_id), ("run_id", run_id)):
            if not isinstance(value, str) or not value.strip():
                raise CoverageEvidenceError("identity_missing", f"{name} must be a non-empty string")
        if channel_id is not None:           # a protected reference channel or an earlier experiment's channel is refused before any file is created
            channel_id = select_experiment_channel(channel_id, purpose="collector-health journaling", option="channel_id")
        self.path = Path(path)
        self.source_id, self.collector_id, self.run_id = source_id, collector_id, run_id
        self.channel_id, self.dataset_id = channel_id, dataset_id
        self.instance = collector_instance or f"{collector_id}:{uuid.uuid4().hex[:12]}"
        self._clock = clock or _system_clock
        self._seq, self._prev = 0, GENESIS_SHA256
        self._started = self._stopped = False
        self._open_error = False
        self._last_watermark: datetime | None = None
        if resume:
            events, torn, _ = read_journal_file(self.path)
            if torn:
                raise CoverageEvidenceError("journal_torn_tail", "an interrupted final write must be reviewed before the journal is resumed")
            if not events:
                raise CoverageEvidenceError("journal_empty", "cannot resume an empty journal")
            self._seq, self._prev = events[-1]["seq"], events[-1]["sha256"]
            for index, ev in enumerate(events, 1):                  # refuse to extend a journal that does not verify
                if ev.get("seq") != index or ev.get("sha256") != event_digest(ev, events[index - 2]["sha256"] if index > 1 else GENESIS_SHA256):
                    raise CoverageEvidenceError("journal_not_verified", f"journal record {index} fails verification; it is not extended")
            self._resumed = True
        else:
            self._resumed = False
            with self.path.open("xb"):                              # a new file only; evidence is never overwritten
                pass

    # ------------------------------------------------------------------------------------------------ writing
    def _append(self, kind: str, **fields: Any) -> dict[str, Any]:
        now = self._clock()
        if now.tzinfo is None:
            raise CoverageEvidenceError("timestamp_naive", "the collector clock must return an aware UTC time")
        body = {"schema": JOURNAL_EVENT_SCHEMA, "seq": self._seq + 1, "kind": kind, "t_utc": iso(now), "source_id": self.source_id,
                "collector_id": self.collector_id, "collector_instance": self.instance, "run_id": self.run_id,
                "channel_id": self.channel_id, "dataset_id": self.dataset_id, **fields}
        body["prev"] = self._prev
        body["sha256"] = event_digest(body, self._prev)
        line = (canonical(body) + "\n").encode("utf-8")
        with self.path.open("ab") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())                               # a crash leaves the row or none of it (at worst a torn tail)
        self._seq, self._prev = body["seq"], body["sha256"]
        return body

    def _require_running(self, kind: str) -> None:
        if not self._started:
            raise CoverageEvidenceError("collector_not_started", f"{kind} before start")
        if self._stopped:
            raise CoverageEvidenceError("collector_stopped", f"{kind} after stop")

    def start(self, *, observation_start_utc: Any | None = None, timestamp_provenance: str = "collector_wall_clock_utc",
              first_event_seq: int | None = None, coverage_scope: str | None = None) -> dict[str, Any]:
        """Declare that this instance begins capturing. ``observation_start_utc`` defaults to the next whole second after now and may not be
        earlier than the moment of writing (no retroactive claim).

        ``coverage_scope`` (A0k) states what this collector's heartbeats can establish: ``source_stream_observer`` (the collector itself receives
        the source stream) or ``recorder_callback`` (an observer of a generator's callbacks; zeros then need an independent source count). Left
        out, the scope is undeclared and no heartbeat certifies a zero second."""
        if self._started:
            raise CoverageEvidenceError("collector_already_started", "start was already written by this instance")
        if coverage_scope is not None and coverage_scope not in COVERAGE_SCOPES:
            raise CoverageEvidenceError("coverage_scope_invalid", f"coverage_scope {coverage_scope!r} is not one of {COVERAGE_SCOPES}")
        now = self._clock()
        if observation_start_utc is None:
            observation_start_utc = floor_second(now) + timedelta(seconds=1)       # the next whole second: never earlier than the write
        start = whole_second(observation_start_utc, "observation_start_utc")
        if start < parse_utc(now, "clock"):
            raise CoverageEvidenceError("retroactive_coverage_claim", "observation cannot start before the start record is written")
        scope_field = {} if coverage_scope is None else {"coverage_scope": coverage_scope}
        row = self._append(KIND_START, observation_start_utc=iso(start), timestamp_provenance=timestamp_provenance,
                           first_event_seq=first_event_seq, **scope_field)
        self._started = True
        self._last_watermark = start
        return row

    def heartbeat(self, observed_through_utc: Any, *, events_received: int, last_event_seq: int | None = None, health: str = "healthy",
                  errors_since_last: int = 0) -> dict[str, Any]:
        """Report capture progress. ``observed_through_utc`` is the whole second up to which capture was CONTINUOUS (the watermark)."""
        self._require_running("heartbeat")
        through = whole_second(observed_through_utc, "observed_through_utc")
        return self._append(KIND_HEARTBEAT, observed_through_utc=iso(through), events_received=events_received,
                            last_event_seq=last_event_seq, health=health, errors_since_last=errors_since_last)

    def error(self, code: str, message: str, *, affects_from_utc: Any | None = None) -> dict[str, Any]:
        """Record that events may have been lost. Coverage from ``affects_from_utc`` (default: the last watermark) stays uncovered until
        ``recovered``."""
        self._require_running("error")
        if self._open_error:
            raise CoverageEvidenceError("error_while_errored", "an error is already open; write recovered first")
        fields: dict[str, Any] = {"error_code": code, "message": message}
        if affects_from_utc is not None:
            fields["affects_from_utc"] = iso(whole_second(affects_from_utc, "affects_from_utc"))
        row = self._append(KIND_ERROR, **fields)
        self._open_error = True
        return row

    def recovered(self, coverage_resumes_at_utc: Any) -> dict[str, Any]:
        self._require_running("recovered")
        if not self._open_error:
            raise CoverageEvidenceError("recovery_without_error", "no error is open")
        row = self._append(KIND_RECOVERED, coverage_resumes_at_utc=iso(whole_second(coverage_resumes_at_utc, "coverage_resumes_at_utc")))
        self._open_error = False
        return row

    def stop(self, observed_through_utc: Any, *, events_received: int, last_event_seq: int | None = None, reason: str = "clean") -> dict[str, Any]:
        """End collection. After this the instance writes nothing more; time after the watermark is not covered."""
        self._require_running("stop")
        row = self._append(KIND_STOP, observed_through_utc=iso(whole_second(observed_through_utc, "observed_through_utc")),
                           events_received=events_received, last_event_seq=last_event_seq, reason=reason)
        self._stopped = True
        return row
