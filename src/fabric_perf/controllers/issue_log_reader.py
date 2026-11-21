"""Incremental reader for the generator-side issuance log, and its source-count evidence (task A0l, 2026-11-21).

What this module is
-------------------
``fabric-testbed/caliper/workloads/e1_issue_log.js`` writes a hash-chained, append-only JSON-lines log from the workload generator
(``intent`` and ``attempt`` BEFORE ``sendRequests``, ``outcome`` after it, ``tick`` liveness rows, a ``stop`` row on a clean close).
``IssueLogTail`` reads such a file incrementally, strictly and with bounded memory per poll. It is a pure file reader: no process, network,
Caliper, Fabric, Docker, model or recorder file is used, and nothing in it imports a recorder module (``test_a0l_reader`` checks this).

What it can and cannot establish
--------------------------------
* ISSUANCE: the generator logged an ``attempt`` for transaction ``n`` at its own ``Date.now()`` (``generator_issue_time``). That is evidence of
  a decision to call ``sendRequests``; it is not proof the call ran, that Caliper emitted ``txSubmitted``, that Fabric accepted it, or that
  it committed. A transaction with an ``attempt`` and no ``outcome`` is UNRESOLVED and is reported as such, never as complete.
* GENERATOR OBSERVATION: a second is "observed" only between two consecutive liveness points (``start``, ``tick``, ``stop``) no further apart
  than ``max_tick_gap_ms``. The file existing, or being quiet, is not evidence the writer was alive. Silence after the last liveness point is
  UNOBSERVED, not zero. The read-time clock is never used to extend coverage.
* SCOPE: a fully observed generator interval supports a claim about the EXPERIMENTAL GENERATOR's issuance only. It says nothing about an
  external (Ethereum mempool) source, and it does not make any Fabric quantity (submitted, endorsed, committed) known.

Event time versus read time
---------------------------
Every record carries the generator's wall clock ``t_ms`` (event time). The reader stamps each record with the ``read_time`` of the poll that
consumed it (read time). Evidence handed to a coverage evaluation at decision time ``as_of`` contains only records whose read time is at or
before ``as_of``; a record the reader had not yet read does not exist for that decision, whatever its event time.

Integrity
---------
A record is accepted only if it is canonical JSON, has the next ``rseq``, chains to the previous record's digest, carries the declared run and
worker identity, and obeys the state machine (intent -> attempt -> outcome, contiguous ``n``, matching tick/stop counts, non-regressing wall and
monotonic clocks that agree within ``max_clock_skew_ms``). Any violation is a sticky ``IssueLogError``: the evidence is then INVALID as a whole
(fail closed), never partially trusted. A final line without a newline is a partial record: it is not consumed, and it is reported.

Restart and replacement
-----------------------
``save_checkpoint`` persists the durable offset, the chain head, the digest of the last bytes consumed and the read-time marks. ``resume``
re-verifies the log from byte 0 in bounded chunks (so a replaced or edited prefix cannot be resumed from), restores the original read times
from the marks, and refuses a log that is shorter than, or different from, what the checkpoint recorded.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import os
from array import array
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import source_completeness as sc
from .workload_coverage import GENESIS_SHA256, canonical, event_digest

ISSUE_LOG_SCHEMA = "e1_issue_log/1"
CHECKPOINT_SCHEMA = "e1_issue_log_checkpoint/1"
ISSUE_DERIVED_FROM = "workload_module_issue_log"
EVENT_TIME_SEMANTICS = "generator_issue_time"
TAIL_BYTES = 4096

K_START, K_INTENT, K_ATTEMPT, K_OUTCOME, K_TICK, K_STOP = "start", "intent", "attempt", "outcome", "tick", "stop"
RECORD_KINDS = (K_START, K_INTENT, K_ATTEMPT, K_OUTCOME, K_TICK, K_STOP)
RESULTS = ("returned", "rejected")

# poll statuses
S_MISSING, S_NO_NEW, S_PARTIAL, S_PROGRESS, S_ERROR = "log_missing", "no_new_data", "partial_tail", "progress", "error"
# writer states (what the LOG shows about the writer; never inferred from the file merely existing)
W_NOT_STARTED, W_ACTIVE, W_CLOSED_CLEAN, W_CLOSED_OTHER, W_ERROR = "not_started", "active_per_last_liveness_point", "closed_clean", "closed_other", "error"

RC_GENERATOR_UNOBSERVED = "generator_issuance_unobserved"
RC_UNRESOLVED = "generator_attempt_unresolved"


class IssueLogError(sc.SourceEvidenceError):
    """The issuance log is malformed, inconsistent, replaced or contradicts itself. A subclass of ``SourceEvidenceError``, so a coverage
    snapshot turns it into an INVALID attestation (the whole attestation, never a part)."""


def _fail(code: str, message: str) -> IssueLogError:
    return IssueLogError(code, message)


def utc_ms(moment: datetime, name: str = "read_time") -> int:
    """A timezone-aware datetime as epoch milliseconds (UTC). A naive datetime is refused: a timezone is never assumed."""
    if not isinstance(moment, datetime) or moment.tzinfo is None:
        raise _fail("timestamp_naive", f"{name} must be a timezone-aware datetime")
    return (moment.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)


def ms_to_utc(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=int(value))


def issue_log_descriptor(run_id: str, worker_id: str) -> dict[str, Any]:
    """The ``source_counter`` descriptor of the issue log: generator-side, interval resolution, issue-time semantics, declared independent of the
    recorder. The independence is a DECLARATION about data flow (the log is written by the generator path and read from a file); see the
    structural tests. ``source_completeness.classify_descriptor`` judges it."""
    return {"counter_id": f"issue-log:{run_id}:{worker_id}", "kind": sc.KIND_ISSUE_LOG, "independent_of_recorder": True, "derived_from": ISSUE_DERIVED_FROM,
            "timestamp_semantics": sc.SEMANTICS_ISSUE, "resolution": "interval"}


# ------------------------------------------------------------------------------------------------------------------ policy
@dataclass(frozen=True)
class IssueLogPolicy:
    """Identity the reader insists on, and the limits it applies. The numeric defaults are PLACEHOLDERS with no scientific justification."""

    run_id: str
    worker_id: str
    clock_domain: str
    expected_roster: tuple = ()
    max_tick_gap_ms: int = 5_000
    max_clock_skew_ms: int = 1_000
    max_bytes_per_poll: int = 1 << 20
    max_records_per_poll: int = 10_000
    max_line_bytes: int = 16_384
    max_unresolved: int = 10_000

    def __post_init__(self) -> None:
        for name in ("run_id", "worker_id", "clock_domain"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise _fail("policy_identity_missing", f"{name} must be a non-empty string")
        for name in ("max_tick_gap_ms", "max_clock_skew_ms", "max_bytes_per_poll", "max_records_per_poll", "max_line_bytes", "max_unresolved"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise _fail("policy_invalid", f"{name} must be a positive integer")
        if self.max_bytes_per_poll < self.max_line_bytes:
            raise _fail("policy_invalid", "max_bytes_per_poll must be at least max_line_bytes")
        roster = tuple(self.expected_roster) or (self.worker_id,)
        if self.worker_id not in roster:
            raise _fail("worker_roster_mismatch", f"the expected roster {roster} does not contain worker {self.worker_id!r}")
        object.__setattr__(self, "expected_roster", roster)

    def as_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "worker_id": self.worker_id, "clock_domain": self.clock_domain, "expected_roster": list(self.expected_roster),
                "max_tick_gap_ms": self.max_tick_gap_ms, "max_clock_skew_ms": self.max_clock_skew_ms, "max_bytes_per_poll": self.max_bytes_per_poll,
                "max_records_per_poll": self.max_records_per_poll, "max_line_bytes": self.max_line_bytes, "max_unresolved": self.max_unresolved,
                "basis": "operator-set placeholders; no scientific justification is claimed"}

    def sha256(self) -> str:
        return hashlib.sha256(canonical(self.as_dict()).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PollResult:
    status: str
    records: int
    bytes_consumed: int
    offset: int
    partial_tail_bytes: int
    more_available: bool
    read_ms: int
    error_code: str | None = None
    error: str | None = None


def _int(rec: Mapping[str, Any], key: str, *, minimum: int = 0) -> int:
    value = rec.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _fail("record_field_invalid", f"record {rec.get('rseq')!r} ({rec.get('kind')}): {key} must be an integer >= {minimum}")
    return value


def _opt_text(rec: Mapping[str, Any], key: str) -> str | None:
    value = rec.get(key)
    if value is not None and not isinstance(value, str):
        raise _fail("record_field_invalid", f"record {rec.get('rseq')!r} ({rec.get('kind')}): {key} must be text or null")
    return value


# ------------------------------------------------------------------------------------------------------------------ the tail
class IssueLogTail:
    """Incremental, strict reader of one generator issue log. One instance reads one file for one run and one worker.

    ``poll(read_time)`` consumes the complete records appended since the last poll (at most ``max_records_per_poll`` and ``max_bytes_per_poll``
    bytes per call) and returns a ``PollResult``. It never raises for a bad log: the error is returned and kept (``tail.error``), and every later
    poll returns it again. The reader keeps the per-record issue times (8 bytes each), the per-poll read marks and the liveness segments; it
    never holds the file.
    """

    def __init__(self, path: Path, policy: IssueLogPolicy, *, checkpoint_path: Path | None = None):
        self.path = Path(path)
        self.policy = policy
        self.checkpoint_path = None if checkpoint_path is None else Path(checkpoint_path)
        self.error: IssueLogError | None = None
        self.error_read_ms: int | None = None                      # the read time of the poll that found the failure
        self._offset = 0
        self._rseq = 0
        self._prev = GENESIS_SHA256
        self._head: str | None = None
        self._tail_buf = b""
        self._file_id: int | None = None
        self._start: dict[str, Any] | None = None
        self._stop: dict[str, Any] | None = None
        self._pending_intent: tuple[int, int] | None = None        # (n, tx_seq) of an intent not yet followed by its attempt
        self._next_n = 0
        self._last_tx_seq = -1
        self._unresolved: dict[int, int] = {}                      # n -> attempt_ms
        self._attempt_n, self._attempt_ms, self._attempt_read = array("q"), array("q"), array("q")
        self._seg_start, self._seg_end, self._seg_read = array("q"), array("q"), array("q")
        self._gaps: list[tuple[int, int]] = []
        self._counts = {"intents": 0, "attempted": 0, "returned": 0, "rejected": 0, "ticks": 0}
        self._client_ids: dict[int, str] = {}                      # n -> client_tx_id the generator saw (only when the return value had one)
        self._asset_keys: dict[int, str] = {}
        self._last_t: int | None = None
        self._last_mono: int | None = None
        self._last_point: int | None = None
        self._partial_tail = 0
        self._marks: list[list[int]] = []                          # [rseq_end, read_ms] per poll that consumed records
        self._last_read_ms: int | None = None
        self._polls = 0

    # --------------------------------------------------------------------------------------------------- reading
    def poll(self, read_time: datetime) -> PollResult:
        read_ms = utc_ms(read_time)
        if self._last_read_ms is not None and read_ms < self._last_read_ms:
            return self._failed(_fail("read_clock_regressed", "poll read times must not move backwards"), read_ms)
        self._last_read_ms = read_ms
        self._polls += 1
        if self.error is not None:
            return self._result(S_ERROR, 0, 0, False, read_ms)
        try:
            return self._poll(read_ms)
        except IssueLogError as exc:
            return self._failed(exc, read_ms)

    def _failed(self, exc: IssueLogError, read_ms: int) -> PollResult:
        if self.error is None:
            self.error, self.error_read_ms = exc, read_ms
        return self._result(S_ERROR, 0, 0, False, read_ms)

    def _result(self, status: str, records: int, consumed: int, more: bool, read_ms: int) -> PollResult:
        return PollResult(status, records, consumed, self._offset, self._partial_tail, more, read_ms,
                          None if self.error is None else self.error.code, None if self.error is None else str(self.error))

    def _check_file(self) -> os.stat_result | None:
        try:
            info = self.path.stat()
        except FileNotFoundError:
            if self._offset or self._rseq:
                raise _fail("log_missing_after_start", f"{self.path.name} existed and has been removed")
            return None
        file_id = getattr(info, "st_ino", 0) or None
        if self._file_id is None:
            self._file_id = file_id
        elif file_id is not None and file_id != self._file_id:
            raise _fail("log_replaced", f"{self.path.name} is a different file from the one being read (file identity changed)")
        if info.st_size < self._offset:
            raise _fail("log_truncated", f"{self.path.name} is {info.st_size} bytes, shorter than the {self._offset} already consumed")
        if self._tail_buf:
            with self.path.open("rb") as stream:
                stream.seek(self._offset - len(self._tail_buf))
                if stream.read(len(self._tail_buf)) != self._tail_buf:
                    raise _fail("log_replaced", f"the bytes of {self.path.name} before offset {self._offset} no longer match what was read")
        return info

    def _poll(self, read_ms: int) -> PollResult:
        info = self._check_file()
        if info is None:
            return self._result(S_MISSING, 0, 0, False, read_ms)
        limit = self.policy.max_bytes_per_poll
        with self.path.open("rb") as stream:
            stream.seek(self._offset)
            data = stream.read(limit)
        if not data:
            self._partial_tail = 0
            return self._result(S_NO_NEW, 0, 0, False, read_ms)
        cut = data.rfind(b"\n")
        if cut < 0:
            if len(data) > self.policy.max_line_bytes:
                raise _fail("line_too_long", f"a record in {self.path.name} exceeds {self.policy.max_line_bytes} bytes without a newline")
            self._partial_tail = len(data)
            return self._result(S_PARTIAL, 0, 0, False, read_ms)
        lines = data[:cut].split(b"\n")
        records = consumed = 0
        stopped_early = False
        for line in lines:
            if records >= self.policy.max_records_per_poll:
                stopped_early = True
                break
            self._process(line, read_ms)
            step = len(line) + 1
            self._offset += step
            self._tail_buf = (self._tail_buf + line + b"\n")[-TAIL_BYTES:]
            consumed += step
            records += 1
        if records:
            if self._marks and self._marks[-1][1] == read_ms:
                self._marks[-1][0] = self._rseq
            else:
                self._marks.append([self._rseq, read_ms])
        # bytes after the last newline are a partial record only when this read reached the end of the file
        reached_end = len(data) < limit and not stopped_early
        self._partial_tail = (len(data) - (cut + 1)) if reached_end else 0
        if self._partial_tail > self.policy.max_line_bytes:
            raise _fail("line_too_long", f"an unterminated record in {self.path.name} already exceeds {self.policy.max_line_bytes} bytes")
        more = stopped_early or len(data) == limit
        if self.checkpoint_path is not None and records:
            self.save_checkpoint()
        return self._result(S_PROGRESS, records, consumed, more, read_ms)

    # ------------------------------------------------------------------------------------------------ one record
    def _process(self, line: bytes, read_ms: int) -> None:
        pol = self.policy
        if not line.strip():
            raise _fail("record_blank", f"blank line after record {self._rseq}")
        if len(line) > pol.max_line_bytes:
            raise _fail("line_too_long", f"record {self._rseq + 1} is {len(line)} bytes, over {pol.max_line_bytes}")
        try:
            body = line.decode("ascii")
            rec = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _fail("record_malformed", f"record {self._rseq + 1} is not ASCII JSON") from exc
        if not isinstance(rec, dict):
            raise _fail("record_malformed", f"record {self._rseq + 1} is not an object")
        if canonical(rec) != body:
            raise _fail("record_not_canonical", f"record {self._rseq + 1} is not in canonical form (edited or re-serialised)")
        if rec.get("schema") != ISSUE_LOG_SCHEMA:
            raise _fail("schema_mismatch", f"record {self._rseq + 1} schema is {rec.get('schema')!r}")
        rseq = rec.get("rseq")
        if isinstance(rseq, bool) or not isinstance(rseq, int):
            raise _fail("record_field_invalid", "rseq must be an integer")
        if rseq != self._rseq + 1:
            raise _fail("record_duplicate" if rseq <= self._rseq else "record_sequence_gap", f"expected record {self._rseq + 1}, found {rseq}")
        if rec.get("prev") != self._prev:
            raise _fail("hash_chain_broken", f"record {rseq} does not follow record {rseq - 1}")
        if rec.get("sha256") != event_digest(rec, self._prev):
            raise _fail("hash_mismatch", f"record {rseq} was altered after it was written")
        kind = rec.get("kind")
        if kind not in RECORD_KINDS:
            raise _fail("record_kind_unknown", f"record {rseq} has kind {kind!r}")
        if rec.get("run_id") != pol.run_id:
            raise _fail("run_identity_mismatch", f"record {rseq} is for run {rec.get('run_id')!r}, not {pol.run_id!r}")
        if rec.get("worker_id") != pol.worker_id:
            raise _fail("worker_identity_mismatch", f"record {rseq} is for worker {rec.get('worker_id')!r}, not {pol.worker_id!r}")
        t, mono = _int(rec, "t_ms", minimum=1), _int(rec, "mono_ms")
        if self._last_t is not None:
            if t < self._last_t:
                raise _fail("clock_regression", f"record {rseq}: the generator wall clock moved back ({self._last_t} -> {t})")
            if mono < self._last_mono:
                raise _fail("monotonic_regression", f"record {rseq}: the generator monotonic clock moved back")
            skew = abs((t - self._last_t) - (mono - self._last_mono))
            if skew > pol.max_clock_skew_ms:
                raise _fail("wall_clock_step", f"record {rseq}: wall and monotonic clocks disagree by {skew} ms (a clock step)")
        if rseq == 1 and kind != K_START:
            raise _fail("journal_not_started", "the first record of an issue log must be a start")
        if rseq > 1 and kind == K_START:
            raise _fail("start_repeated", f"record {rseq}: a second start in one log")
        if self._stop is not None:
            raise _fail("record_after_stop", f"record {rseq} follows the stop record")
        if self._pending_intent is not None and kind != K_ATTEMPT:
            raise _fail("attempt_ordering_violation", f"record {rseq} ({kind}) follows intent {self._pending_intent[0]} that has no attempt")
        getattr(self, f"_on_{kind}")(rec, t, read_ms)
        self._rseq, self._prev = rseq, rec["sha256"]
        self._head = self._head or rec["sha256"]
        self._last_t, self._last_mono = t, mono

    def _on_start(self, rec: Mapping[str, Any], t: int, read_ms: int) -> None:
        pol = self.policy
        index = _int(rec, "worker_index")
        if rec.get("worker_id") != f"w{index}":
            raise _fail("worker_identity_mismatch", f"the start record's worker_id {rec.get('worker_id')!r} is not w{index}")
        if rec.get("total_workers") != 1:
            raise _fail("worker_count_unsupported", f"start declares {rec.get('total_workers')!r} workers; exactly one is supported")
        if tuple(rec.get("worker_roster") or ()) != pol.expected_roster:
            raise _fail("worker_roster_mismatch", f"start declares roster {rec.get('worker_roster')!r}, expected {list(pol.expected_roster)}")
        if rec.get("clock_domain") != pol.clock_domain:
            raise _fail("clock_domain_mismatch", f"start declares clock domain {rec.get('clock_domain')!r}, expected {pol.clock_domain!r}")
        if rec.get("event_time_semantics") != EVENT_TIME_SEMANTICS:
            raise _fail("timestamp_semantics_mismatch", f"start declares event time {rec.get('event_time_semantics')!r}, not {EVENT_TIME_SEMANTICS!r}")
        _int(rec, "round_index")
        _int(rec, "tick_interval_ms", minimum=1)
        self._start = dict(rec)
        self._point(t, read_ms)

    def _on_intent(self, rec: Mapping[str, Any], t: int, read_ms: int) -> None:
        n, tx_seq = _int(rec, "n"), _int(rec, "tx_seq")
        if n != self._next_n:
            raise _fail("issue_sequence_gap" if n > self._next_n else "issue_duplicate", f"intent n={n}, expected {self._next_n}")
        if tx_seq <= self._last_tx_seq:
            raise _fail("tx_seq_regressed", f"intent {n}: tx_seq {tx_seq} was already issued (last {self._last_tx_seq}); a retry or double send")
        if rec.get("issue_id") != f"{self.policy.run_id}:{self._start['round_index']}:{n}":
            raise _fail("issue_id_mismatch", f"intent {n}: issue_id {rec.get('issue_id')!r} does not follow the run, round and n")
        _opt_text(rec, "op")
        key = _opt_text(rec, "asset_key")
        if key is not None:
            self._asset_keys[n] = key
        self._pending_intent = (n, tx_seq)
        self._next_n += 1
        self._last_tx_seq = tx_seq
        self._counts["intents"] += 1

    def _on_attempt(self, rec: Mapping[str, Any], t: int, read_ms: int) -> None:
        n = _int(rec, "n")
        if self._pending_intent is None or self._pending_intent[0] != n:
            raise _fail("attempt_without_intent", f"attempt {n} has no immediately preceding intent")
        if rec.get("issue_id") != f"{self.policy.run_id}:{self._start['round_index']}:{n}":
            raise _fail("issue_id_mismatch", f"attempt {n}: issue_id does not match")
        self._pending_intent = None
        self._unresolved[n] = t
        if len(self._unresolved) > self.policy.max_unresolved:
            raise _fail("unresolved_limit_exceeded", f"more than {self.policy.max_unresolved} attempts have no outcome")
        self._attempt_n.append(n)
        self._attempt_ms.append(t)
        self._attempt_read.append(read_ms)
        self._counts["attempted"] += 1

    def _on_outcome(self, rec: Mapping[str, Any], t: int, read_ms: int) -> None:
        n = _int(rec, "n")
        if n not in self._unresolved:
            attempted = n < self._next_n and not (self._pending_intent is not None and self._pending_intent[0] == n)
            raise _fail("outcome_duplicate" if attempted else "outcome_without_attempt", f"outcome for n={n} has no open attempt")
        result = rec.get("result")
        if result not in RESULTS:
            raise _fail("record_field_invalid", f"outcome {n}: result {result!r} is not one of {RESULTS}")
        _opt_text(rec, "error")
        _opt_text(rec, "client_status")
        client_id = _opt_text(rec, "client_tx_id")
        if client_id is not None:
            self._client_ids[n] = client_id
        if t < self._unresolved[n]:
            raise _fail("clock_regression", f"outcome {n} is earlier than its attempt")
        del self._unresolved[n]
        self._counts["returned" if result == "returned" else "rejected"] += 1

    def _on_tick(self, rec: Mapping[str, Any], t: int, read_ms: int) -> None:
        expected = {"attempted": self._counts["attempted"], "intents": self._counts["intents"],
                    "outcomes": self._counts["returned"] + self._counts["rejected"]}
        for key, value in expected.items():
            if _int(rec, key) != value:
                raise _fail("tick_count_mismatch", f"tick reports {key}={rec[key]}, the records before it hold {value}")
        self._counts["ticks"] += 1
        self._point(t, read_ms)

    def _on_stop(self, rec: Mapping[str, Any], t: int, read_ms: int) -> None:
        if not isinstance(rec.get("reason"), str):
            raise _fail("record_field_invalid", "stop needs a text reason")
        expected = {"attempted": self._counts["attempted"], "intents": self._counts["intents"],
                    "outcomes": self._counts["returned"] + self._counts["rejected"], "in_flight": len(self._unresolved)}
        for key, value in expected.items():
            if _int(rec, key) != value:
                raise _fail("stop_count_mismatch", f"stop reports {key}={rec[key]}, the records before it hold {value}")
        self._stop = dict(rec)
        self._point(t, read_ms)

    def _point(self, t: int, read_ms: int) -> None:
        """A liveness point (start, tick, stop). Two consecutive points no further apart than ``max_tick_gap_ms`` make an observed segment; a wider
        gap is recorded as unobserved. A point never extends coverage forward in time."""
        previous = self._last_point
        if previous is not None:
            if t - previous <= self.policy.max_tick_gap_ms:
                self._seg_start.append(previous)
                self._seg_end.append(t)
                self._seg_read.append(read_ms)
            else:
                self._gaps.append((previous, t))
        self._last_point = t

    # --------------------------------------------------------------------------------------------- checkpoint
    def _checkpoint_body(self) -> dict[str, Any]:
        return {"schema": CHECKPOINT_SCHEMA, "log_name": self.path.name, "run_id": self.policy.run_id, "worker_id": self.policy.worker_id,
                "policy_sha256": self.policy.sha256(), "offset": self._offset, "rseq": self._rseq, "last_sha256": self._prev, "head_sha256": self._head,
                "tail_hex": self._tail_buf.hex(), "tail_sha256": hashlib.sha256(self._tail_buf).hexdigest(), "file_id": self._file_id,
                "counts": dict(self._counts), "unresolved": len(self._unresolved), "marks": [list(m) for m in self._marks]}

    def save_checkpoint(self) -> Path:
        """Atomically write the checkpoint (write, fsync, replace). Called after every poll that consumed records when a path was given."""
        if self.checkpoint_path is None:
            raise _fail("checkpoint_path_missing", "no checkpoint path was given")
        body = self._checkpoint_body()
        doc = {**body, "self_sha256": hashlib.sha256(canonical(body).encode("utf-8")).hexdigest()}
        tmp = self.checkpoint_path.with_name(self.checkpoint_path.name + ".tmp")
        with tmp.open("wb") as handle:
            handle.write((canonical(doc) + "\n").encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.checkpoint_path)
        return self.checkpoint_path

    @classmethod
    def resume(cls, path: Path, policy: IssueLogPolicy, checkpoint_path: Path) -> "IssueLogTail":
        """Rebuild the reader from a checkpoint. The log is re-verified from byte 0 in bounded chunks (records read before the checkpoint keep their
        ORIGINAL read times, taken from the marks), then compared with what the checkpoint recorded. A missing, corrupt, mismatched, truncated or
        replaced log raises ``IssueLogError``; a resumed reader is never built on an unverified prefix."""
        checkpoint_path = Path(checkpoint_path)
        try:
            doc = json.loads(checkpoint_path.read_text(encoding="ascii"))
        except (OSError, ValueError) as exc:
            raise _fail("checkpoint_unreadable", f"{checkpoint_path.name} cannot be read") from exc
        body = {k: v for k, v in doc.items() if k != "self_sha256"}
        if doc.get("self_sha256") != hashlib.sha256(canonical(body).encode("utf-8")).hexdigest():
            raise _fail("checkpoint_corrupt", f"{checkpoint_path.name} fails its own digest")
        if body.get("schema") != CHECKPOINT_SCHEMA:
            raise _fail("checkpoint_schema_mismatch", f"checkpoint schema is {body.get('schema')!r}")
        for key, want in (("run_id", policy.run_id), ("worker_id", policy.worker_id), ("log_name", Path(path).name), ("policy_sha256", policy.sha256())):
            if body.get(key) != want:
                raise _fail("checkpoint_mismatch", f"checkpoint {key} is {body.get(key)!r}, expected {want!r}")
        tail = cls(path, policy, checkpoint_path=checkpoint_path)
        size = tail.path.stat().st_size if tail.path.exists() else -1
        if size < 0:
            raise _fail("log_missing_after_start", f"{tail.path.name} is missing")
        if size < body["offset"]:
            raise _fail("log_truncated", f"{tail.path.name} is {size} bytes, shorter than the checkpoint offset {body['offset']}")
        marks = [list(m) for m in body["marks"]]
        ends = [m[0] for m in marks]
        info = tail.path.stat()
        tail._file_id = getattr(info, "st_ino", 0) or None
        if body.get("file_id") is not None and tail._file_id is not None and body["file_id"] != tail._file_id:
            raise _fail("log_replaced", f"{tail.path.name} is a different file from the one checkpointed")
        with tail.path.open("rb") as stream:
            carry = b""
            while tail._offset < body["offset"]:
                data = stream.read(policy.max_bytes_per_poll)
                if not data:
                    raise _fail("log_truncated", "the log ended before the checkpoint offset")
                data = carry + data
                *lines, carry = data.split(b"\n")
                for line in lines:
                    index = bisect.bisect_left(ends, tail._rseq + 1)
                    if index >= len(marks):
                        raise _fail("checkpoint_mismatch", "the log holds records the checkpoint's read marks do not cover")
                    tail._process(line, marks[index][1])
                    tail._offset += len(line) + 1
                    tail._tail_buf = (tail._tail_buf + line + b"\n")[-TAIL_BYTES:]
                    if tail._offset > body["offset"]:
                        raise _fail("checkpoint_mismatch", "the checkpoint offset is not a record boundary")
                    if tail._offset == body["offset"]:
                        break
        if tail._rseq == 1 and body["head_sha256"] != tail._head:
            raise _fail("log_replaced", "the first record differs from the checkpoint's")
        if tail._head != body["head_sha256"] or tail._rseq != body["rseq"] or tail._prev != body["last_sha256"] or tail._offset != body["offset"]:
            raise _fail("log_replaced", "the log prefix does not reproduce the checkpoint's chain head")
        if tail._tail_buf.hex() != body["tail_hex"] or dict(tail._counts) != body["counts"]:
            raise _fail("checkpoint_mismatch", "the re-read state differs from the checkpoint")
        tail._marks = marks
        tail._last_read_ms = marks[-1][1] if marks else None
        return tail

    # ------------------------------------------------------------------------------------------------- queries
    @property
    def offset(self) -> int:
        return self._offset

    @property
    def partial_tail_bytes(self) -> int:
        return self._partial_tail

    @property
    def chain_head(self) -> str:
        return self._prev

    @property
    def start_record(self) -> dict[str, Any] | None:
        return None if self._start is None else dict(self._start)

    @property
    def stop_record(self) -> dict[str, Any] | None:
        return None if self._stop is None else dict(self._stop)

    @property
    def last_point_ms(self) -> int | None:
        return self._last_point

    def counts(self) -> dict[str, int]:
        return {**self._counts, "unresolved": len(self._unresolved), "intent_without_attempt": 0 if self._pending_intent is None else 1,
                "records": self._rseq}

    def writer_state(self) -> dict[str, Any]:
        """What the LOG shows about the writer. ``active`` never means "alive now": it means a start was seen, no stop, and the last liveness
        point is the reported one. Whether the writer stopped or merely went quiet is not decidable from a quiet log, which is why coverage ends at
        the last liveness point."""
        if self.error is not None:
            state = W_ERROR
        elif self._start is None:
            state = W_NOT_STARTED
        elif self._stop is not None:
            state = W_CLOSED_CLEAN if self._stop.get("reason") in ("cleanup", "reinitialized") and not self._unresolved else W_CLOSED_OTHER
        else:
            state = W_ACTIVE
        return {"state": state, "last_liveness_point_ms": self._last_point, "unobserved_gaps": len(self._gaps), "unresolved_attempts": len(self._unresolved),
                "intent_without_attempt": self._pending_intent is not None, "partial_tail_bytes": self._partial_tail,
                "error": None if self.error is None else {"code": self.error.code, "message": str(self.error)},
                "note": "a quiet log is not a stopped writer and not a zero second; coverage ends at the last liveness point"}

    def attempt_events(self, instance: str, *, as_of_ms: int | None = None) -> list[dict[str, Any]]:
        """The issuance events (one per ``attempt``) as ``{id, ts_ms, instance, seq, read_ms}``: the event time is the generator's issue time, never a
        callback or completion time. With ``as_of_ms`` only records already READ by then are returned."""
        run, rnd = self.policy.run_id, (self._start or {}).get("round_index")
        out = []
        for n, ms, rd in zip(self._attempt_n, self._attempt_ms, self._attempt_read):
            if as_of_ms is not None and rd > as_of_ms:
                break
            out.append({"id": f"{run}:{rnd}:{n}", "ts_ms": int(ms), "instance": instance, "seq": int(n), "read_ms": int(rd)})
        return out

    def observed_segments(self, *, as_of_ms: int | None = None) -> list[tuple[int, int]]:
        """Merged ``(start_ms, end_ms)`` generator-observed segments, restricted to those whose closing liveness point had been read by ``as_of_ms``."""
        merged: list[list[int]] = []
        for s, e, rd in zip(self._seg_start, self._seg_end, self._seg_read):
            if as_of_ms is not None and rd > as_of_ms:
                break
            if merged and s <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        return [(s, e) for s, e in merged]

    def liveness_points_since(self, count: int) -> list[tuple[int, int, int]]:
        """Segments ``[count:]`` as ``(start_ms, end_ms, read_ms)`` (for the journal bridge)."""
        return [(int(s), int(e), int(r)) for s, e, r in list(zip(self._seg_start, self._seg_end, self._seg_read))[count:]]

    @property
    def segment_count(self) -> int:
        return len(self._seg_end)

    @property
    def gaps(self) -> tuple:
        return tuple(self._gaps)

    def unresolved_ns(self) -> list[int]:
        return sorted(self._unresolved)

    def summary(self) -> dict[str, Any]:
        start = self._start or {}
        return {"schema": "issue_log_evidence_summary/1", "log_schema": ISSUE_LOG_SCHEMA, "log_name": self.path.name, "run_id": self.policy.run_id,
                "worker_id": self.policy.worker_id, "clock_domain": self.policy.clock_domain, "expected_roster": list(self.policy.expected_roster),
                "workload_id": start.get("workload_id"), "writer_version": start.get("writer_version"), "chain_head_sha256": self._prev if self._rseq else None,
                "head_record_sha256": self._head, "records": self._rseq, "bytes_consumed": self._offset, "counts": self.counts(),
                "writer": self.writer_state(), "event_time": "generator_issue_time (attempt record t_ms, the worker's Date.now())",
                "read_time": "the poll that consumed each record; evidence at a decision time contains only records already read",
                "not_claimed": ["ledger commit", "Caliper txSubmitted emission", "Fabric acceptance", "external Ethereum mempool capture",
                                "a live run (unless the caller says so)"],
                "descriptor": issue_log_descriptor(self.policy.run_id, self.policy.worker_id)}

    # ---------------------------------------------------------------------------------------------- evidence
    def evidence(self, *, instance: str, max_callback_lag_ms: int) -> "IssueLogEvidence":
        """The source-count evidence for a recorder-scope coverage (``VerifiedCoverage(source_evidence=...)``). Raises when the log is invalid or has
        not started; a log with a pending partial tail is marked ``torn_tail`` (nothing after its last complete record is certified)."""
        if self.error is not None:
            raise self.error
        if self._start is None:
            raise _fail("issue_log_not_started", "no start record has been read: there is no generator-side evidence")
        return IssueLogEvidence(
            descriptor=issue_log_descriptor(self.policy.run_id, self.policy.worker_id), worker=self.policy.worker_id, clock_domain=self.policy.clock_domain,
            run_id=self.policy.run_id, instance=instance, max_callback_lag_ms=max_callback_lag_ms,
            dispatch_ms=tuple((int(n), int(ms)) for n, ms in zip(self._attempt_n, self._attempt_ms)), overflow_rows=0, duplicates_dropped=0,
            torn_tail=self._partial_tail > 0, log_sha256=self._prev, read_ms=tuple(int(r) for r in self._attempt_read),
            observed_segments=tuple((int(s), int(e), int(r)) for s, e, r in zip(self._seg_start, self._seg_end, self._seg_read)),
            counts=dict(self.counts()), chain_head_sha256=self._prev)


# ------------------------------------------------------------------------------------------------------------ recorder-route evidence
def _round_in(start_ms: int, end_ms: int) -> tuple[int, int]:
    """Whole-second bounds INSIDE ``[start_ms, end_ms]`` (ceil the start, floor the end), as epoch ms."""
    lo = -(-start_ms // 1000) * 1000
    hi = (end_ms // 1000) * 1000
    return lo, hi


@dataclass(frozen=True)
class IssueLogEvidence(sc.DispatchLogEvidence):
    """Generator-side issuance evidence for a recorder-scope coverage. A ``DispatchLogEvidence`` whose rows are the issue log's ``attempt`` records
    (so the A0k reconciliation with the recorder's cumulative counts is reused unchanged), restricted to records already read at the decision time,
    and whose verified spans are further intersected with the spans in which the generator was OBSERVED (liveness points no further apart than
    the policy's ``max_tick_gap_ms``). A span is verified only where the recorder's count equals the generator's count AND the generator was
    observed; anything else stays unverified, so a zero there stays unknown."""

    read_ms: tuple = ()
    observed_segments: tuple = ()
    counts: Mapping[str, Any] = field(default_factory=dict)
    chain_head_sha256: str | None = None

    def __post_init__(self) -> None:                                   # replaces the parent's check, which pins derived_from to the dispatch log
        for name in ("worker", "clock_domain", "run_id", "instance"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise sc.SourceEvidenceError("source_evidence_identity_missing", f"issue-log evidence needs a non-empty {name}")
        lag = self.max_callback_lag_ms
        if isinstance(lag, bool) or not isinstance(lag, int) or lag < 0:
            raise sc.SourceEvidenceError("source_evidence_policy_invalid", "max_callback_lag_ms must be a non-negative whole number (declared, unmeasured)")
        verdict = sc.classify_descriptor(self.descriptor)
        if not verdict.certifies_intervals or self.descriptor.get("kind") != sc.KIND_ISSUE_LOG:
            raise sc.SourceEvidenceError("source_descriptor_not_interval_suitable", f"{verdict.label}: {verdict.detail}")
        if self.descriptor.get("derived_from") != ISSUE_DERIVED_FROM:
            raise sc.SourceEvidenceError("source_descriptor_not_interval_suitable", f"derived_from must be {ISSUE_DERIVED_FROM!r}")
        if len(self.read_ms) != len(self.dispatch_ms):
            raise sc.SourceEvidenceError("source_evidence_inconsistent", "every issuance needs a read time")
        previous = previous_read = None
        for index, (seq, stamp) in enumerate(self.dispatch_ms):
            if seq != index:
                raise sc.SourceEvidenceError("dispatch_seq_gap", f"issue numbers are not contiguous from 0 at {seq}")
            if previous is not None and stamp < previous:
                raise sc.SourceEvidenceError("dispatch_clock_regression", f"issue time moved backwards at n={seq}")
            if previous_read is not None and self.read_ms[index] < previous_read:
                raise sc.SourceEvidenceError("read_clock_regressed", f"read time moved backwards at n={seq}")
            previous, previous_read = stamp, self.read_ms[index]

    def timestamp_provenance(self) -> dict[str, Any]:
        doc = super().timestamp_provenance()
        doc.update({"raw_unit": "epoch_ms (workload module Date.now(), the attempt record)", "log_sha256": None, "chain_head_sha256": self.chain_head_sha256,
                    "read_time": "evidence at a decision time contains only records read by then", "source": "e1_issue_log/1"})
        return doc

    def spans(self, derivation: Any, as_of: Any, events: Sequence[Mapping[str, Any]] | None = None) -> sc.SourceSpans:
        for row in events or ():                                          # the log must belong to the recorder instance it is reconciled with
            if row.get("kind") == "start" and row.get("collector_instance") == self.instance:
                if row.get("worker") is not None and f"w{row['worker']}" != self.worker:
                    raise sc.SourceEvidenceError("worker_identity_mismatch",
                                                 f"the recorder instance {self.instance!r} is worker {row['worker']!r}; the issue log is worker {self.worker!r}")
                if row.get("run_id") != self.run_id:
                    raise sc.SourceEvidenceError("run_identity_mismatch", f"the recorder instance is for run {row.get('run_id')!r}, the issue log for {self.run_id!r}")
        as_of_ms = utc_ms(as_of, "as_of")
        cut = bisect.bisect_right(self.read_ms, as_of_ms)
        view = replace(self, dispatch_ms=self.dispatch_ms[:cut], read_ms=self.read_ms[:cut])
        base = sc.DispatchLogEvidence.spans(view, derivation, as_of, events)
        segments: list[list[int]] = []
        for s, e, rd in self.observed_segments:
            if rd > as_of_ms:
                break
            if segments and s <= segments[-1][1]:
                segments[-1][1] = max(segments[-1][1], e)
            else:
                segments.append([s, e])
        rounded = [lo_hi for lo_hi in (_round_in(s, e) for s, e in segments) if lo_hi[1] > lo_hi[0]]
        verified = []
        for v_start, v_end in base.verified:
            vs, ve = utc_ms(v_start, "span start"), utc_ms(v_end, "span end")
            for lo, hi in rounded:
                a, b = max(vs, lo), min(ve, hi)
                if b > a:
                    verified.append((ms_to_utc(a), ms_to_utc(b)))
        rejections = list(base.rejections)
        if not rounded:
            rejections.append((RC_GENERATOR_UNOBSERVED, self.instance, "no generator liveness segment had been read: the generator was never shown to be observed"))
        elif base.verified and not verified:
            rejections.append((RC_GENERATOR_UNOBSERVED, self.instance, "the reconciled spans lie outside every generator-observed segment"))
        # An attempt without an outcome is still an ATTEMPT (it is counted, and must be matched by a recorder callback or the span stays unverified), but it is
        # never reported as a completed submission: the counts say how many are unresolved (``reconcile_counts``, ``IssueLogTail.summary``), and a stop with
        # unresolved attempts is an aborted stop in the journal bridge.
        return sc.SourceSpans(tuple(sc.merge_spans(verified)), base.mismatches, (ISSUE_DERIVED_FROM,), ("True",), base.classifications,
                              tuple(rejections), "issue_log")


# ------------------------------------------------------------------------------------------------------------ count reconciliation
def reconcile_counts(tail: IssueLogTail, *, recorder_submit_units: int | None = None, recorder_completed_ids: Iterable[str] | None = None,
                     ledger_committed_keys: Iterable[str] | None = None) -> dict[str, Any]:
    """Place the generator's counts next to the other observers' counts WITHOUT forcing them to agree.

    Differences are legitimate (failed sends, timeouts, retries inside Caliper, uncommitted transactions) and are reported, never repaired. A
    join between two observers is made only on a stable identifier both sides carry: the Caliper transaction id (``client_tx_id``, only when the
    ``sendRequests`` return value exposed one) or the asset key. Without a shared identifier only COUNTS are compared and the record says so;
    a timestamp is never used to match records.
    """
    c = tail.counts()
    out: dict[str, Any] = {"schema": "issue_log_count_reconciliation/1", "generator": {
        "intents": c["intents"], "submission_attempts": c["attempted"], "intent_without_attempt": c["intent_without_attempt"],
        "send_returned": c["returned"], "send_rejected": c["rejected"], "unresolved_attempts": c["unresolved"]},
        "note": "counts are reported side by side; a difference is evidence of a failure mode, not an error to be reconciled away"}
    if recorder_submit_units is not None:
        out["recorder_submit_callbacks"] = {"units": int(recorder_submit_units), "minus_generator_attempts": int(recorder_submit_units) - c["attempted"],
                                            "join": "counts only (no stable identifier on a submit callback)"}
    if recorder_completed_ids is not None:
        ids = {str(i) for i in recorder_completed_ids}
        mine = set(tail._client_ids.values())
        out["recorder_completed"] = {"records": len(ids), "generator_ids_known": len(mine), "matched_by_client_tx_id": len(ids & mine),
                                     "generator_ids_without_recorder_row": len(mine - ids), "recorder_rows_without_generator_id": len(ids - mine),
                                     "join": "stable identifier (client_tx_id); only attempts whose sendRequests return value exposed an id can match"}
    if ledger_committed_keys is not None:
        keys = {str(k) for k in ledger_committed_keys}
        mine_keys = set(tail._asset_keys.values())
        out["ledger_committed"] = {"keys": len(keys), "generator_keys_known": len(mine_keys), "matched_by_asset_key": len(keys & mine_keys),
                                   "generator_keys_not_committed": len(mine_keys - keys), "committed_keys_not_issued_here": len(keys - mine_keys),
                                   "join": "stable identifier (asset key; CreateAsset operations only)"}
    return out
