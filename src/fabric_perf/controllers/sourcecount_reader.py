"""SourceCount reader driver (task A0o, 2026-11-21): a bounded, round-isolated driver around the unmodified ``IssueLogTail`` and the A0l coverage bridge. OFFLINE ONLY.

Flow it serves:  generator issue log -> ``IssueLogTail`` -> this driver -> coverage validation (``GeneratorIssueCoverage``) -> captured workload history -> Stage 3.

What it adds to what exists (nothing in ``issue_log_reader`` or ``issue_log_coverage`` is edited; both are candidate files with recorded hashes):

* IDENTITY. A reader is bound to run, ROUND, worker, log path and checkpoint path. The round is checked against the log's own ``start`` record (a log of another round is
  refused as ``round_identity_mismatch``) and is written into the checkpoint, so a checkpoint cannot be resumed against another round. The coverage bridge's literal ``:r0``
  instance is replaced by a round-carrying instance (``RoundIsolatedBridge``), so the warm-up and the measured round never share a reader identity.
* MODES. ``inspect`` (a finished log, one explicit read time), ``replay`` (a stored log revealed to the reader step by step on a caller-supplied schedule; a restart can be
  scheduled), ``follow`` (an interface only: it is REFUSED here, always; nothing watches an active log).
* DESCRIPTOR. The generator-issue-log descriptor is derived here, and a supplied descriptor must equal it exactly. A declared bare counter, an unknown kind or a wrong run or
  worker is refused. ``classify_descriptor`` alone trusts a declared resolution (A0n finding 4); this driver additionally requires an observed liveness segment before it
  calls the interval source-count evidence verified.
* EVIDENCE. ``source_count_evidence/1`` (A0n design, section 6) with event time and read time kept apart, the decision cutoff, the read watermark, a per-read audit and the
  limits of the claim.

What a pass does NOT mean. A log count is independent of the recorder's callbacks at the software data-flow level only: it is not independent of the generator process, the
operating system or its clock, and it says nothing about an external Ethereum mempool, a Caliper submit, a Fabric acceptance or a ledger commit. A quiet or missing log is
never zero issuance. One worker only (as everywhere else).

No process, network, container, model or Caliper API is imported (a test asserts that).
"""

from __future__ import annotations

import bisect
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import issue_log_coverage as gc
from . import issue_log_reader as ilr
from . import source_completeness as sc
from .workload_coverage import CoveragePolicy

DRIVER_VERSION = "sourcecount_reader 0.1 (A0o, 2026-11-21)"
EVIDENCE_SCHEMA = "source_count_evidence/1"
AUDIT_SCHEMA = "sourcecount_reader_audit/1"
REPLAY_SCHEMA = "sourcecount_replay_schedule/1"
MODE_INSPECT, MODE_REPLAY, MODE_FOLLOW = "inspect", "replay", "follow"
MODES = (MODE_INSPECT, MODE_REPLAY, MODE_FOLLOW)
ST_VERIFIED, ST_PARTIAL, ST_UNVERIFIED, ST_INVALID = "VERIFIED_SPANS", "PARTIAL", "UNVERIFIED", "INVALID"
LIVE_FOLLOW_ENABLED = False                                    # assigned here and nowhere else; ``follow`` refuses whatever the caller passes
SUPPORTED_WORKER = "w0"
MAX_INSPECT_POLLS = 100_000                                    # a hard bound on the inspect loop (each productive poll consumes at least one record)


class SourceCountReaderError(sc.SourceEvidenceError):
    """The reader request is invalid, or an identity, schedule, checkpoint or descriptor rule failed. ``code`` is machine-readable."""


def _fail(code: str, message: str) -> SourceCountReaderError:
    return SourceCountReaderError(code, message)


# ------------------------------------------------------------------------------------------------------------------ identity
@dataclass(frozen=True)
class ReaderIdentity:
    """Everything that names one reader: run, round, worker, source log, checkpoint. All are explicit (no default, no derivation from a file name)."""

    run_id: str
    round_index: int
    worker_id: str
    log_path: Path
    clock_domain: str
    checkpoint_path: Path | None = None

    def __post_init__(self) -> None:
        for name in ("run_id", "worker_id", "clock_domain"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise _fail("identity_missing", f"{name} must be a non-blank string without surrounding blanks")
        if isinstance(self.round_index, bool) or not isinstance(self.round_index, int) or self.round_index < 0:
            raise _fail("identity_missing", "round_index must be a non-negative integer, stated explicitly")
        if self.worker_id != SUPPORTED_WORKER:
            raise _fail("worker_count_unsupported", f"only worker {SUPPORTED_WORKER!r} is supported (the E1 workload, the writer and the reader are single-worker), got {self.worker_id!r}")
        object.__setattr__(self, "log_path", Path(self.log_path))
        if self.checkpoint_path is not None:
            object.__setattr__(self, "checkpoint_path", Path(self.checkpoint_path))
            if _same(self.checkpoint_path, self.log_path):
                raise _fail("checkpoint_is_log", "the checkpoint path must differ from the log path")

    @property
    def instance(self) -> str:
        """The reader instance, carrying the round (the coverage bridge's own literal ends in ``:r0`` for every round)."""
        return f"issue-log-reader:{self.run_id}:{self.worker_id}:r{self.round_index}"

    def policy(self, **limits: Any) -> ilr.IssueLogPolicy:
        return ilr.IssueLogPolicy(run_id=self.run_id, worker_id=self.worker_id, clock_domain=self.clock_domain, **limits)

    def as_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "round_index": self.round_index, "worker_id": self.worker_id, "worker_roster": [self.worker_id], "total_workers": 1,
                "clock_domain": self.clock_domain, "reader_instance": self.instance, "log_name": self.log_path.name,
                "checkpoint_name": None if self.checkpoint_path is None else self.checkpoint_path.name}


def _same(a: Path, b: Path) -> bool:
    return Path(a).resolve() == Path(b).resolve()


def validate_round_set(identities: Sequence[ReaderIdentity], *, journal_paths: Sequence[Path | None] = ()) -> None:
    """Rounds of one run must not share a reader instance, a source log, a checkpoint or a journal. Raises on the first collision."""
    if not identities:
        raise _fail("round_set_empty", "no reader identity was supplied")
    if len({i.run_id for i in identities}) != 1:
        raise _fail("run_identity_mixed", "one round set belongs to exactly one run")
    seen: dict[str, set] = {"round": set(), "instance": set(), "log": set(), "checkpoint": set(), "journal": set()}
    for ident in identities:
        for kind, value in (("round", ident.round_index), ("instance", ident.instance), ("log", str(ident.log_path.resolve())),
                            ("checkpoint", None if ident.checkpoint_path is None else str(ident.checkpoint_path.resolve()))):
            if value is None:
                continue
            if value in seen[kind]:
                raise _fail(f"{kind}_shared", f"two readers of run {ident.run_id!r} share the same {kind}: {value}")
            seen[kind].add(value)
    for path in journal_paths:
        if path is None:
            continue
        resolved = str(Path(path).resolve())
        if resolved in seen["journal"] or resolved in seen["log"] or resolved in seen["checkpoint"]:
            raise _fail("journal_shared", f"a journal path collides with another reader's journal, log or checkpoint: {resolved}")
        seen["journal"].add(resolved)


def validate_source_descriptor(descriptor: Mapping[str, Any] | None, identity: ReaderIdentity) -> dict[str, Any]:
    """The only descriptor accepted is the one this driver derives for the run and worker. ``None`` means 'derive it'; anything supplied must equal it exactly. A declared
    counter, a declared resolution or a descriptor of another run or worker is never accepted on the strength of its own declaration."""
    expected = ilr.issue_log_descriptor(identity.run_id, identity.worker_id)
    if descriptor is not None and dict(descriptor) != expected:
        raise _fail("source_descriptor_mismatch", f"the supplied descriptor {dict(descriptor)!r} is not the issue-log descriptor {expected!r} for this run and worker")
    verdict = sc.classify_descriptor(expected)
    if not verdict.certifies_intervals or expected.get("kind") != sc.KIND_ISSUE_LOG:
        raise _fail("source_descriptor_not_interval_suitable", f"{verdict.label}: {verdict.detail}")
    return expected


# ------------------------------------------------------------------------------------------------------------------ round-bound tail and bridge
class RoundBoundTail(ilr.IssueLogTail):
    """``IssueLogTail`` that also insists on the ROUND. The round is checked against the ``start`` record and stored in the checkpoint; the base class is not edited."""

    def __init__(self, path: Path, policy: ilr.IssueLogPolicy, *, round_index: int | None = None, checkpoint_path: Path | None = None):
        super().__init__(path, policy, checkpoint_path=checkpoint_path)
        self.round_index = round_index

    @property
    def reader_instance(self) -> str:
        return f"issue-log-reader:{self.policy.run_id}:{self.policy.worker_id}:r{self.round_index}"

    def poll(self, read_time: datetime) -> ilr.PollResult:
        if self.round_index is None:
            raise _fail("round_unbound", "a reader must be bound to a round before it polls")
        return super().poll(read_time)

    def _on_start(self, rec: Mapping[str, Any], t: int, read_ms: int) -> None:
        if self.round_index is not None and rec.get("round_index") != self.round_index:
            raise ilr._fail("round_identity_mismatch", f"the log's start record is for round {rec.get('round_index')!r}, this reader is bound to round {self.round_index!r}")
        super()._on_start(rec, t, read_ms)

    def _checkpoint_body(self) -> dict[str, Any]:
        body = super()._checkpoint_body()
        body["round_index"] = self.round_index
        body["reader_instance"] = self.reader_instance
        return body

    @classmethod
    def resume(cls, path: Path, policy: ilr.IssueLogPolicy, checkpoint_path: Path, *, round_index: int) -> "RoundBoundTail":     # type: ignore[override]
        """Resume only a checkpoint that was written for THIS round. The base class re-verifies the log from byte 0; the round is then checked against both the checkpoint
        and the log's start record."""
        try:
            doc = json.loads(Path(checkpoint_path).read_text(encoding="ascii"))
        except (OSError, ValueError) as exc:
            raise ilr._fail("checkpoint_unreadable", f"{Path(checkpoint_path).name} cannot be read") from exc
        if doc.get("round_index") != round_index:
            raise ilr._fail("checkpoint_round_mismatch", f"the checkpoint is for round {doc.get('round_index')!r} (absent means it was not written by this driver), "
                                                          f"the reader is bound to round {round_index!r}")
        tail = super().resume(path, policy, checkpoint_path)
        tail.round_index = round_index
        if tail.start_record is not None and tail.start_record.get("round_index") != round_index:
            raise ilr._fail("round_identity_mismatch", "the log's start record is for another round than the one resumed")
        if doc.get("reader_instance") != tail.reader_instance:
            raise ilr._fail("checkpoint_round_mismatch", f"the checkpoint's reader instance {doc.get('reader_instance')!r} is not {tail.reader_instance!r}")
        return tail


class RoundIsolatedBridge(gc.IssueLogJournalBridge):
    """The A0l journal bridge with a round-carrying instance (the base hard-codes ``...:r0``). Everything else is inherited unchanged."""

    def __init__(self, tail: RoundBoundTail, journal_path: Path, *, source_id: str, origin: str, channel_id: str | None = None):
        if not isinstance(tail, RoundBoundTail) or tail.round_index is None:
            raise _fail("round_unbound", "the bridge needs a RoundBoundTail bound to a round")
        super().__init__(tail, journal_path, source_id=source_id, origin=origin, channel_id=channel_id)
        self.instance = tail.reader_instance


# ------------------------------------------------------------------------------------------------------------------ helpers
def _to_dt(value: Any) -> datetime:
    """An aware datetime, ISO-8601 text with an explicit offset, or epoch milliseconds. A naive time is refused, never assumed UTC."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, bool):
        raise _fail("time_invalid", "a time must not be a boolean")
    if isinstance(value, int):
        return ilr.ms_to_utc(value)
    if isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise _fail("time_invalid", f"{value!r} is not an ISO-8601 time") from exc
        if moment.tzinfo is None:
            raise _fail("time_naive", f"{value!r} has no UTC offset; a timezone is never assumed")
        return moment
    raise _fail("time_invalid", f"unsupported time value {value!r}")


def _iso(ms: int | None) -> str | None:
    return None if ms is None else ilr.ms_to_utc(ms).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _prefix_sha256(path: Path, length: int) -> str | None:
    digest, left = hashlib.sha256(), length
    try:
        with Path(path).open("rb") as handle:
            while left > 0:
                chunk = handle.read(min(1 << 20, left))
                if not chunk:
                    break
                digest.update(chunk)
                left -= len(chunk)
    except OSError:
        return None
    return digest.hexdigest() if left == 0 else None


def _merge(segments: Sequence[tuple]) -> list[list[int]]:
    merged: list[list[int]] = []
    for s, e, *_ in segments:
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return merged


# ------------------------------------------------------------------------------------------------------------------ the reader
class SourceCountReader:
    """One reader for one (run, round, worker, log). ``poll(read_time)`` advances the tail (and the optional journal bridge) and records the read in the audit trail."""

    def __init__(self, identity: ReaderIdentity, *, policy_limits: Mapping[str, Any] | None = None, journal_path: Path | None = None, source_id: str | None = None,
                 origin: str | None = None, channel_id: str | None = None, descriptor: Mapping[str, Any] | None = None):
        self.identity = identity
        self.descriptor = validate_source_descriptor(descriptor, identity)
        self.policy_limits = dict(policy_limits or {})
        if "max_tick_gap_ms" not in self.policy_limits:
            raise _fail("policy_missing", "max_tick_gap_ms must be stated by the caller; the placeholder default is not accepted (it is unapproved)")
        self.policy = identity.policy(**self.policy_limits)
        self.tail = RoundBoundTail(identity.log_path, self.policy, round_index=identity.round_index, checkpoint_path=identity.checkpoint_path)
        self.bridge: RoundIsolatedBridge | None = None
        if journal_path is not None:
            if source_id is None or origin is None:
                raise _fail("journal_arguments_missing", "a journal needs an explicit source_id and an explicit evidence origin")
            self.bridge = RoundIsolatedBridge(self.tail, journal_path, source_id=source_id, origin=origin, channel_id=channel_id)
        self.journal_path = None if journal_path is None else Path(journal_path)
        self.reads: list[dict[str, Any]] = []
        self.first_read_ms: int | None = None
        self.last_read_ms: int | None = None
        self.restarts = 0

    # ------------------------------------------------------------------------------------------------ reading
    def poll(self, read_time: Any) -> dict[str, Any]:
        moment = _to_dt(read_time)
        result = self.tail.poll(moment)
        report = self.bridge.sync(moment) if self.bridge is not None else None
        round_error = self.tail.error.code if self.tail.error is not None else None
        entry = {"index": len(self.reads), "read_utc": _iso(result.read_ms), "read_ms": result.read_ms, "status": result.status, "records": result.records,
                 "bytes_consumed": result.bytes_consumed, "offset": result.offset, "partial_tail_bytes": result.partial_tail_bytes, "more_available": result.more_available,
                 "error_code": result.error_code, "bridge": None if report is None else {"state": report["state"], "rows": report["rows"], "held_back": report["held_back"]}}
        if round_error:
            entry["round_error"] = round_error
        self.reads.append(entry)
        if self.first_read_ms is None:
            self.first_read_ms = result.read_ms
        self.last_read_ms = result.read_ms
        return entry

    def poll_until_drained(self, read_time: Any) -> list[dict[str, Any]]:
        """Poll at ONE read time until the tail reports nothing more available. Bounded: an unproductive poll ends the loop."""
        out = []
        for _ in range(MAX_INSPECT_POLLS):
            entry = self.poll(read_time)
            out.append(entry)
            if entry["status"] != ilr.S_PROGRESS or not entry["more_available"]:
                return out
        raise _fail("inspect_not_bounded", f"more than {MAX_INSPECT_POLLS} polls at one read time")

    def restart(self) -> None:
        """Rebuild the tail from its checkpoint (the log is re-verified from byte 0). A bridge cannot be restarted: its journal state is not checkpointed, so the request
        is refused rather than approximated."""
        if self.identity.checkpoint_path is None:
            raise _fail("checkpoint_path_missing", "a restart needs a checkpoint path")
        if self.bridge is not None:
            raise _fail("bridge_restart_unsupported", "the journal bridge keeps state that is not checkpointed; restart is supported for the tail only")
        self.tail = RoundBoundTail.resume(self.identity.log_path, self.policy, self.identity.checkpoint_path, round_index=self.identity.round_index)
        self.restarts += 1

    # ------------------------------------------------------------------------------------------------ evidence
    def evidence(self, *, as_of: Any | None = None, require_closed: bool = False, decision_cutoff: Any | None = None) -> dict[str, Any]:
        """``source_count_evidence/1`` for what has been read, optionally restricted to the records already READ at ``as_of`` (the decision-time rule). Nothing read after the
        cutoff is used. Missing evidence is reported as such (UNVERIFIED / INVALID), never as zero."""
        tail, ident = self.tail, self.identity
        cutoff = decision_cutoff if decision_cutoff is not None else as_of
        as_of_ms = None if cutoff is None else ilr.utc_ms(_to_dt(cutoff), "as_of")
        failures: list[str] = []
        status = ST_VERIFIED
        last_status = self.reads[-1]["status"] if self.reads else None
        if tail.error is not None:
            status, failures = ST_INVALID, [tail.error.code]
        elif last_status == ilr.S_MISSING and tail.start_record is None:
            status, failures = ST_UNVERIFIED, ["log_missing"]
        elif tail.start_record is None:
            status, failures = ST_UNVERIFIED, ["issue_log_not_started"]
        attempts = tail.attempt_events(ident.instance, as_of_ms=as_of_ms) if tail.error is None else []
        segments = _merge(tail.observed_segments(as_of_ms=as_of_ms)) if tail.error is None else []
        total_attempts = tail.counts()["attempted"]
        excluded = max(0, total_attempts - len(attempts)) if tail.error is None else 0
        writer = tail.writer_state()
        if status == ST_VERIFIED:
            if tail.partial_tail_bytes > 0:
                failures.append("partial_tail")
            if tail.stop_record is None and require_closed:
                failures.append("incomplete_lifecycle_no_stop_record")
            if tail.counts()["unresolved"]:
                failures.append(ilr.RC_UNRESOLVED)
            if tail.counts()["intent_without_attempt"]:
                failures.append("intent_without_attempt")
            if tail.gaps:
                failures.append("generator_observation_gap")
            if not segments:
                failures.append(ilr.RC_GENERATOR_UNOBSERVED)
            status = ST_VERIFIED if not failures else (ST_PARTIAL if segments else ST_UNVERIFIED)
        full_counts = tail.counts()
        stale_counts = as_of_ms is not None and self.last_read_ms is not None and as_of_ms < self.last_read_ms
        counts = {"attempts": len(attempts) if as_of_ms is not None else total_attempts, "scope": "records read at or before the decision cutoff" if as_of_ms is not None
                  else "all records read so far"}
        if status == ST_INVALID:
            counts = {"attempts": None, "intents": None, "returned": None, "rejected": None, "unresolved": None, "intent_without_attempt": None,
                      "scope": "withheld", "note": "the log failed an integrity rule: the evidence is invalid as a whole and no count taken from it is reported"}
        elif stale_counts:
            counts.update({"intents": None, "returned": None, "rejected": None, "unresolved": None, "intent_without_attempt": None,
                           "note": "only attempts carry a per-record read time; the other counters are not available as of an earlier cutoff and are withheld, not estimated"})
        else:
            counts.update({"intents": full_counts["intents"], "returned": full_counts["returned"], "rejected": full_counts["rejected"],
                           "unresolved": full_counts["unresolved"], "intent_without_attempt": full_counts["intent_without_attempt"]})
        intervals = []
        stamps = [a["ts_ms"] for a in attempts]
        for s, e in segments:
            lo, hi = ilr._round_in(s, e)
            if hi > lo:
                intervals.append({"start_utc": _iso(lo), "end_utc": _iso(hi), "attempts_in_interval": bisect.bisect_left(stamps, hi) - bisect.bisect_left(stamps, lo)})
        start = tail.start_record or {}
        return {
            "schema": EVIDENCE_SCHEMA, "driver": DRIVER_VERSION, "status": status, "failure_states": failures, "mode_provenance": "offline_fixture_or_stored_log; no live run",
            "scope": {"coverage_subject": gc.COVERAGE_SUBJECT, "external_mempool_coverage": False, "live_source_coverage_verified": False, "live_quality": False,
                      "independence": "software data-flow level only (the log is written by the generator path and read from a file); NOT independent of the generator process, "
                                      "the operating system or its clock"},
            "identity": {**ident.as_dict(), "workload_id": start.get("workload_id"), "channel_id": None if self.bridge is None else self.bridge.channel_id,
                         "recorder_instance": None, "log_round_index": start.get("round_index")},
            "log": {"schema": ilr.ISSUE_LOG_SCHEMA, "name": ident.log_path.name, "records": full_counts["records"], "bytes_consumed": tail.offset,
                    "prefix_sha256": _prefix_sha256(ident.log_path, tail.offset), "chain_head_sha256": tail.chain_head if full_counts["records"] else None,
                    "head_record_sha256": tail._head, "writer_state": writer["state"], "partial_tail_bytes": tail.partial_tail_bytes},
            "checkpoint": self._checkpoint_identity(), "descriptor": self.descriptor,
            "time": {"event_time": "generator_issue_time (attempt t_ms, the worker's Date.now())", "read_time": "the reader's observation time of the poll that consumed each record",
                     "first_event_utc": _iso(attempts[0]["ts_ms"]) if attempts else None, "last_event_utc": _iso(attempts[-1]["ts_ms"]) if attempts else None,
                     "first_read_utc": _iso(self.first_read_ms), "read_watermark_utc": _iso(self.last_read_ms), "decision_cutoff_utc": _iso(as_of_ms),
                     "event_time_never_replaced_by_read_time": True, "reads": len(self.reads), "restarts": self.restarts},
            "cutoff": {"records_read_after_cutoff_excluded": excluded, "rule": "a record not yet read at the decision time does not exist for that decision"},
            "interval": {"alignment": "whole second inside an observed liveness segment", "intervals": intervals},
            "counts": counts,
            "liveness": {"segments": [[_iso(s), _iso(e)] for s, e in segments], "max_tick_gap_ms": self.policy.max_tick_gap_ms,
                         "gaps": len(tail.gaps), "last_liveness_point_utc": _iso(tail.last_point_ms)},
            "journal": None if self.journal_path is None else {"path_name": self.journal_path.name, "instance": self.bridge.instance if self.bridge else None},
            "reconciliation": {"recorder_route": None, "ledger_count_level": None, "join_policy": "counts only; a timestamp is never used to match records"},
            "timing": {"parameters_approved": False, "clock_offset_verdict": None,
                       "note": "the reader's clock and the generator's clock are different domains unless declared otherwise; no agreement was measured"},
            "limitations": ["a generator log does not prove external Ethereum mempool completeness", "a logged intent is not a successful Fabric submission",
                            "a submission is not a ledger commit", "a quiet or missing log is not zero issuance", "no live run produced this evidence",
                            "the poll cadence and max_tick_gap_ms are caller-supplied placeholders; none is approved"],
        }

    def _checkpoint_identity(self) -> dict[str, Any] | None:
        path = self.identity.checkpoint_path
        if path is None:
            return None
        doc = None
        if Path(path).is_file():
            try:
                doc = json.loads(Path(path).read_text(encoding="ascii"))
            except (OSError, ValueError):
                doc = None
        return {"name": Path(path).name, "present": doc is not None, "self_sha256": None if doc is None else doc.get("self_sha256"),
                "round_index": None if doc is None else doc.get("round_index"), "reader_instance": None if doc is None else doc.get("reader_instance"),
                "offset": None if doc is None else doc.get("offset"), "rseq": None if doc is None else doc.get("rseq")}

    def audit(self, **evidence_args: Any) -> dict[str, Any]:
        """The machine-readable audit: the evidence plus every read."""
        return {"schema": AUDIT_SCHEMA, "driver": DRIVER_VERSION, "evidence": self.evidence(**evidence_args), "reads": list(self.reads)}


# ------------------------------------------------------------------------------------------------------------------ modes
def inspect_log(identity: ReaderIdentity, *, read_time: Any, policy_limits: Mapping[str, Any], descriptor: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Validate a COMPLETED issue log read at one explicit read time. A log that was never closed is reported as an incomplete lifecycle (PARTIAL), never as complete."""
    reader = SourceCountReader(identity, policy_limits=policy_limits, descriptor=descriptor)
    reader.poll_until_drained(read_time)
    return reader.audit(require_closed=True)


def load_schedule(doc: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Validate a replay schedule: ``{"schema": REPLAY_SCHEMA, "steps": [{"read_time": ..., "reveal_records": k | null, "partial_bytes": b, "restart": bool}]}``. Read times
    must be aware and non-decreasing; ``reveal_records`` null reveals everything that remains."""
    if not isinstance(doc, Mapping) or doc.get("schema") != REPLAY_SCHEMA:
        raise _fail("schedule_schema", f"the schedule must be an object with schema {REPLAY_SCHEMA!r}")
    steps = doc.get("steps")
    if not isinstance(steps, list) or not steps:
        raise _fail("schedule_empty", "the schedule has no steps")
    out, previous = [], None
    for index, step in enumerate(steps):
        if not isinstance(step, Mapping) or "read_time" not in step:
            raise _fail("schedule_step_invalid", f"step {index} needs a read_time")
        read_ms = ilr.utc_ms(_to_dt(step["read_time"]), f"step {index} read_time")
        if previous is not None and read_ms < previous:
            raise _fail("schedule_time_regressed", f"step {index} read time moves backwards")
        previous = read_ms
        reveal, partial = step.get("reveal_records"), step.get("partial_bytes", 0)
        if reveal is not None and (isinstance(reveal, bool) or not isinstance(reveal, int) or reveal < 0):
            raise _fail("schedule_step_invalid", f"step {index} reveal_records must be a non-negative integer or null")
        if isinstance(partial, bool) or not isinstance(partial, int) or partial < 0:
            raise _fail("schedule_step_invalid", f"step {index} partial_bytes must be a non-negative integer")
        out.append({"read_ms": read_ms, "reveal_records": reveal, "partial_bytes": partial, "restart": bool(step.get("restart", False))})
    return out


def replay_log(identity: ReaderIdentity, *, source_log: Path, schedule: Sequence[Mapping[str, Any]], policy_limits: Mapping[str, Any],
               journal_path: Path | None = None, source_id: str | None = None, origin: str | None = None, channel_id: str | None = None,
               as_of: Any | None = None, descriptor: Mapping[str, Any] | None = None) -> tuple[SourceCountReader, dict[str, Any]]:
    """Reveal a STORED log to the reader step by step. ``identity.log_path`` is a work file created here (it must not exist); ``source_log`` is only read. Each step appends
    the next records (and, optionally, a torn fragment of the one after) to the work file, optionally restarts the tail from its checkpoint, then polls at the step's read time."""
    work = identity.log_path
    if work.exists():
        raise _fail("replay_work_file_exists", f"{work} exists: a replay writes a new work file")
    if _same(source_log, work):
        raise _fail("replay_source_is_work_file", "the replay work file must differ from the source log")
    try:
        lines = Path(source_log).read_bytes().split(b"\n")
    except OSError as exc:
        raise _fail("source_log_missing", f"{source_log}: {exc}") from exc
    complete = [line + b"\n" for line in lines[:-1]]
    if lines[-1]:
        raise _fail("source_log_partial_tail", "the stored source log ends with an unterminated record; a replay source must be complete")
    steps = load_schedule({"schema": REPLAY_SCHEMA, "steps": list(schedule)}) if not (schedule and "read_ms" in schedule[0]) else list(schedule)
    work.parent.mkdir(parents=True, exist_ok=True)
    work.write_bytes(b"")
    reader = SourceCountReader(identity, policy_limits=policy_limits, journal_path=journal_path, source_id=source_id, origin=origin, channel_id=channel_id, descriptor=descriptor)
    revealed, fragment = 0, b""
    for step in steps:
        take = len(complete) - revealed if step["reveal_records"] is None else min(step["reveal_records"], len(complete) - revealed)
        chunk = b"".join(complete[revealed:revealed + take])
        revealed += take
        fragment = b""
        if step["partial_bytes"] and revealed < len(complete):
            fragment = complete[revealed][:min(step["partial_bytes"], len(complete[revealed]) - 1)]
        with work.open("ab") as handle:
            handle.write(chunk)
            handle.write(fragment)
        if step["restart"]:
            reader.restart()
        reader.poll(step["read_ms"])
        if fragment:                                                      # the torn bytes are removed before the next reveal; the real writer finishes the record instead
            data = work.read_bytes()
            work.write_bytes(data[:len(data) - len(fragment)])
    return reader, reader.audit(as_of=as_of, require_closed=False)


def follow(*_args: Any, **_kwargs: Any) -> None:
    """The interface a LATER, separately authorized follow driver would implement: ``poll_once(read_time)`` on an injected clock and an injected sleep, a checkpoint after each
    productive poll, a stop condition, and the same evidence builder. It is not implemented: nothing here watches an active log, and no argument enables it."""
    raise _fail("live_follow_refused", "follow mode is disabled: it could read an active live experiment, which this task does not authorize. No flag or environment variable enables it.")


FOLLOW_INTERFACE = {
    "status": "DESIGNED BUT NOT IMPLEMENTED (refused)",
    "required_inputs": ["ReaderIdentity (run, round, worker, log, checkpoint)", "an injected clock returning aware datetimes", "an injected sleep",
                        "policy limits including an explicit max_tick_gap_ms", "a stop condition", "an explicit evidence origin"],
    "required_behaviour": ["checkpoint after every productive poll", "never poll with a clock earlier than the previous poll", "fail closed on every IssueLogError",
                           "emit source_count_evidence/1 at each decision cutoff, restricted to records already read"],
    "enabled": LIVE_FOLLOW_ENABLED,
}


# ------------------------------------------------------------------------------------------------------------------ Stage 3 hand-over
def build_captured_history(reader: SourceCountReader, *, utc_day: str, source_id: str, coverage_policy: CoveragePolicy, freshness_limit_s: float,
                           origin: str = gc.ORIGIN_SYNTHETIC, forecast_horizon_s: int = 60, min_history_bins: int = 600) -> "gc.GeneratorIssueCapturedHistory":
    """Coverage validation -> captured workload history, using the existing A0l classes. Needs a reader built WITH a journal bridge; the history is the generator-issuance
    history (``GeneratorIssueCapturedHistory``): arrivals are the generator's attempts at its own issue time, never recorder callbacks and never Ethereum arrivals."""
    if reader.bridge is None:
        raise _fail("journal_bridge_missing", "a captured history needs a reader built with a journal bridge (and therefore an explicit source id and origin)")
    coverage = gc.GeneratorIssueCoverage.from_bridge(reader.bridge, policy=coverage_policy)
    source = gc.GeneratorIssueSource(source_id=source_id, utc_day=utc_day, dataset_path="issue_log_replay", evidence_origin=origin)
    binned = gc.bin_issue_events(reader.tail.attempt_events(reader.bridge.instance), source)
    return gc.GeneratorIssueCapturedHistory(binned, coverage=coverage, freshness_limit_s=freshness_limit_s, forecast_horizon_s=forecast_horizon_s,
                                            min_history_bins=min_history_bins)
