"""Generator-side issuance history: issue log -> collector-health journal -> verified coverage -> captured history (task A0l, 2026-11-21).

Two routes use the same issue-log reader (``issue_log_reader``) and keep their claims apart:

* RECORDER ROUTE (``issue_log_reader.IssueLogEvidence``): the arrivals are the recorder's submit callbacks (callback time); the issue log is the
  independent source count that decides whether a zero second may be believed. Nothing in this module is involved.
* GENERATOR ROUTE (this module): the arrivals ARE the generator's ``attempt`` records, binned by the generator's own issue time. The collector-health
  journal is written by ``IssueLogJournalBridge`` from the log's liveness points, so a quiet second is a genuine zero only inside a span in which
  the generator was shown to be alive and logging (ticks no further apart than the policy allows). The journal declares the scope
  ``source_stream_observer`` AND the subject ``generator_issuance``: it supports claims about the EXPERIMENTAL GENERATOR's issuance and nothing
  about an external (Ethereum mempool) source, a recorder, Caliper, or any Fabric quantity.

Timing. The bridge writes each journal row at the READ time of the evidence it carries (never at the generator's event time), so a decision at
``as_of`` can only see what the reader had read by then. Coverage starts at the first whole second at or after the first read of the start
record; the seconds before it are "before the collector's recorded observation start" (unknown), because the journal contract refuses a
retroactive claim. A reader that begins late therefore loses the early seconds; it never certifies them.

Evidence origin. A fixture or scripted log is labelled ``synthetic_fixture`` in every provenance block and is never live quality.
``live_quality`` is false on every record this module produces; no real run has been read.

Pure standard library plus the sibling modules; files are only read, except that the bridge writes the journal file it is given.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import issue_log_reader as ilr
from .captured_history import SECONDS_PER_DAY, BinnedDay, CapturedHistory, CapturedHistoryError, CapturedSource, _day_start
from .channel_selection import select_experiment_channel
from .collector_health import CollectorHealthJournal
from .workload_coverage import (KIND_START, SCOPE_SOURCE_STREAM, CoverageClass, CoveragePolicy, CoverageSnapshot, CoverageEvidenceError,
                                VerifiedCoverage, iso, parse_utc, read_journal_file)

SOURCE_KIND_GENERATOR_ISSUE = "generator_issue_log_attempts"
COVERAGE_SUBJECT = "generator_issuance"
ORIGIN_SYNTHETIC = "synthetic_fixture"
ORIGIN_LIVE = "live_generator"
ORIGINS = (ORIGIN_LIVE, ORIGIN_SYNTHETIC)
REPRESENTS = ("submission attempts made by the experimental workload generator (the attempt record written immediately before sendRequests), at the "
              "generator's own issue time; NOT Ethereum mempool arrivals, NOT Caliper submit events, NOT ledger commits")
# reason codes (they explain a predictive-history abstention; they never change its category)
RC_GENERATOR_UNOBSERVED = ilr.RC_GENERATOR_UNOBSERVED
RC_INTEGRITY = "issue_log_integrity_failure"
RC_INTERRUPTION = "collector_interruption"
RC_GAP = "heartbeat_gap"
RC_STALE = "stale_collector_evidence"
RC_MISSING = "missing_observation"
RC_UNRESOLVED = ilr.RC_UNRESOLVED


# ------------------------------------------------------------------------------------------------------------------ source and binning
@dataclass(frozen=True)
class GeneratorIssueSource(CapturedSource):
    """Identity of a generator-issuance series. Never claims to be an Ethereum archive, a recorder series, a ledger or a real run unless the origin says so."""

    kind: str = SOURCE_KIND_GENERATOR_ISSUE
    schema: str = ilr.ISSUE_LOG_SCHEMA + " attempt records"
    timestamp_precision: str = "bins of 1 s built from millisecond generator issue (attempt) times"
    evidence_origin: str = "unspecified"

    def as_dict(self) -> dict[str, Any]:
        doc = super().as_dict()
        doc.update({"represents": REPRESENTS, "is_fabric_observation": False, "evidence_origin": self.evidence_origin,
                    "synthetic": self.evidence_origin == ORIGIN_SYNTHETIC, "event_time": ilr.EVENT_TIME_SEMANTICS,
                    "coverage_subject": COVERAGE_SUBJECT, "external_mempool_capture": False,
                    "unit": "submission attempts per 1 s bin (= attempts/s)"})
        return doc


def bin_issue_events(events: Sequence[Mapping[str, Any]], source: CapturedSource) -> BinnedDay:
    """Aggregate the issuance events of ONE UTC day into 1 s counts by GENERATOR ISSUE time. A second without an event is 0 HERE: whether that zero may
    be believed is decided by the coverage evidence, never by this function. Events outside the day are counted in the report, not binned."""
    start_ms = int(_day_start(source.utc_day).timestamp() * 1000)
    counts = [0] * SECONDS_PER_DAY
    seen: set[str] = set()
    duplicates = outside = units = 0
    for event in events:
        key = str(event["id"])
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        offset = int(event["ts_ms"]) - start_ms
        if offset < 0 or offset >= SECONDS_PER_DAY * 1000:
            outside += 1
            continue
        counts[offset // 1000] += 1
        units += 1
    report = {"input": "issue-log attempt records", "records_read": len(seen) + duplicates, "unique_events": len(seen), "units_binned": units,
              "duplicates_dropped": duplicates, "events_outside_utc_day": outside,
              "duplicate_handling": "by issue id (run:round:n); a repeat is dropped and counted (the reader already refuses a repeated n)",
              "rows_without_series_row": 0, "zero_count_seconds": sum(1 for c in counts if c == 0), "bins": SECONDS_PER_DAY,
              "event_time": "generator issue time (attempt t_ms), never a callback or completion time",
              "zero_semantics": "a second with no attempt is 0 here; it is believed only inside generator-observed coverage"}
    return BinnedDay(source, tuple(counts), report)


# ------------------------------------------------------------------------------------------------------------------ journal bridge
class GeneratorIssueJournal(CollectorHealthJournal):
    """A collector-health journal whose start row also names the coverage SUBJECT. The extra fields avoid the recorder fingerprint, so the journal is
    never mistaken for a recorder's."""

    def start_generator(self, *, observation_start_utc: Any, issue_log: Mapping[str, Any], origin: str) -> dict[str, Any]:
        if self._started:
            raise CoverageEvidenceError("collector_already_started", "start was already written by this instance")
        if origin not in ORIGINS:
            raise CoverageEvidenceError("evidence_origin_invalid", f"issue_log_origin {origin!r} is not one of {ORIGINS}")
        now = self._clock()
        start = parse_utc(observation_start_utc, "observation_start_utc")
        if start.microsecond or start < parse_utc(now, "clock"):
            raise CoverageEvidenceError("retroactive_coverage_claim", "observation must start on a whole second at or after the start record is written")
        row = self._append(KIND_START, observation_start_utc=iso(start), timestamp_provenance="reader_read_time_utc", first_event_seq=0,
                           coverage_scope=SCOPE_SOURCE_STREAM, coverage_subject=COVERAGE_SUBJECT, issue_log_origin=origin, issue_log=dict(issue_log),
                           source_count_declared=False)
        self._started = True
        self._last_watermark = start
        return row


def _ceil_second(ms: int) -> datetime:
    return ilr.ms_to_utc(-(-ms // 1000) * 1000)


def _floor_second(ms: int) -> datetime:
    return ilr.ms_to_utc((ms // 1000) * 1000)


class IssueLogJournalBridge:
    """Turns what an ``IssueLogTail`` has read into collector-health rows. Call ``sync(read_time)`` after each ``tail.poll(read_time)``.

    Rules (each is fail-closed):
    * nothing is written before the start record has been read;
    * a heartbeat watermark is the whole second at or below the last liveness point of an observed segment, and never above the read time;
    * two liveness points further apart than the tail's ``max_tick_gap_ms`` are a gap: an ``error`` row (uncovered) and a ``recovered`` row at the first
      whole second at or after both the read time and the next segment's start;
    * an invalid log writes one ``error`` row and nothing more; the coverage object additionally invalidates the attestation from that read time on;
    * a clean ``stop`` is written only when the log's own stop record closes it; anything else is an aborted stop.
    """

    def __init__(self, tail: ilr.IssueLogTail, journal_path: Path, *, source_id: str, origin: str, channel_id: str | None = None):
        if origin not in ORIGINS:
            raise CoverageEvidenceError("evidence_origin_invalid", f"origin must be one of {ORIGINS}")
        if channel_id is not None:                      # a protected reference channel or an earlier experiment's channel is refused before any file exists
            channel_id = select_experiment_channel(channel_id, purpose="generator issuance coverage", option="channel_id")
        self.tail, self.path, self.source_id, self.origin, self.channel_id = tail, Path(journal_path), source_id, origin, channel_id
        self._now: datetime | None = None
        self._journal: GeneratorIssueJournal | None = None
        self._seg_done = 0
        self._last_end_ms: int | None = None
        self._wm: datetime | None = None
        self._origin_dt: datetime | None = None
        self._pending_recovery: int | None = None           # start ms of the segment that follows a gap
        self._error_written = False
        self._stopped = False
        self.instance = f"issue-log-reader:{tail.policy.run_id}:{tail.policy.worker_id}:r0"

    # the journal's clock is the READ time of the sync that writes the row
    def _clock(self) -> datetime:
        assert self._now is not None
        return self._now

    def _count(self, watermark: datetime) -> int:
        stamps = self.tail._attempt_ms
        lo = bisect.bisect_left(stamps, ilr.utc_ms(self._origin_dt))
        hi = bisect.bisect_left(stamps, ilr.utc_ms(watermark))
        return hi - lo

    def sync(self, read_time: datetime) -> dict[str, Any]:
        self._now = read_time
        tail = self.tail
        report: dict[str, Any] = {"rows": [], "held_back": None, "state": None}
        if tail.start_record is None or self._stopped:
            report["state"] = "not_started" if tail.start_record is None else "stopped"
            return report
        now_ms = ilr.utc_ms(read_time)
        if self._journal is None:
            start_ms = int(tail.start_record["t_ms"])
            origin_dt = max(_ceil_second(start_ms), (_floor_second(now_ms) + timedelta(seconds=1)))
            self._journal = GeneratorIssueJournal(self.path, source_id=self.source_id, collector_id="issue-log-reader", run_id=tail.policy.run_id,
                                                  channel_id=self.channel_id, clock=self._clock, collector_instance=self.instance)
            row = self._journal.start_generator(observation_start_utc=origin_dt, origin=self.origin, issue_log={
                "schema": ilr.ISSUE_LOG_SCHEMA, "run_id": tail.policy.run_id, "worker_id": tail.policy.worker_id, "clock_domain": tail.policy.clock_domain,
                "worker_roster": list(tail.policy.expected_roster), "workload_id": tail.start_record.get("workload_id"),
                "head_record_sha256": tail._head, "descriptor": ilr.issue_log_descriptor(tail.policy.run_id, tail.policy.worker_id)})
            self._origin_dt, self._wm = origin_dt, origin_dt
            report["rows"].append(row["kind"])
        journal = self._journal
        if tail.error is not None:
            if not self._error_written:
                journal.error("issue_log_invalid", tail.error.code)
                self._error_written = True
                report["rows"].append("error")
            report["state"] = "invalid"
            return report
        candidate: datetime | None = None
        segments = tail.liveness_points_since(self._seg_done)
        for index, (s_ms, e_ms, _rd) in enumerate(segments):
            floor = _floor_second(e_ms)
            if floor > read_time:                                # the generator clock is ahead of the reader's: hold the rest back, claim nothing
                report["held_back"] = "generator_clock_ahead_of_reader"
                break
            if self._last_end_ms is not None and s_ms != self._last_end_ms:        # an unobserved gap between two liveness points
                self._flush(candidate, report)
                candidate = None
                if not journal._open_error:
                    journal.error("generator_observation_gap", "no liveness point within the allowed gap", affects_from_utc=self._wm)
                    report["rows"].append("error")
                self._pending_recovery = s_ms
            if self._pending_recovery is not None and journal._open_error:
                resumes = max(_ceil_second(self._pending_recovery), _floor_second(now_ms) + timedelta(seconds=1))
                journal.recovered(resumes)
                self._wm = resumes
                self._pending_recovery = None
                report["rows"].append("recovered")
            self._last_end_ms = e_ms
            self._seg_done += 1
            if floor >= self._wm:
                candidate = floor
        stop = tail.stop_record
        if stop is not None and self._seg_done == tail.segment_count and report["held_back"] is None:
            final = candidate if candidate is not None else self._wm
            if final is not None and final >= self._wm and not journal._open_error:
                clean = tail.writer_state()["state"] == ilr.W_CLOSED_CLEAN
                journal.stop(final, events_received=self._count(final), reason="clean" if clean else "abort")
                self._wm, self._stopped = final, True
                report["rows"].append("stop")
                report["state"] = "stopped"
                return report
        self._flush(candidate, report)
        report["state"] = "observing"
        return report

    def _flush(self, candidate: datetime | None, report: dict[str, Any]) -> None:
        journal = self._journal
        if candidate is None or journal is None or journal._open_error or candidate <= self._wm:
            return
        journal.heartbeat(candidate, events_received=self._count(candidate))
        self._wm = candidate
        report["rows"].append("heartbeat")


# ------------------------------------------------------------------------------------------------------------------ coverage
class GeneratorIssueCoverage(VerifiedCoverage):
    """``VerifiedCoverage`` over the bridge's journal. It adds one rule: once the tail has recorded an integrity failure, every snapshot at or after the
    time that failure was READ is INVALID (the whole attestation), whatever the journal rows say."""

    def __init__(self, *, tail: ilr.IssueLogTail, events: Sequence[Mapping[str, Any]], policy: CoveragePolicy, source_id: str, run_id: str,
                 observed_events: Sequence[Mapping[str, Any]] | None = None, channel_id: str | None = None, journal_sha256: str | None = None):
        super().__init__(source_id=source_id, mode="strict_attested", events=events, policy=policy, observed_events=observed_events, run_id=run_id,
                         channel_id=channel_id, journal_sha256=journal_sha256)
        self.tail = tail

    @classmethod
    def from_bridge(cls, bridge: IssueLogJournalBridge, *, policy: CoveragePolicy, instance_events: bool = True) -> "GeneratorIssueCoverage":
        if not bridge.path.is_file():
            raise CoverageEvidenceError("journal_missing", "the bridge has written no journal (no start record has been read): there is no coverage evidence")
        events, _torn, digest = read_journal_file(bridge.path)
        observed = bridge.tail.attempt_events(bridge.instance) if instance_events else None
        return cls(tail=bridge.tail, events=events, policy=policy, source_id=bridge.source_id, run_id=bridge.tail.policy.run_id, observed_events=observed,
                   channel_id=bridge.channel_id, journal_sha256=digest)

    def snapshot(self, as_of: Any) -> CoverageSnapshot:
        base = super().snapshot(as_of)
        error = self.tail.error
        if error is not None and base.valid and self.tail.error_read_ms is not None and ilr.utc_ms(base.as_of, "as_of") >= self.tail.error_read_ms:
            return CoverageSnapshot(base.mode, base.source_id, base.as_of, None, error.code, str(error), (), None, self.journal_sha256)
        return base


class GeneratorIssueCapturedHistory(CapturedHistory):
    """``CapturedHistory`` over generator issuance. Same ``ArrivalHistoryStore``, same strict gap policy, same Stage 3 hand-over, different claim:
    arrivals are the generator's own submission attempts, not Ethereum arrivals and not a Fabric measurement."""

    def __init__(self, binned: BinnedDay, *, coverage: GeneratorIssueCoverage, freshness_limit_s: float = 5.0, forecast_horizon_s: int = 60,
                 min_history_bins: int = 600):
        if not isinstance(coverage, GeneratorIssueCoverage):
            raise CapturedHistoryError("coverage_type_invalid", "a generator-issuance history needs GeneratorIssueCoverage")
        if not isinstance(binned.source, GeneratorIssueSource):
            raise CapturedHistoryError("source_type_invalid", "a generator-issuance history needs a GeneratorIssueSource")
        if coverage.declared_scopes() != {SCOPE_SOURCE_STREAM}:
            raise CapturedHistoryError("coverage_scope_source_mismatch",
                                       f"generator arrivals need coverage declaring scope {SCOPE_SOURCE_STREAM!r}, found {sorted(map(str, coverage.declared_scopes()))}")
        subjects = {ev.get("coverage_subject") for ev in coverage.events if ev.get("kind") == KIND_START}
        if subjects != {COVERAGE_SUBJECT}:
            raise CapturedHistoryError("coverage_subject_mismatch",
                                       f"generator arrivals need coverage whose subject is {COVERAGE_SUBJECT!r}, found {sorted(map(str, subjects))}")
        super().__init__(binned, coverage=coverage, max_age_s=freshness_limit_s, forecast_horizon_s=forecast_horizon_s, min_history_bins=min_history_bins)
        self.freshness_limit_s = freshness_limit_s

    def coverage_provenance(self, decision_cutoff_utc: Any) -> dict[str, Any]:
        block = super().coverage_provenance(decision_cutoff_utc)
        cutoff = parse_utc(decision_cutoff_utc, "decision cutoff")
        snap = self.coverage.snapshot(cutoff)
        tail = self.coverage.tail
        codes: set[str] = set()
        counts = self.coverage_counts
        if not snap.valid:
            codes.add(RC_INTEGRITY)
        if counts[CoverageClass.UNKNOWN_COVERAGE_ZERO.value]:
            codes.add(RC_GENERATOR_UNOBSERVED)
        if counts[CoverageClass.COVERAGE_GAP.value]:
            codes.add(RC_GAP)
        if counts[CoverageClass.INTERRUPTED_COLLECTION.value]:
            codes.add(RC_INTERRUPTION)
        if counts[CoverageClass.STALE_COLLECTOR_EVIDENCE.value] or self._deferred is not None:
            codes.add(RC_STALE)
        if counts[CoverageClass.MISSING_OBSERVATION.value]:
            codes.add(RC_MISSING)
        origins = {ev.get("issue_log_origin") for ev in self.coverage.events if ev.get("kind") == KIND_START}
        read_by_cutoff = len(tail.attempt_events("x", as_of_ms=ilr.utc_ms(cutoff, "cutoff")))
        block.update({
            "abstention_reason_codes": sorted(codes), "coverage_subject": COVERAGE_SUBJECT, "external_mempool_coverage": False,
            "live_quality": False, "synthetic_evidence": ORIGIN_SYNTHETIC in origins or not origins, "evidence_origins": sorted(map(str, origins)),
            "freshness_limit_s": self.freshness_limit_s,
            "issue_log": {"schema": ilr.ISSUE_LOG_SCHEMA, "run_id": tail.policy.run_id, "worker_id": tail.policy.worker_id,
                          "clock_domain": tail.policy.clock_domain, "worker_roster": list(tail.policy.expected_roster),
                          "descriptor": ilr.issue_log_descriptor(tail.policy.run_id, tail.policy.worker_id),
                          "event_time": ilr.EVENT_TIME_SEMANTICS, "attempts_read_by_cutoff": read_by_cutoff,
                          "independent_of_recorder": "DECLARED by data flow (the log is written by the generator path and read from a file); never proven"},
            "claim": ("a verified zero here means the EXPERIMENTAL GENERATOR was shown to be alive and logging and issued nothing in that second; it says "
                      "nothing about an external Ethereum mempool source, Caliper, or Fabric"),
            "note": "generator-issuance evidence; no real Caliper or Fabric run has produced it (live_source_coverage_verified is false)"})
        block["live_source_coverage_verified"] = False
        return block

    def evidence(self, decision_cutoff_utc: Any) -> dict[str, Any]:
        doc = super().evidence(decision_cutoff_utc)
        doc["note"] = ("generator-side submission attempts at the generator's issue time; not Ethereum arrivals, not Caliper submit events, not ledger commits; "
                       "a valid history is an input contract check, not forecast skill")
        return doc
