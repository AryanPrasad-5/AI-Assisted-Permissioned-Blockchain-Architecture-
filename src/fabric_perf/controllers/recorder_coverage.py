"""Recorder-derived workload coverage (task A0j, 2026-11-21): when may a zero-arrival second from the arrival RECORDER be believed?

This module began (A0j) as a layer ON TOP of ``workload_coverage``. A0k found that the generic ``VerifiedCoverage`` ignored the journal's
``coverage_scope`` and would certify a heartbeat-only zero, so the scope rule now lives in ``workload_coverage`` itself and EVERY coverage path
obeys it; the source-count verification moved to ``source_completeness``. What stays here is what is specific to the recorder route:

1. Three kinds of coverage are kept apart
   * RECORDER OPERATIONAL COVERAGE: the recorder ran and wrote healthy heartbeats (``workload_coverage`` verifies this from the journal).
   * RECORDER CALLBACK COVERAGE: its TxSubmitted callback fed it events that reached the live submits file before each heartbeat. Checked
     here: every heartbeat's cumulative count must equal the units in the submit file (a mismatch makes the attestation CONTRADICTORY).
   * VERIFIED SOURCE-EVENT COVERAGE: an INDEPENDENT source count agrees with the recorder's count over the span. Only this can certify a
     zero. A5 (A0k): the counter must also carry a ``source_counter`` descriptor that classifies as SOURCE-INDEPENDENT AND
     INTERVAL-SUITABLE (``source_completeness.classify_descriptor``); a bare ``source_independence_claim`` is not enough. Independence is
     still a declaration that cannot be verified here, so ``live_source_coverage_verified`` is ALWAYS false.
2. A zero second inside recorder coverage but outside verified source coverage is ``zero_count_unknown_coverage`` with the cause
   ``recorder_coverage_only``; it is never stored as 0 and the predictive history stays incomplete from that second on.
   A positive second inside recorder coverage is still ``observed_positive`` (a count above zero shows the callback was fed, although it can
   under-count if callbacks were missed; that residual risk is stated in provenance).
3. Submit events are binned by their SUBMIT observation time. Completion times (``final_ms``) are never converted into arrival times.

What this module does not do: it verifies nothing about a real Caliper run, a real clock or a real source counter. Evidence of origin
``synthetic_fixture`` is labelled synthetic in every provenance block and can never be live quality.

No network, process, Fabric, Docker, model or forecaster is used. Files are only read.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from .captured_history import (SECONDS_PER_DAY, BinnedDay, CapturedHistory, CapturedHistoryError, CapturedSource, _day_start)
from .recorder_health_bridge import COVERAGE_SCOPE_RECORDER, EVIDENCE_ORIGINS, ORIGIN_SYNTHETIC, BridgeError, RecorderHealthPolicy
from .workload_coverage import (KIND_HEARTBEAT, KIND_START, KIND_STOP, MODE_ATTESTED, RECORDER_ONLY_CAUSE, CoverageClass, CoverageEvidenceError,
                                CoveragePolicy, CoverageSnapshot, VerifiedCoverage, iso, parse_utc, read_journal_file)

RECORDER_PROVENANCE_SCHEMA = "recorder_coverage_provenance/1"
SUBMIT_COLUMNS = ("seq_submit", "t_submit_evt_ms", "count")
REPRESENTS = ("submit-event observation times (Caliper TxSubmitted callback) seen by one recorder worker; client-side arrivals, NOT "
              "ledger commits, endorsements or completion times")
HASH_FAILURE_CODES = frozenset({"hash_chain_broken", "hash_mismatch", "event_sequence_broken"})

# Canonical reason codes for coverage-related predictive abstentions. They EXPLAIN a predictive_history_abstention; they do not
# change the abstention category and never turn a rejected frame into an accepted one.
RC_HASH_CHAIN = "hash_chain_integrity_failure"
RC_CONTRADICTORY = "contradictory_collector_evidence"
RC_HEALTH_FAILURE = "collector_health_failure"
RC_INTERRUPTION = "collector_interruption"
RC_RESTART_GAP = "collector_restart_gap"
RC_HEARTBEAT_GAP = "heartbeat_gap"
RC_STALE = "stale_collector_evidence"
RC_MISSING_CALLBACKS = "missing_callbacks_detected"
RC_NO_SOURCE_EVIDENCE = "missing_source_completeness_evidence"
RC_ZERO_OUTSIDE_WINDOW = "zero_outside_observation_window"
RC_MISSING_OBSERVATION = "missing_observation"


# ----------------------------------------------------------------------------------------------------------- submit events
@dataclass(frozen=True)
class RecorderArrivalSource(CapturedSource):
    """Identity of a recorder-derived arrival series. Never claims to be an Ethereum archive, a ledger or a real run unless the origin says so."""

    kind: str = "caliper_client_submit_events"
    schema: str = "e1_per_tx_live/1 live.submits.csv"
    timestamp_precision: str = "bins of 1 s built from millisecond TxSubmitted observation times"
    evidence_origin: str = "unspecified"

    def as_dict(self) -> dict[str, Any]:
        doc = super().as_dict()
        doc.update({"represents": REPRESENTS, "is_fabric_observation": False, "evidence_origin": self.evidence_origin,
                    "synthetic": self.evidence_origin == ORIGIN_SYNTHETIC,
                    "unit": "transactions submitted per 1 s bin (= transactions/s)"})
        return doc


def read_submit_events(path: Path, *, instance: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read ``live.submits.csv`` (or the final ``submits.csv``) of one recorder instance.

    A last line without a newline is a torn write and is ignored. A repeated ``seq_submit`` that is identical is dropped and counted; one
    that differs is refused. Rows are validated, never repaired. The event time is the SUBMIT observation time ``t_submit_evt_ms``.
    """
    raw = Path(path).read_bytes().decode("utf-8")
    lines = raw.split("\n")
    torn = bool(lines[-1])
    complete = lines[:-1]
    if not complete or complete[0].strip() != ",".join(SUBMIT_COLUMNS):
        raise BridgeError("submits_schema_mismatch", f"{Path(path).name} must start with the header {','.join(SUBMIT_COLUMNS)}")
    events: dict[int, dict[str, Any]] = {}
    duplicates = 0
    for number, line in enumerate(complete[1:], 2):
        parts = line.strip().split(",")
        try:
            if len(parts) != 3:
                raise ValueError
            seq, ts, count = (int(p) for p in parts)
        except ValueError as exc:
            raise BridgeError("submits_row_malformed", f"{Path(path).name} line {number} is not three integers") from exc
        if seq < 0 or ts <= 0 or count < 1:
            raise BridgeError("submits_row_invalid", f"{Path(path).name} line {number}: seq >= 0, a positive epoch ms and a count >= 1 are required")
        event = {"id": f"{instance}#sub{seq}", "seq": seq, "ts_ms": ts, "units": count, "instance": instance}
        if seq in events:
            if events[seq] != event:
                raise BridgeError("submits_row_conflict", f"{Path(path).name}: seq_submit {seq} appears twice with different content")
            duplicates += 1
            continue
        events[seq] = event
    ordered = [events[k] for k in sorted(events)]
    return ordered, {"file": Path(path).name, "rows": len(ordered), "units": sum(e["units"] for e in ordered), "duplicates_dropped": duplicates,
                     "torn_tail_ignored": torn, "event_time": "t_submit_evt_ms: when the recorder observed TxSubmitted (submit time, not completion time)"}


def bin_submit_events(events: Sequence[Mapping[str, Any]], source: CapturedSource) -> BinnedDay:
    """Aggregate submit events of ONE UTC day into 1 s unit counts. A second without an event is 0 HERE: whether that zero may be believed
    is decided by the coverage evidence, never by this function. Events outside the day are counted in the report, not binned."""
    start_ms = int(_day_start(source.utc_day).timestamp() * 1000)
    counts = [0] * SECONDS_PER_DAY
    outside = units = 0
    seen: set[str] = set()
    duplicates = 0
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
        counts[offset // 1000] += int(event["units"])
        units += int(event["units"])
    report = {"input": "recorder submit events", "records_read": len(seen) + duplicates, "unique_events": len(seen), "units_binned": units,
              "duplicates_dropped": duplicates, "events_outside_utc_day": outside,
              "duplicate_handling": "by instance and seq_submit: an identical repeat is dropped and counted; a differing repeat is refused",
              "rows_without_series_row": 0, "zero_count_seconds": sum(1 for c in counts if c == 0), "bins": SECONDS_PER_DAY,
              "zero_semantics": "a second with no submit event is 0 here; it is believed only inside verified source coverage"}
    return BinnedDay(source, tuple(counts), report)


# ------------------------------------------------------------------------------------------------------------------ coverage
def _merge(spans: list) -> list:
    out: list = []
    for start, end in sorted(spans):
        if end <= start:
            continue
        if out and start <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], end))
        else:
            out.append((start, end))
    return out


def _inside(spans: Sequence, start: datetime, end: datetime) -> bool:
    index = bisect.bisect_right([s for s, _ in spans], start) - 1
    return index >= 0 and end <= spans[index][1]


def _contradiction(code: str, message: str) -> CoverageEvidenceError:
    return CoverageEvidenceError(code, message)


@dataclass(frozen=True)
class RecorderCoverageSnapshot(CoverageSnapshot):
    """``CoverageSnapshot`` plus the recorder's own provenance. The zero rule (source evidence, not recorder health) is in the base class."""

    origins: tuple = ()
    scopes: tuple = ()
    source_basis_claims: tuple = ()
    independence_claims: tuple = ()
    submit_units_visible: int = 0

    def recorder_only_zero_seconds(self, counts: Sequence, day_start: datetime, limit_index: int) -> int:
        """Zero-count seconds below ``limit_index`` that lie in recorder coverage but outside verified source coverage."""
        if self.derivation is None:
            return 0
        total = 0
        for start, end in self.derivation.covered:
            lo = max(int((start - day_start).total_seconds()), 0)
            hi = min(int((end - day_start).total_seconds()), limit_index, SECONDS_PER_DAY)
            for index in range(lo, hi):
                moment = day_start + timedelta(seconds=index)
                if counts[index] == 0 and not _inside(self.source_verified, moment, moment + timedelta(seconds=1)):
                    total += 1
        return total

    def cause_codes(self) -> list[str]:
        d = self.derivation
        codes: set[str] = set()
        if not self.valid:
            codes.add(RC_HASH_CHAIN if self.failure_code in HASH_FAILURE_CODES else RC_CONTRADICTORY)
            return sorted(codes)
        if d is None:
            return sorted(codes)
        for _s, _e, cls, cause in (*d.uncovered, *self.extra_uncovered):
            if cause.startswith("source_count_mismatch"):
                codes.add(RC_MISSING_CALLBACKS)
            elif cause.startswith("collector_error:") or cause.startswith("collector_reported_"):
                codes.add(RC_HEALTH_FAILURE)
            elif cause == "restart_boundary":
                codes.add(RC_RESTART_GAP)
            elif cause == "heartbeat_gap":
                codes.add(RC_HEARTBEAT_GAP)
            elif cls == CoverageClass.INTERRUPTED_COLLECTION:
                codes.add(RC_INTERRUPTION)
        # a source count WAS declared but cannot certify (A0k): say why (recorder-derived, round-only, independence unproven, ...). "Not declared" stays
        # the plain missing_source_completeness_evidence added by the history.
        codes.update(c for c in (self.source_info or {}).get("rejection_codes", ()) if c != "source_counter_not_declared")
        return sorted(codes)

    def summary(self) -> dict[str, Any]:
        doc = super().summary()
        verified_seconds = int(sum((e - s).total_seconds() for s, e in self.source_verified))
        doc["recorder_evidence"] = {
            "schema": RECORDER_PROVENANCE_SCHEMA, "coverage_scope": list(self.scopes), "evidence_origins": list(self.origins),
            "synthetic": ORIGIN_SYNTHETIC in self.origins or not self.origins,
            "submit_units_visible": self.submit_units_visible,
            "operational_coverage": "verified from the hash-chained journal (workload_coverage)",
            "callback_coverage": "each heartbeat's cumulative count equals the units in the live submits file, or the attestation is invalid",
            "source_completeness": {"verified_seconds": verified_seconds, "basis_claimed_by_collector": list(self.source_basis_claims),
                                    "independence": "DECLARED by the collector (claims: " + ", ".join(map(str, self.independence_claims or ("none",)))
                                                    + "); this module cannot verify it"},
            "live_source_coverage_verified": False,
            "unverified_reason": ("no independent source-completeness evidence has been verified against a real Caliper/Fabric run; "
                                  "synthetic evidence is never live coverage"),
            "positive_second_caveat": "a positive count can still under-count if callbacks were missed"}
        return doc


class RecorderVerifiedCoverage(VerifiedCoverage):
    """``VerifiedCoverage`` for a recorder journal plus the recorder's own submit events (both required)."""

    def __init__(self, *, source_id: str, events: Sequence[Mapping[str, Any]], submit_events: Sequence[Mapping[str, Any]],
                 policy: CoveragePolicy | None = None, run_id: str | None = None, channel_id: str | None = None,
                 journal_sha256: str | None = None, torn_tail: bool = False, source_evidence: Any = None):
        super().__init__(source_id=source_id, mode=MODE_ATTESTED, events=events, policy=policy, observed_events=None, run_id=run_id,
                         channel_id=channel_id, journal_sha256=journal_sha256, torn_tail=torn_tail, source_evidence=source_evidence)
        for event in submit_events:
            if not {"id", "ts_ms", "units", "instance"} <= set(event) or isinstance(event["ts_ms"], bool) or not isinstance(event["ts_ms"], int) \
                    or isinstance(event["units"], bool) or not isinstance(event["units"], int) or event["units"] < 1:
                raise CoverageEvidenceError("event_record_invalid", "every submit event needs id, integer ts_ms, instance and integer units >= 1")
        unique: dict[str, Mapping[str, Any]] = {}
        for event in submit_events:
            unique.setdefault(str(event["id"]), dict(event))
        self.submit_events = tuple(unique.values())

    @classmethod
    def from_recorder_journal(cls, journal_path: Path, *, source_id: str, submit_events: Sequence[Mapping[str, Any]],
                              policy: RecorderHealthPolicy, run_id: str | None = None, channel_id: str | None = None,
                              source_evidence: Any = None) -> "RecorderVerifiedCoverage":
        events, torn, digest = read_journal_file(Path(journal_path))
        return cls(source_id=source_id, events=events, submit_events=submit_events, policy=policy.to_coverage_policy(), run_id=run_id,
                   channel_id=channel_id, journal_sha256=digest, torn_tail=torn, source_evidence=source_evidence)

    def snapshot(self, as_of: Any) -> RecorderCoverageSnapshot:
        base = super().snapshot(as_of)
        moment = base.as_of
        common = {f.name: getattr(base, f.name) for f in fields(CoverageSnapshot)}
        if not base.valid or base.derivation is None:
            return RecorderCoverageSnapshot(**common)
        try:
            origins, scopes, units = self._analyse(base, moment)
        except CoverageEvidenceError as exc:
            common.update(derivation=None, failure_code=exc.code, failure_reason=str(exc), extra_uncovered=(), crosscheck=None, scope=None,
                          source_verified=(), source_info=None)
            return RecorderCoverageSnapshot(**common)
        info = base.source_info or {}
        return RecorderCoverageSnapshot(**common, origins=tuple(origins), scopes=tuple(scopes), source_basis_claims=tuple(info.get("basis_claimed", ())),
                                        independence_claims=tuple(info.get("independence_claims", ())), submit_units_visible=units)

    def _analyse(self, base: CoverageSnapshot, as_of: datetime):
        """Walk the journal rows visible at ``as_of`` (already verified by ``derive``) and apply the recorder-specific checks: the declared scope,
        the evidence origin, and callback coverage (each heartbeat's cumulative count equals the units in the live submits file). The source
        counts themselves are verified by ``source_completeness`` inside ``VerifiedCoverage.snapshot``."""
        d = base.derivation
        as_of_ms = int(as_of.timestamp() * 1000)
        visible = [e for e in self.submit_events if e["ts_ms"] <= as_of_ms]
        per_instance: dict[str, list] = {}
        for event in visible:
            per_instance.setdefault(str(event["instance"]), []).append(event)
        for instance in per_instance:
            if instance not in d.instances:
                raise _contradiction("event_instance_unknown", f"submit events name instance {instance!r}, which has no start record")
        origins: set[str] = set()
        scopes: set[str] = set()
        state: dict[str, Any] = {"inst": None, "origin_ms": None}
        for index, ev in enumerate(self.events, 1):
            if parse_utc(ev["t_utc"], "t_utc") > as_of:
                break
            kind, instance = ev["kind"], ev["collector_instance"]
            if kind == KIND_START:
                if ev.get("coverage_scope") != COVERAGE_SCOPE_RECORDER:
                    raise _contradiction("coverage_scope_invalid", f"journal record {index}: a recorder journal must declare coverage_scope={COVERAGE_SCOPE_RECORDER!r}")
                origin = ev.get("evidence_origin")
                if origin not in EVIDENCE_ORIGINS:
                    raise _contradiction("evidence_origin_invalid", f"journal record {index}: evidence_origin {origin!r} is not one of {EVIDENCE_ORIGINS}")
                origins.add(origin)
                scopes.add(ev["coverage_scope"])
                state.update(inst=instance, origin_ms=int(d.instances[instance]["observation_start"].timestamp() * 1000))
            elif kind in (KIND_HEARTBEAT, KIND_STOP):
                watermark = parse_utc(ev["observed_through_utc"], "observed_through_utc")
                received = ev["events_received"]
                wm_ms = int(watermark.timestamp() * 1000)
                expected_units = sum(e["units"] for e in per_instance.get(instance, ()) if state["origin_ms"] <= e["ts_ms"] < wm_ms) \
                    if state["inst"] == instance else None
                if expected_units is not None and received != expected_units:
                    raise _contradiction("heartbeat_event_count_mismatch",
                                         f"journal record {index}: the heartbeat reports {received} submitted transaction(s) through {iso(watermark)}, "
                                         f"the live submits file holds {expected_units}")
        return sorted(origins), sorted(scopes), sum(e["units"] for e in visible)


# --------------------------------------------------------------------------------------------------------------- history
class RecorderCapturedHistory(CapturedHistory):
    """``CapturedHistory`` over a recorder-derived day. Same ``ArrivalHistoryStore``, same strict gap policy, same Stage 3 hand-over."""

    def __init__(self, binned: BinnedDay, *, coverage: RecorderVerifiedCoverage, policy: RecorderHealthPolicy,
                 forecast_horizon_s: int = 60, min_history_bins: int = 600):
        if not isinstance(coverage, RecorderVerifiedCoverage):
            raise CapturedHistoryError("coverage_type_invalid", "a recorder history needs RecorderVerifiedCoverage (zeros require source evidence)")
        if not isinstance(binned.source, RecorderArrivalSource):
            raise CapturedHistoryError("source_type_invalid", "a recorder history needs a RecorderArrivalSource")
        super().__init__(binned, coverage=coverage, max_age_s=policy.freshness_limit_s, forecast_horizon_s=forecast_horizon_s,
                         min_history_bins=min_history_bins)
        self.recorder_policy = policy

    def coverage_provenance(self, decision_cutoff_utc: Any) -> dict[str, Any]:
        block = super().coverage_provenance(decision_cutoff_utc)
        cutoff = parse_utc(decision_cutoff_utc, "decision cutoff")
        snap = self.coverage.snapshot(cutoff)
        recorder_only = snap.recorder_only_zero_seconds(self.binned.counts, self.binned.day_start, self._next) if snap.valid else 0
        origins = set(block.get("recorder_evidence", {}).get("evidence_origins", ()))
        codes = set(snap.cause_codes())
        counts = self.coverage_counts
        if counts[CoverageClass.UNKNOWN_COVERAGE_ZERO.value]:
            codes.add(RC_NO_SOURCE_EVIDENCE if recorder_only else RC_ZERO_OUTSIDE_WINDOW)
        if counts[CoverageClass.COVERAGE_GAP.value] and RC_HEARTBEAT_GAP not in codes:
            codes.add(RC_HEARTBEAT_GAP)
        if counts[CoverageClass.INTERRUPTED_COLLECTION.value] and not codes & {RC_HEALTH_FAILURE, RC_RESTART_GAP, RC_MISSING_CALLBACKS, RC_HEARTBEAT_GAP}:
            codes.add(RC_INTERRUPTION)
        if counts[CoverageClass.STALE_COLLECTOR_EVIDENCE.value] or self._deferred is not None:
            codes.add(RC_STALE)
        if counts[CoverageClass.MISSING_OBSERVATION.value]:
            codes.add(RC_MISSING_OBSERVATION)
        block["abstention_reason_codes"] = sorted(codes)
        block["recorder_only_zero_seconds"] = recorder_only
        block["live_quality"] = False                                    # this module never certifies live-quality coverage
        block["synthetic_evidence"] = ORIGIN_SYNTHETIC in origins or not origins
        block["freshness_limit_s"] = self.recorder_policy.freshness_limit_s
        block["note"] = ("recorder-derived evidence: the recorder's own operation and callback coverage are verified from a hash-chained "
                         "journal; a zero second is believed only where an independent source count agrees. Source independence and any real "
                         "Caliper/Fabric run are NOT verified (live_source_coverage_verified is false).")
        return block

    def evidence(self, decision_cutoff_utc: Any) -> dict[str, Any]:
        doc = super().evidence(decision_cutoff_utc)
        doc["note"] = ("recorder-derived client-side submit events (Caliper TxSubmitted); not Ethereum arrivals, not ledger commits; "
                       "a valid history is an input contract check, not forecast skill")
        return doc
