"""Workload-coverage contract (task A0i, 2026-11-21): when may a zero-arrival second be believed?

The problem (A0h, central finding)
----------------------------------
A stored 1 s arrival series cannot tell "no transaction arrived" from "the collector was not listening". The strict live gap policy of
``workload_history`` says a missing second is never zero. A zero is therefore genuine ONLY when evidence outside the count itself shows the
collector was capturing during that second. This module defines that evidence, verifies it, and classifies every bin.

What counts as evidence (and what does not)
-------------------------------------------
A collector's own claim "I am healthy" is NOT evidence of coverage. Coverage is derived, by this module, from a hash-chained, append-only
collector-health journal (``collector_health.CollectorHealthJournal`` writes it; ``collector_health_event/1`` rows):

* an explicit ``start`` (collector instance, observation start, no retroactive claim),
* ``heartbeat`` rows whose watermark ``observed_through_utc`` says capture was continuous up to there, spaced no more than the policy's
  heartbeat gap apart (a longer silence is a COVERAGE GAP; the collector does not get to certify across it),
* ``error`` / ``recovered`` rows (an interruption stays uncovered until an explicit recovery, never repaired silently),
* a clean ``stop`` (coverage ends at its watermark),
* optionally the captured events themselves: unique event ids, per-instance sequence numbers (a hole is an interruption) and the
  heartbeat's cumulative event count (a mismatch makes the attestation contradictory).

Nothing is inferred from the count series, from nearby transactions, from the existence of a file, or from a dataset containing other
transactions. An empty second inside a verified interval is a GENUINE ZERO and is represented as 0; an empty second with no or broken
evidence is UNKNOWN and is never represented as 0. Evidence recorded after a decision time is never used at that decision time
(``VerifiedCoverage.snapshot(as_of)`` reads only rows with ``t_utc <= as_of``), so a later heartbeat cannot retroactively certify a past
interval for an earlier decision, and a collector that stops reporting leaves later seconds STALE, not covered.

Historical archives (the two Flashbots days) carry NO such evidence (their ``sources`` column is empty and no heartbeat, sequence or
lifecycle record exists). ``VerifiedCoverage.no_evidence`` represents that honestly: every zero is unknown. The separate, labelled
``benchmark_zero_fill`` mode of ``captured_history`` reproduces the stored benchmark input and never satisfies this contract.

Coverage scope (A0k, 2026-11-21): HEARTBEATS ARE NOT SOURCE COMPLETENESS
------------------------------------------------------------------------
Every journal ``start`` row declares what its collector's health can establish (``coverage_scope``):

* ``recorder_callback``: the recorder ran and its callback fed it events. A zero second inside this coverage is ``zero_count_unknown_coverage``
  (cause ``recorder_coverage_only``) UNLESS an independent source count verified by ``source_completeness`` agrees with the recorder over a span
  containing it. A recorder heartbeat never certifies a zero by itself.
* ``source_stream_observer``: the collector itself receives the source stream (the A0i premise, e.g. a mempool listener). Heartbeats may certify
  zeros, but this is a DECLARED claim; a journal that carries recorder fields cannot declare it (``coverage_scope_contradiction``).
* no ``coverage_scope`` (undeclared, including every journal written before A0k): positives are observed, zeros are UNKNOWN. Nothing is
  certified by an undeclared scope.

The generic ``VerifiedCoverage`` and every subclass obey this table; there is no other route to ``verified_zero``. ``benchmark_zero_fill`` lives
in ``captured_history`` and still never satisfies this contract.

No network, process, model, forecaster, Fabric or Docker use. This module only parses and computes; the journal writer is in
``collector_health`` and ``read_journal_file`` here only reads.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

COVERAGE_ATTESTATION_SCHEMA = "workload_coverage_attestation/2"
JOURNAL_EVENT_SCHEMA = "collector_health_event/1"
PROVENANCE_SCHEMA = "stage3_coverage_provenance/1"
GENESIS_SHA256 = "0" * 64
FAR_FUTURE = datetime(9999, 12, 31, tzinfo=timezone.utc)

MODE_ATTESTED = "strict_attested"                    # a verified collector-health journal backs the coverage
MODE_NO_EVIDENCE = "strict_no_evidence"              # strict, but no coverage evidence exists: every zero is unknown
MODE_BENCHMARK = "benchmark_zero_fill"               # historical reproduction; never live-quality, never satisfies the contract
MODE_LEGACY_ASSERTED = "legacy_asserted_intervals"   # the A0h ``CoverageAttestation``: intervals asserted by a named party, no journal
COVERAGE_MODES = (MODE_ATTESTED, MODE_NO_EVIDENCE, MODE_BENCHMARK, MODE_LEGACY_ASSERTED)

SCOPE_RECORDER_CALLBACK = "recorder_callback"          # the recorder ran and its callback fed it events; says nothing about the source
SCOPE_SOURCE_STREAM = "source_stream_observer"         # the collector itself receives the source stream (A0i premise); a DECLARED claim
COVERAGE_SCOPES = (SCOPE_RECORDER_CALLBACK, SCOPE_SOURCE_STREAM)
RECORDER_FINGERPRINT = ("recorder_version", "lifecycle", "declared_policy", "evidence_origin", "options_sha256")
RECORDER_ONLY_CAUSE = "recorder_coverage_only"
UNDECLARED_SCOPE_CAUSE = "coverage_scope_undeclared"

KIND_START, KIND_HEARTBEAT, KIND_ERROR, KIND_RECOVERED, KIND_STOP = "start", "heartbeat", "error", "recovered", "stop"
EVENT_KINDS = (KIND_START, KIND_HEARTBEAT, KIND_ERROR, KIND_RECOVERED, KIND_STOP)
HEALTH_VALUES = ("healthy", "degraded", "error")
STOP_REASONS = ("clean", "abort")


class CoverageClass(str, Enum):
    """What one bin is, as far as the evidence can say. Only the first two may enter a strict live-quality history."""

    OBSERVED_POSITIVE = "observed_positive"                              # arrivals > 0 (and, in attested mode, inside verified coverage)
    VERIFIED_ZERO = "verified_zero"                                      # zero arrivals inside verified coverage: a genuine zero
    UNKNOWN_COVERAGE_ZERO = "zero_count_unknown_coverage"                # zero count, nothing shows the collector was listening
    MISSING_OBSERVATION = "missing_observation"                          # the series has no row for the second
    INTERRUPTED_COLLECTION = "interrupted_collection"                    # an error, a degraded report or a restart boundary covers it
    STALE_COLLECTOR_EVIDENCE = "stale_collector_evidence"                # newer than the last evidence available at the decision time
    INVALID_ATTESTATION = "invalid_or_contradictory_attestation"         # the evidence failed an integrity or consistency check
    COVERAGE_GAP = "coverage_gap"                                        # heartbeats too far apart, or no verified coverage for the second
    UNVERIFIED_ZERO_FILL = "unverified_historical_zero_fill"             # benchmark reproduction only


ACCEPTED_CLASSES = frozenset({CoverageClass.OBSERVED_POSITIVE, CoverageClass.VERIFIED_ZERO})


class CoverageEvidenceError(ValueError):
    """Coverage evidence is malformed, inconsistent or contradictory. ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


@dataclass(frozen=True)
class CoveragePolicy:
    """Operator-set limits. The defaults are PLACEHOLDERS with no scientific justification; an experiment must state its own."""

    max_heartbeat_gap_s: float = 5.0

    def __post_init__(self) -> None:
        value = self.max_heartbeat_gap_s
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise CoverageEvidenceError("policy_invalid", "max_heartbeat_gap_s must be a finite positive number")

    def as_dict(self) -> dict[str, Any]:
        return {"max_heartbeat_gap_s": self.max_heartbeat_gap_s,
                "basis": "operator-set placeholder; no scientific justification is claimed"}


# ------------------------------------------------------------------------------------------------------------------ helpers
def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_utc(value: Any, name: str) -> datetime:
    try:
        moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise CoverageEvidenceError("timestamp_malformed", f"{name} is not an ISO-8601 timestamp: {value!r}") from exc
    if moment.tzinfo is None:
        raise CoverageEvidenceError("timestamp_naive", f"{name} has no timezone: {value!r}")
    return moment.astimezone(timezone.utc)


def whole_second(value: Any, name: str) -> datetime:
    moment = parse_utc(value, name)
    if moment.microsecond:
        raise CoverageEvidenceError("timestamp_not_whole_second", f"{name} must be a whole UTC second: {moment.isoformat()}")
    return moment


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def floor_second(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(microsecond=0)


def event_digest(body: Mapping[str, Any], previous: str) -> str:
    """SHA-256 of a journal row: its body (without ``prev`` and ``sha256``) chained to the previous row's digest."""
    return sha256_text(canonical({**{k: v for k, v in body.items() if k not in ("prev", "sha256")}, "prev": previous}))


def read_journal_file(path: Path) -> tuple[list[dict[str, Any]], bool, str]:
    """Read a journal (JSON lines). Returns ``(events, torn_tail, file_sha256)``. A final line without a newline, or one that is not
    JSON, is an interrupted write: it is reported as ``torn_tail=True`` and dropped from ``events`` (never repaired in the file)."""
    raw = Path(path).read_bytes()
    text = raw.decode("utf-8")
    lines = text.split("\n")
    torn = bool(lines[-1])                                  # a last line with no terminating newline
    complete = lines[:-1]
    events: list[dict[str, Any]] = []
    for number, line in enumerate(complete, 1):
        if not line.strip():
            raise CoverageEvidenceError("journal_blank_line", f"journal line {number} is blank")
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CoverageEvidenceError("journal_row_malformed", f"journal line {number} is not JSON") from exc
        if not isinstance(row, dict):
            raise CoverageEvidenceError("journal_row_malformed", f"journal line {number} is not an object")
        events.append(row)
    return events, torn, hashlib.sha256(raw).hexdigest()


# ------------------------------------------------------------------------------------------------------------- derivation
@dataclass
class _Derivation:
    covered: list            # [(start, end)] merged, sorted
    uncovered: list          # [(start, end, CoverageClass, cause)]
    first_start: datetime | None
    frontier: datetime | None            # the last watermark of the active instance
    active: bool
    errored: bool
    last_evidence_t: datetime | None
    heartbeats: list         # [(instance, watermark, events_received, last_event_seq, t)]
    instances: dict          # instance -> {"observation_start": dt, "first_event_seq": int|None}
    terminated_cleanly: bool
    health_final: str | None
    records_verified: int
    chain_head: str


def _need(ev: Mapping[str, Any], keys: Sequence[str], index: int) -> None:
    for key in keys:
        if key not in ev or ev[key] is None:
            raise CoverageEvidenceError("event_field_missing", f"journal record {index} ({ev.get('kind')}) lacks {key!r}")


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


def _count(value: Any, name: str, index: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CoverageEvidenceError("event_field_invalid", f"journal record {index}: {name} must be a non-negative integer")
    return value


def derive(events: Sequence[Mapping[str, Any]], policy: CoveragePolicy, *, source_id: str, run_id: str | None = None,
           channel_id: str | None = None, as_of: datetime | None = None) -> _Derivation:
    """Verify the journal rows with ``t_utc <= as_of`` and derive covered and uncovered spans. Raises ``CoverageEvidenceError`` on any
    structural or logical contradiction: the caller turns that into an INVALID attestation (the whole attestation, never a part)."""
    covered: list = []
    uncovered: list = []
    heartbeats: list = []
    instances: dict = {}
    previous_hash, previous_t = GENESIS_SHA256, None
    st: dict[str, Any] = {"active": False, "errored": False, "wm": None, "last_hb_t": None, "inst": None, "err_from": None,
                          "err_code": None, "first_start": None, "stopped_cleanly": False, "health": None, "last_t": None}
    verified = 0
    for index, ev in enumerate(events, 1):
        if not isinstance(ev, Mapping):
            raise CoverageEvidenceError("event_malformed", f"journal record {index} is not an object")
        if ev.get("schema") != JOURNAL_EVENT_SCHEMA:
            raise CoverageEvidenceError("event_schema_mismatch", f"journal record {index} schema is {ev.get('schema')!r}")
        t = parse_utc(ev.get("t_utc"), f"record {index} t_utc")
        if as_of is not None and t > as_of:
            break                                           # evidence after the decision time does not exist yet
        if ev.get("seq") != index:
            raise CoverageEvidenceError("event_sequence_broken", f"journal record {index} has seq {ev.get('seq')!r}")
        if ev.get("prev") != previous_hash:
            raise CoverageEvidenceError("hash_chain_broken", f"journal record {index} does not follow record {index - 1}")
        if ev.get("sha256") != event_digest(ev, previous_hash):
            raise CoverageEvidenceError("hash_mismatch", f"journal record {index} was altered after it was written")
        previous_hash = ev["sha256"]
        if ev.get("source_id") != source_id:
            raise CoverageEvidenceError("source_identity_mismatch", f"journal record {index} is for source {ev.get('source_id')!r}, not {source_id!r}")
        if run_id is not None and ev.get("run_id") != run_id:
            raise CoverageEvidenceError("run_identity_mismatch", f"journal record {index} is for run {ev.get('run_id')!r}, not {run_id!r}")
        if channel_id is not None and ev.get("channel_id") != channel_id:
            raise CoverageEvidenceError("channel_identity_mismatch", f"journal record {index} is for channel {ev.get('channel_id')!r}")
        if previous_t is not None and t < previous_t:
            raise CoverageEvidenceError("timestamp_regressed", f"journal record {index} is earlier than record {index - 1}")
        previous_t = t
        kind = ev.get("kind")
        if kind not in EVENT_KINDS:
            raise CoverageEvidenceError("event_kind_unknown", f"journal record {index} has kind {kind!r}")
        if index == 1 and kind != KIND_START:
            raise CoverageEvidenceError("journal_not_started", "the first journal record must be a start")
        _need(ev, ("collector_instance",), index)
        if kind == KIND_START:
            _need(ev, ("observation_start_utc", "timestamp_provenance"), index)
            start = whole_second(ev["observation_start_utc"], f"record {index} observation_start_utc")
            if start < t:
                raise CoverageEvidenceError("retroactive_coverage_claim",
                                            f"record {index}: observation starts at {iso(start)}, before the start was recorded at {iso(t)}")
            if ev["collector_instance"] in instances:
                raise CoverageEvidenceError("instance_reused", f"record {index}: instance {ev['collector_instance']!r} already started")
            scope = ev.get("coverage_scope")
            if scope is not None and scope not in COVERAGE_SCOPES:
                raise CoverageEvidenceError("coverage_scope_invalid", f"journal record {index}: coverage_scope {scope!r} is not one of {COVERAGE_SCOPES}")
            fingerprint = [k for k in RECORDER_FINGERPRINT if k in ev]
            if fingerprint and scope is None:
                raise CoverageEvidenceError("coverage_scope_invalid", f"journal record {index}: a start row carrying recorder fields {fingerprint} "
                                                                      f"must declare coverage_scope={SCOPE_RECORDER_CALLBACK!r}")
            if fingerprint and scope != SCOPE_RECORDER_CALLBACK:
                raise CoverageEvidenceError("coverage_scope_contradiction", f"journal record {index}: recorder fields {fingerprint} contradict the "
                                                                            f"declared scope {scope!r}; a recorder cannot claim source coverage")
            if any(info["coverage_scope"] != scope for info in instances.values()):
                raise CoverageEvidenceError("coverage_scope_inconsistent", f"journal record {index}: a restart changed the declared coverage scope")
            if st["active"]:                                # a restart without a stop: the time between is not covered
                # (start >= t >= every earlier watermark, so a new instance can never overlap what an earlier one certified)
                if st["errored"]:                           # an error still open when the process died stays uncovered up to the new start
                    uncovered.append((st["err_from"], start, CoverageClass.INTERRUPTED_COLLECTION, f"collector_error:{st['err_code']}"))
                else:
                    uncovered.append((st["wm"], start, CoverageClass.INTERRUPTED_COLLECTION, "restart_boundary"))
            first_seq = ev.get("first_event_seq")
            if first_seq is not None:
                _count(first_seq, "first_event_seq", index)
            instances[ev["collector_instance"]] = {"observation_start": start, "first_event_seq": first_seq, "coverage_scope": scope}
            st.update(active=True, errored=False, wm=start, last_hb_t=t, inst=ev["collector_instance"], stopped_cleanly=False,
                      health="healthy", first_start=st["first_start"] or start, err_from=None)
        else:
            if not st["active"]:
                raise CoverageEvidenceError("event_without_active_collector", f"record {index} ({kind}) arrives with no collector started")
            if ev["collector_instance"] != st["inst"]:
                raise CoverageEvidenceError("instance_mismatch", f"record {index} belongs to instance {ev['collector_instance']!r}, not {st['inst']!r}")
            if kind in (KIND_HEARTBEAT, KIND_STOP):
                _need(ev, ("observed_through_utc", "events_received"), index)
                watermark = whole_second(ev["observed_through_utc"], f"record {index} observed_through_utc")
                received = _count(ev["events_received"], "events_received", index)
                if ev.get("last_event_seq") is not None:
                    _count(ev["last_event_seq"], "last_event_seq", index)
                if watermark > t:
                    raise CoverageEvidenceError("future_watermark", f"record {index} claims capture through {iso(watermark)}, after it was written")
                if kind == KIND_HEARTBEAT:
                    _need(ev, ("health", "errors_since_last"), index)
                    health, errors = ev["health"], _count(ev["errors_since_last"], "errors_since_last", index)
                    if health not in HEALTH_VALUES:
                        raise CoverageEvidenceError("event_field_invalid", f"record {index}: health {health!r} is not one of {HEALTH_VALUES}")
                    if health == "healthy" and errors > 0:
                        raise CoverageEvidenceError("contradictory_health_evidence",
                                                    f"record {index} reports healthy while counting {errors} error(s) since the last heartbeat")
                else:
                    _need(ev, ("reason",), index)
                    if ev["reason"] not in STOP_REASONS:
                        raise CoverageEvidenceError("event_field_invalid", f"record {index}: stop reason {ev['reason']!r}")
                    health = "healthy" if ev["reason"] == "clean" and not st["errored"] else "error"
                if not st["errored"]:
                    if watermark < st["wm"]:
                        raise CoverageEvidenceError("watermark_regressed", f"record {index} moves the watermark back")
                    if health != "healthy":
                        uncovered.append((st["wm"], watermark, CoverageClass.INTERRUPTED_COLLECTION, f"collector_reported_{health}"))
                    elif (t - st["last_hb_t"]).total_seconds() > policy.max_heartbeat_gap_s:
                        uncovered.append((st["wm"], watermark, CoverageClass.COVERAGE_GAP, "heartbeat_gap"))
                    else:
                        covered.append((st["wm"], watermark))
                    st["wm"] = watermark
                st["last_hb_t"], st["health"] = t, health
                heartbeats.append((ev["collector_instance"], watermark, received, ev.get("last_event_seq"), t))
                if kind == KIND_STOP:
                    if st["errored"]:
                        uncovered.append((st["err_from"], FAR_FUTURE, CoverageClass.INTERRUPTED_COLLECTION, f"collector_error:{st['err_code']}"))
                    st.update(active=False, stopped_cleanly=(ev["reason"] == "clean" and not st["errored"]), errored=False)
            elif kind == KIND_ERROR:
                _need(ev, ("error_code",), index)
                if st["errored"]:
                    raise CoverageEvidenceError("error_while_errored", f"record {index}: a collector error is already open")
                origin = instances[st["inst"]]["observation_start"]
                affected = (whole_second(ev["affects_from_utc"], f"record {index} affects_from_utc")
                            if ev.get("affects_from_utc") is not None else st["wm"])
                if affected < origin:
                    raise CoverageEvidenceError("event_field_invalid", f"record {index}: affects_from_utc precedes the instance's observation start")
                if affected > t:
                    raise CoverageEvidenceError("event_field_invalid", f"record {index}: affects_from_utc is in the future")
                st.update(errored=True, err_from=min(affected, st["wm"]), err_code=str(ev["error_code"]))
            else:                                           # recovered
                _need(ev, ("coverage_resumes_at_utc",), index)
                if not st["errored"]:
                    raise CoverageEvidenceError("recovery_without_error", f"record {index} recovers a collector that has no open error")
                resumes = whole_second(ev["coverage_resumes_at_utc"], f"record {index} coverage_resumes_at_utc")
                if resumes < t:
                    raise CoverageEvidenceError("retroactive_coverage_claim", f"record {index}: coverage cannot resume before the recovery was recorded")
                uncovered.append((st["err_from"], resumes, CoverageClass.INTERRUPTED_COLLECTION, f"collector_error:{st['err_code']}"))
                st.update(errored=False, wm=resumes, last_hb_t=t)
        st["last_t"] = t
        verified = index
    if st["errored"]:                                       # an error still open at the end of the evidence
        uncovered.append((st["err_from"], FAR_FUTURE, CoverageClass.INTERRUPTED_COLLECTION, f"collector_error:{st['err_code']}"))
    return _Derivation(covered=_merge(covered), uncovered=uncovered, first_start=st["first_start"],
                       frontier=st["wm"] if st["active"] and not st["errored"] else None, active=st["active"], errored=st["errored"],
                       last_evidence_t=st["last_t"], heartbeats=heartbeats, instances=instances,
                       terminated_cleanly=st["stopped_cleanly"], health_final=st["health"], records_verified=verified,
                       chain_head=previous_hash)


def _crosscheck_events(derivation: _Derivation, observed: Sequence[Mapping[str, Any]]) -> tuple[list, dict]:
    """Compare the journal with the captured events. Returns ``(extra_uncovered_spans, report)``. Raises on a contradiction.

    Duplicate event ids are dropped first (the first occurrence counts), so a duplicate cannot inflate a count and cannot hide a hole.
    A heartbeat's cumulative ``events_received`` must equal the unique events of its instance in ``[observation_start, watermark)``.
    Sequence numbers, where every unique event of an instance has one, must be contiguous from ``first_event_seq``; a hole is an
    interruption between the neighbouring events.
    """
    unique: dict[str, Mapping[str, Any]] = {}
    duplicates = 0
    for event in observed:
        if "id" not in event or "ts_ms" not in event or "instance" not in event:
            raise CoverageEvidenceError("event_record_invalid", "every captured event needs id, ts_ms and instance")
        key = str(event["id"])
        if key in unique:
            duplicates += 1
            continue
        unique[key] = event
    per_instance: dict[str, list] = {}
    for event in unique.values():
        per_instance.setdefault(str(event["instance"]), []).append(event)
    extra: list = []
    sequence_state = {}
    for instance, items in per_instance.items():
        if instance not in derivation.instances:
            raise CoverageEvidenceError("event_instance_unknown", f"captured events name instance {instance!r}, which has no start record")
        items.sort(key=lambda e: (int(e["ts_ms"]), -1 if e.get("seq") is None else int(e["seq"])))
        if all(e.get("seq") is not None for e in items) and items:
            seqs = sorted(int(e["seq"]) for e in items)
            first = derivation.instances[instance]["first_event_seq"]
            sequence_state[instance] = "checked"
            if len(set(seqs)) != len(seqs):
                raise CoverageEvidenceError("sequence_duplicate", f"two different events of instance {instance!r} share a sequence number")
            if first is not None and seqs[0] != first:
                raise CoverageEvidenceError("sequence_start_mismatch", f"instance {instance!r}: first sequence {seqs[0]} is not {first}")
            by_seq = {int(e["seq"]): int(e["ts_ms"]) for e in items}
            for left, right in zip(seqs, seqs[1:]):
                if right != left + 1:
                    start = datetime.fromtimestamp(by_seq[left] // 1000, tz=timezone.utc)
                    end = datetime.fromtimestamp(by_seq[right] // 1000 + 1, tz=timezone.utc)
                    extra.append((start, end, CoverageClass.INTERRUPTED_COLLECTION, f"event_sequence_gap:{left + 1}-{right - 1}"))
        else:
            sequence_state[instance] = "not_available"
    for instance in derivation.instances:
        sequence_state.setdefault(instance, "no_events")
    for instance, watermark, received, _last_seq, _t in derivation.heartbeats:
        origin_ms = int(derivation.instances[instance]["observation_start"].timestamp() * 1000)
        wm_ms = int(watermark.timestamp() * 1000)
        expected = sum(1 for e in per_instance.get(instance, ()) if origin_ms <= int(e["ts_ms"]) < wm_ms)
        if received != expected:
            raise CoverageEvidenceError("heartbeat_event_count_mismatch",
                                        f"instance {instance!r}: a heartbeat reports {received} events through {iso(watermark)}, the data holds {expected}")
    return extra, {"unique_events": len(unique), "duplicates_dropped": duplicates, "sequence_evidence": sequence_state}


# -------------------------------------------------------------------------------------------------------------- snapshot
@dataclass(frozen=True)
class CoverageSnapshot:
    """The coverage evidence available at ONE decision time. Immutable."""

    mode: str
    source_id: str
    as_of: datetime | None
    derivation: _Derivation | None
    failure_code: str | None
    failure_reason: str | None
    extra_uncovered: tuple = ()
    crosscheck: Mapping[str, Any] | None = None
    journal_sha256: str | None = None
    scope: str | None = None                 # the journal's declared coverage scope (None: undeclared)
    source_verified: tuple = ()              # [start, end) spans in which an independent source count agrees with the collector
    source_info: Mapping[str, Any] | None = None

    @property
    def valid(self) -> bool:
        return self.failure_code is None

    def _zero_verdict(self, second: datetime, why: str) -> tuple[CoverageClass, str]:
        """A zero inside operational coverage: may it be believed? Heartbeats alone never certify one unless the collector observes the source."""
        if self.scope == SCOPE_SOURCE_STREAM:
            return CoverageClass.VERIFIED_ZERO, why
        if self.scope == SCOPE_RECORDER_CALLBACK:
            if _inside(self.source_verified, second, second + timedelta(seconds=1)):
                return CoverageClass.VERIFIED_ZERO, f"{why}; an independent source count agrees over a span containing this second"
            return (CoverageClass.UNKNOWN_COVERAGE_ZERO,
                    f"{RECORDER_ONLY_CAUSE}: the recorder was healthy and its callback fed it events here, but no independent "
                    f"source count covers this second, so a zero cannot be told from a missed event")
        return (CoverageClass.UNKNOWN_COVERAGE_ZERO,
                f"{UNDECLARED_SCOPE_CAUSE}: the journal declares no coverage scope, so heartbeats are not read as source coverage")

    def _verdict(self, second: datetime) -> tuple[str, CoverageClass | None, str]:
        """``("covered" | "uncovered" | "pending" | "unknown" | "invalid", class, cause)`` for the whole second [second, second+1)."""
        end = second + timedelta(seconds=1)
        if self.derivation is None:
            return "unknown", None, self.failure_reason or "no collector evidence exists for this source"
        d = self.derivation
        hit = [(c, why) for s, e, c, why in (*d.uncovered, *self.extra_uncovered) if s < end and second < e]
        if hit:
            for priority in (CoverageClass.INTERRUPTED_COLLECTION, CoverageClass.COVERAGE_GAP):
                for cls, why in hit:
                    if cls == priority:
                        return "uncovered", cls, why
        index = bisect.bisect_right([s for s, _ in d.covered], second) - 1
        if index >= 0 and end <= d.covered[index][1]:
            return "covered", None, "inside a verified coverage interval"
        if d.first_start is not None and second >= d.first_start and d.frontier is not None and end > d.frontier:
            return "pending", CoverageClass.STALE_COLLECTOR_EVIDENCE, \
                f"newer than the last collector evidence available at the decision time (watermark {iso(d.frontier)})"
        if d.first_start is None or second < d.first_start:
            return "unknown", None, "before the collector's recorded observation start"
        if d.terminated_cleanly:
            return "unknown", None, "after the collector terminated"
        return "unknown", None, "no verified coverage for this second"

    def classify(self, second: datetime, count: int | None) -> tuple[CoverageClass, str]:
        """Classify the bin starting at ``second``. ``count`` is None when the series has no row for it."""
        if count is None:
            return CoverageClass.MISSING_OBSERVATION, "the series has no row for this second; it is never read as zero"
        if self.mode == MODE_NO_EVIDENCE:
            return (CoverageClass.OBSERVED_POSITIVE, "positive count; no coverage evidence exists for this source") if count > 0 else \
                (CoverageClass.UNKNOWN_COVERAGE_ZERO, self.failure_reason or "no collector evidence exists for this source")
        if not self.valid:
            return CoverageClass.INVALID_ATTESTATION, f"{self.failure_code}: {self.failure_reason}"
        verdict, cls, why = self._verdict(second)
        if verdict == "covered":
            return (CoverageClass.OBSERVED_POSITIVE, why) if count > 0 else self._zero_verdict(second, why)
        if cls is not None:
            return cls, why
        return (CoverageClass.UNKNOWN_COVERAGE_ZERO, why) if count == 0 else (CoverageClass.COVERAGE_GAP, why)

    def summary(self) -> dict[str, Any]:
        d = self.derivation
        return {"schema": PROVENANCE_SCHEMA, "mode": self.mode, "source_id": self.source_id,
                "as_of_utc": None if self.as_of is None else iso(self.as_of),
                "integrity": {"status": ("NO_EVIDENCE" if self.mode == MODE_NO_EVIDENCE else "VERIFIED" if self.valid else "FAILED"),
                              "failure_code": self.failure_code, "failure_reason": self.failure_reason},
                "journal_sha256": self.journal_sha256,
                "chain_head_sha256": None if d is None else d.chain_head,
                "coverage_scope": self.scope,
                "source_evidence": None if self.source_info is None else dict(self.source_info),
                "records_verified": None if d is None else d.records_verified,
                "collector_active": None if d is None else d.active,
                "collector_final_health": None if d is None else d.health_final,
                "frontier_utc": None if d is None or d.frontier is None else iso(d.frontier),
                "last_evidence_utc": None if d is None or d.last_evidence_t is None else iso(d.last_evidence_t),
                "event_crosscheck": None if self.crosscheck is None else dict(self.crosscheck)}


class VerifiedCoverage:
    """Coverage evidence for one source. ``snapshot(as_of)`` is the only way to read it, so every read has a decision time."""

    def __init__(self, *, source_id: str, mode: str, events: Sequence[Mapping[str, Any]] | None = None,
                 policy: CoveragePolicy | None = None, observed_events: Sequence[Mapping[str, Any]] | None = None,
                 run_id: str | None = None, channel_id: str | None = None, no_evidence_reason: str | None = None,
                 journal_sha256: str | None = None, torn_tail: bool = False, source_evidence: Any = None):
        if not source_id or not isinstance(source_id, str):
            raise CoverageEvidenceError("source_identity_missing", "coverage needs a source_id")
        if mode not in (MODE_ATTESTED, MODE_NO_EVIDENCE):
            raise CoverageEvidenceError("coverage_mode_invalid", f"VerifiedCoverage serves {MODE_ATTESTED!r} or {MODE_NO_EVIDENCE!r}, not {mode!r}")
        if mode == MODE_ATTESTED and not events:
            raise CoverageEvidenceError("journal_empty", "attested coverage needs a non-empty collector-health journal")
        self.source_id, self.mode = source_id, mode
        self.events = tuple(dict(e) for e in events or ())
        self.policy = policy or CoveragePolicy()
        if observed_events is not None:
            for event in observed_events:
                if not {"id", "ts_ms", "instance"} <= set(event) or isinstance(event["ts_ms"], bool) or not isinstance(event["ts_ms"], int):
                    raise CoverageEvidenceError("event_record_invalid", "every captured event needs id, integer ts_ms and instance")
        self.observed_events = None if observed_events is None else tuple(dict(e) for e in observed_events)
        self.run_id, self.channel_id = run_id, channel_id
        self.no_evidence_reason = no_evidence_reason
        self.journal_sha256, self.torn_tail = journal_sha256, bool(torn_tail)
        # Optional independent source evidence: a ``source_completeness.DispatchLogEvidence`` and nothing else (a duck-typed object that returned its own
        # "verified" spans would be a bypass). Without it the journal's own reported source counts are used. Either way a recorder_callback zero
        # needs a verified source span.
        if source_evidence is not None:
            from . import source_completeness as sc                      # lazy: that module imports this one
            if not isinstance(source_evidence, sc.DispatchLogEvidence):
                raise CoverageEvidenceError("source_evidence_type_invalid", "source_evidence must be a source_completeness.DispatchLogEvidence")
        self.source_evidence = source_evidence

    def declared_scopes(self) -> set:
        """The coverage scopes named by the journal's start rows as written (``None`` for an undeclared start). Empty without a journal."""
        return {ev.get("coverage_scope") for ev in self.events if ev.get("kind") == KIND_START}

    @classmethod
    def no_evidence(cls, source_id: str, reason: str) -> "VerifiedCoverage":
        """A source with no collector lifecycle, heartbeat or sequence evidence (every historical archive in this repository)."""
        return cls(source_id=source_id, mode=MODE_NO_EVIDENCE, no_evidence_reason=reason)

    @classmethod
    def from_journal_file(cls, path: Path, *, source_id: str, policy: CoveragePolicy | None = None,
                          observed_events: Sequence[Mapping[str, Any]] | None = None, run_id: str | None = None,
                          channel_id: str | None = None, source_evidence: Any = None) -> "VerifiedCoverage":
        events, torn, digest = read_journal_file(Path(path))
        return cls(source_id=source_id, mode=MODE_ATTESTED, events=events, policy=policy, observed_events=observed_events,
                   run_id=run_id, channel_id=channel_id, journal_sha256=digest, torn_tail=torn, source_evidence=source_evidence)

    def snapshot(self, as_of: Any) -> CoverageSnapshot:
        moment = parse_utc(as_of, "as_of")
        if self.mode == MODE_NO_EVIDENCE:
            return CoverageSnapshot(self.mode, self.source_id, moment, None, None,
                                    self.no_evidence_reason or "no collector evidence exists for this source")
        try:
            derivation = derive(self.events, self.policy, source_id=self.source_id, run_id=self.run_id, channel_id=self.channel_id, as_of=moment)
            extra, report = (), None
            if self.observed_events is not None:
                visible = [e for e in self.observed_events if e["ts_ms"] <= int(moment.timestamp() * 1000)]   # nothing from after the decision time
                spans, report = _crosscheck_events(derivation, visible)
                extra = tuple(spans)
            from . import source_completeness as sc                      # lazy: that module imports this one
            found = (self.source_evidence.spans(derivation, moment, self.events) if self.source_evidence is not None
                     else sc.journal_source_spans(self.events, derivation, moment))
            scopes = {info["coverage_scope"] for info in derivation.instances.values()}
            scope = next(iter(scopes)) if len(scopes) == 1 else None
            extra = (*extra, *((s, e, CoverageClass(c), why) for s, e, c, why in found.mismatches))
            info = {"kind": found.evidence_kind, "verified_seconds": int(sum((e - s).total_seconds() for s, e in found.verified)),
                    "classifications": dict(found.classifications), "rejection_codes": found.reason_codes(),
                    "basis_claimed": list(found.claims), "independence_claims": list(found.independence_claims),
                    "independence": "DECLARED by the collector and structurally checked; never proven by this module"}
            return CoverageSnapshot(self.mode, self.source_id, moment, derivation, None, None, extra, report, self.journal_sha256,
                                    scope, tuple(found.verified), info)
        except CoverageEvidenceError as exc:
            return CoverageSnapshot(self.mode, self.source_id, moment, None, exc.code, str(exc), (), None, self.journal_sha256)


# ----------------------------------------------------------------------------------------------------------- attestation
def build_attestation(coverage: VerifiedCoverage, *, source: Mapping[str, Any], as_of: Any, created_utc: str,
                      journal_path: str | None = None) -> dict[str, Any]:
    """The ``workload_coverage_attestation/2`` document for ``coverage`` at ``as_of``. Fields the source cannot supply are ``null``.

    The document is a REPORT of what the journal verifies; ``check_attestation`` recomputes it from the journal, so a document cannot
    claim more than its journal supports.
    """
    snap = coverage.snapshot(as_of)
    d = snap.derivation
    doc: dict[str, Any] = {
        "schema": COVERAGE_ATTESTATION_SCHEMA, "schema_version": 2, "created_utc": created_utc,
        "source": {"source_id": coverage.source_id, "dataset_id": source.get("dataset_id"), "dataset_sha256": source.get("dataset_sha256"),
                   "kind": source.get("kind"), "is_fabric_observation": source.get("is_fabric_observation")},
        "run_id": coverage.run_id, "channel_id": coverage.channel_id, "mode": coverage.mode, "evaluated_as_of_utc": iso(snap.as_of),
        "policy": coverage.policy.as_dict() if coverage.mode == MODE_ATTESTED else None,
        "integrity": {"status": ("VERIFIED" if snap.valid else "FAILED") if coverage.mode == MODE_ATTESTED else "NO_EVIDENCE",
                      "failure_code": snap.failure_code, "failure_reason": snap.failure_reason},
        "failure_reason": snap.failure_reason, "journal": None, "collector": None, "observation_interval": None,
        "coverage_intervals": [], "uncovered_spans": [], "events": None,
        "timestamp_provenance": None,
        "claim": "coverage_intervals are the ONLY seconds in which a zero count is a genuine zero; everything else is unknown or worse",
    }
    if coverage.mode == MODE_ATTESTED:
        doc["journal"] = {"path": journal_path, "sha256": coverage.journal_sha256, "records_in_scope": len(coverage.events),
                          "torn_tail_dropped": coverage.torn_tail, "chain_head_sha256": None if d is None else d.chain_head}
        if d is not None:
            starts = [e for e in coverage.events if e.get("kind") == KIND_START]
            doc["timestamp_provenance"] = sorted({str(e.get("timestamp_provenance")) for e in starts})
            doc["collector"] = {"instances": sorted(d.instances), "heartbeat_count": len(d.heartbeats), "final_health": d.health_final,
                                "active_at_as_of": d.active, "frontier_utc": None if d.frontier is None else iso(d.frontier),
                                "last_evidence_utc": None if d.last_evidence_t is None else iso(d.last_evidence_t),
                                "terminated_cleanly": d.terminated_cleanly}
            ends = [e for _, e in d.covered] + [e for _, e, _, _ in d.uncovered if e != FAR_FUTURE]
            doc["observation_interval"] = {"start_utc": None if d.first_start is None else iso(d.first_start),
                                           "end_utc": iso(max(ends)) if ends else None}
            doc["coverage_intervals"] = [[iso(s), iso(e)] for s, e in d.covered]
            doc["uncovered_spans"] = [{"start_utc": iso(s), "end_utc": None if e == FAR_FUTURE else iso(e), "class": c.value, "cause": why}
                                      for s, e, c, why in (*d.uncovered, *snap.extra_uncovered)]
            doc["events"] = {"heartbeat_count": len(d.heartbeats), "crosscheck": None if snap.crosscheck is None else dict(snap.crosscheck),
                             "sequence_evidence": "see crosscheck.sequence_evidence; null when no captured events were supplied"}
    else:
        doc["failure_reason"] = coverage.no_evidence_reason or "no collector evidence exists for this source"
        doc["events"] = {"heartbeat_count": None, "crosscheck": None, "sequence_evidence": None}
    return doc


def check_attestation(doc: Mapping[str, Any], coverage: VerifiedCoverage) -> list[str]:
    """Recompute the attestation from ``coverage`` and return the disagreements (empty = consistent). A document that claims a
    coverage interval its journal does not verify is CONTRADICTORY and must be treated as invalid."""
    problems: list[str] = []
    if doc.get("schema") != COVERAGE_ATTESTATION_SCHEMA:
        return [f"schema is {doc.get('schema')!r}"]
    try:
        as_of = doc["evaluated_as_of_utc"]
        rebuilt = build_attestation(coverage, source=doc.get("source") or {}, as_of=as_of, created_utc=str(doc.get("created_utc")),
                                    journal_path=(doc.get("journal") or {}).get("path"))
    except (KeyError, CoverageEvidenceError) as exc:
        return [f"cannot recompute: {exc}"]
    for key in ("mode", "coverage_intervals", "uncovered_spans", "integrity", "observation_interval"):
        if rebuilt.get(key) != doc.get(key):
            problems.append(f"{key} differs from what the journal verifies")
    if (doc.get("journal") or {}).get("sha256") != (rebuilt.get("journal") or {}).get("sha256"):
        problems.append("journal sha256 differs")
    return problems
