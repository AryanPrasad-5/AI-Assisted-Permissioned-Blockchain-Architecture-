"""Source-completeness evidence contract (task A0k, 2026-11-21): what may count as an INDEPENDENT source count, and how it is reconciled.

The question this module answers
--------------------------------
A zero-arrival second is believable only if something OTHER than the observer's own callbacks shows that the workload generator issued no
transaction in it. Recorder health, callbacks, completion logs and generator-side issuance records are different things; this module keeps
them apart and refuses to let the first three stand in for the fourth.

What exists in this repository (found by inspection, A0k report section 6; nothing here talks to Caliper, Fabric or a network)
-------------------------------------------------------------------------------------------------------------------------------
* ``fabric-testbed/caliper/workloads/e1_trace_replay.js`` appends one row per dispatched transaction to a dispatch log
  (``seq,intended_offset_ms,intended_ms,dispatch_ms,lag_ms``) BEFORE it calls ``sutAdapter.sendRequests``. That is generator-side, per
  transaction, and does not pass through the recorder's callbacks. It is the one source-side per-transaction record that exists.
* ``fabric-testbed/caliper/workloads/e1_create_asset.js`` (the workload the pilot renderer ``e1_la2_render.py`` uses) keeps only an in-memory
  ``this.seq``; it exposes and logs nothing. NOT AVAILABLE as written.
* A configured ``txNumber`` / ``txDuration`` is intent, not an observed issued count (``p4_replay/caliper_check.py`` F5-F8 and the trace-replay
  comment on Caliper 0.6 trailing callbacks show the issued count can differ). ROUND-ONLY at best.
* ``txSubmitted`` / ``txFinished`` counts, ``live.submits.csv`` and the recorder's ``progress`` counters come from the observer itself:
  RECORDER-DEPENDENT.

Independence cannot be proven here. ``SourceCounterDescriptor`` records what the collector DECLARES about its counter; the contract checks the
declaration for structure and for the declarations that would make it recorder-dependent, and every output says the independence is a
declaration (``live_source_coverage_verified`` stays false everywhere). No evidence object in this module is a measurement of a real run.

Time semantics (A0k workstream E)
---------------------------------
Generator issue time (``dispatch_ms``), recorder callback observation time (``t_submit_evt_ms``), completion time (``final_ms``), health spool
write time, journal append time and the decision time are DIFFERENT quantities. A callback time is never reported as an issue time. Two
different workers' clocks are never assumed synchronized: a clock domain is a declared label, and two domains are never joined by
approximate timestamp.

Reconciliation (dispatch log vs recorder count)
-----------------------------------------------
A transaction dispatched just before a heartbeat watermark may call back just after it, so the two cumulative counts differ transiently. A
heartbeat boundary is therefore compared only when it is SETTLED: the generator dispatched nothing in the ``max_callback_lag_ms`` before it
(a declared, unmeasured assumption). Between two settled boundaries the recorder's delta must equal the generator's delta. A deficit is
missed callbacks (the span is uncovered); a surplus is a contradiction (the whole attestation is invalid). An unsettled boundary verifies
nothing. This is deliberately conservative: it can leave a span unverified, never certify one that is not.

Pure Python standard library; no process, network, model or Fabric use. Files are only read.
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .workload_coverage import (COVERAGE_SCOPES, RECORDER_FINGERPRINT, SCOPE_RECORDER_CALLBACK, SCOPE_SOURCE_STREAM,  # noqa: F401  (re-exported)
                                CoverageEvidenceError, parse_utc)

# ------------------------------------------------------------------------------------------------------------------ vocabulary
# The two coverage scopes live in ``workload_coverage`` (which derives scope from the journal and must not import this module at import time).

KIND_ISSUE_LOG = "generator_issue_log"                 # one record per issued transaction, written by the workload generator
KIND_ISSUE_COUNTER = "generator_issue_counter"         # a live cumulative counter inside the workload generator
KIND_ROUND_CONFIGURED = "round_total_configured"       # the round's configured txNumber / txDuration x rate
KIND_ROUND_OBSERVED = "round_total_observed"           # a total counted by the generator for a whole round, no interval resolution
KIND_RECORDER_DERIVED = "recorder_derived"             # derived from the recorder's callbacks or files
KIND_CALIPER_AGGREGATE = "caliper_aggregate"           # Caliper's own totals; their source is not in this repository
SOURCE_COUNTER_KINDS = (KIND_ISSUE_LOG, KIND_ISSUE_COUNTER, KIND_ROUND_CONFIGURED, KIND_ROUND_OBSERVED, KIND_RECORDER_DERIVED, KIND_CALIPER_AGGREGATE)
INTERVAL_KINDS = (KIND_ISSUE_LOG, KIND_ISSUE_COUNTER)

SEMANTICS_ISSUE = "generator_issue_time"
SEMANTICS_CALLBACK = "recorder_callback_time"
SEMANTICS_COMPLETION = "completion_time"
SEMANTICS_UNKNOWN = "unknown"
TIMESTAMP_SEMANTICS = (SEMANTICS_ISSUE, SEMANTICS_CALLBACK, SEMANTICS_COMPLETION, SEMANTICS_UNKNOWN)
RESOLUTIONS = ("interval", "round_total")

# strings in ``derived_from`` that mean the count comes from the observer being checked
RECORDER_MARKERS = ("recorder", "txsubmitted", "txfinished", "tx_submitted", "tx_finished", "callback", "live.submits", "submits.csv",
                    "observer", "txstatus")

CLASS_INTERVAL = "SOURCE-INDEPENDENT AND INTERVAL-SUITABLE"
CLASS_ROUND_ONLY = "SOURCE-INDEPENDENT BUT ROUND-ONLY"
CLASS_RECORDER_DEPENDENT = "RECORDER-DEPENDENT"
CLASS_UNVERIFIABLE = "INCOMPLETE OR UNVERIFIABLE"
CLASS_NOT_AVAILABLE = "NOT AVAILABLE"
FEASIBILITY_CLASSES = (CLASS_INTERVAL, CLASS_ROUND_ONLY, CLASS_RECORDER_DEPENDENT, CLASS_UNVERIFIABLE, CLASS_NOT_AVAILABLE)

# reason codes (machine-readable; they explain why a zero stays unknown)
RC_COUNTER_NOT_DECLARED = "source_counter_not_declared"
RC_DESCRIPTOR_MISSING = "source_counter_descriptor_missing"
RC_DESCRIPTOR_INVALID = "source_counter_descriptor_invalid"
RC_RECORDER_DERIVED = "source_counter_recorder_derived"
RC_ROUND_ONLY = "source_counter_round_only"
RC_UNVERIFIABLE = "source_counter_unverifiable"
RC_TIMESTAMP_SEMANTICS = "source_counter_timestamp_semantics"
RC_INDEPENDENCE_UNPROVEN = "source_independence_unproven"
RC_INTERVAL_EVIDENCE = "source_counter_independence_declared"


class SourceEvidenceError(CoverageEvidenceError):
    """Source-count evidence is malformed or contradicts the recorder. A subclass of ``CoverageEvidenceError``, so a coverage snapshot turns
    it into an INVALID attestation (the whole attestation, never a part)."""


# ------------------------------------------------------------------------------------------------------------------ descriptor
@dataclass(frozen=True)
class SourceClassification:
    label: str
    certifies_intervals: bool
    reason_code: str
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {"class": self.label, "certifies_intervals": self.certifies_intervals, "reason_code": self.reason_code, "detail": self.detail,
                "independence": "DECLARED by the collector; structurally checked only; never proven here"}


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def classify_descriptor(descriptor: Any) -> SourceClassification:
    """Classify a declared source counter. Only a generator-side, interval-resolution, issue-time counter that is declared independent of the
    recorder certifies intervals. Everything else is told apart by WHY it cannot."""
    if descriptor is None:
        return SourceClassification(CLASS_NOT_AVAILABLE, False, RC_DESCRIPTOR_MISSING,
                                    "a source count was reported without a source_counter descriptor: its kind, independence and time semantics are unknown")
    if not isinstance(descriptor, Mapping):
        return SourceClassification(CLASS_UNVERIFIABLE, False, RC_DESCRIPTOR_INVALID, "source_counter must be an object")
    kind = descriptor.get("kind")
    if kind not in SOURCE_COUNTER_KINDS:
        return SourceClassification(CLASS_UNVERIFIABLE, False, RC_DESCRIPTOR_INVALID, f"source_counter.kind {kind!r} is not one of {SOURCE_COUNTER_KINDS}")
    derived = _text(descriptor.get("derived_from"))
    if kind == KIND_RECORDER_DERIVED or descriptor.get("independent_of_recorder") is False \
            or (derived is not None and any(marker in derived.lower() for marker in RECORDER_MARKERS)):
        return SourceClassification(CLASS_RECORDER_DEPENDENT, False, RC_RECORDER_DERIVED,
                                    "the counter is derived from, or declared dependent on, the recorder's own callbacks or files; "
                                    "agreement with itself proves nothing")
    if kind in (KIND_ROUND_CONFIGURED, KIND_ROUND_OBSERVED):
        why = ("a configured round total is intent, not an observed issued count" if kind == KIND_ROUND_CONFIGURED
               else "a round total has no interval resolution; it cannot place an event in a one-second bin")
        return SourceClassification(CLASS_ROUND_ONLY, False, RC_ROUND_ONLY, why)
    if kind == KIND_CALIPER_AGGREGATE:
        return SourceClassification(CLASS_UNVERIFIABLE, False, RC_UNVERIFIABLE,
                                    "Caliper's own totals are fed by the same submit events the recorder sees; their source is not in this repository")
    # an interval kind: every proof field must be present and consistent
    missing = [k for k in ("counter_id", "derived_from") if _text(descriptor.get(k)) is None]
    if descriptor.get("independent_of_recorder") is not True:
        missing.append("independent_of_recorder")
    if missing:
        return SourceClassification(CLASS_UNVERIFIABLE, False, RC_INDEPENDENCE_UNPROVEN,
                                    f"no independence declaration: missing or not true: {', '.join(missing)}")
    if descriptor.get("timestamp_semantics") != SEMANTICS_ISSUE:
        return SourceClassification(CLASS_UNVERIFIABLE, False, RC_TIMESTAMP_SEMANTICS,
                                    f"timestamp_semantics is {descriptor.get('timestamp_semantics')!r}; an interval claim needs {SEMANTICS_ISSUE!r}, "
                                    "and a callback or completion time is never an issue time")
    if descriptor.get("resolution") != "interval":
        return SourceClassification(CLASS_ROUND_ONLY, False, RC_ROUND_ONLY, f"resolution is {descriptor.get('resolution')!r}, not 'interval'")
    return SourceClassification(CLASS_INTERVAL, True, RC_INTERVAL_EVIDENCE,
                                "generator-side, interval-resolution, issue-time counter declared independent of the recorder")


# ------------------------------------------------------------------------------------------------------------------ spans result
@dataclass(frozen=True)
class SourceSpans:
    """What the source evidence verifies for ONE coverage evaluation. ``verified`` are merged ``[start, end)`` spans in which the recorder's
    count equals the source's count; ``mismatches`` are spans where it does not (uncovered)."""

    verified: tuple = ()
    mismatches: tuple = ()
    claims: tuple = ()
    independence_claims: tuple = ()
    classifications: Mapping[str, Any] = field(default_factory=dict)
    rejections: tuple = ()            # (reason_code, instance, detail)
    evidence_kind: str = "none"

    def reason_codes(self) -> list[str]:
        return sorted({code for code, _i, _d in self.rejections})


def merge_spans(spans: Iterable[tuple]) -> list:
    out: list = []
    for start, end in sorted(spans):
        if end <= start:
            continue
        if out and start <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], end))
        else:
            out.append((start, end))
    return out


def inside(spans: Sequence, start: datetime, end: datetime) -> bool:
    index = bisect.bisect_right([s for s, _ in spans], start) - 1
    return index >= 0 and end <= spans[index][1]


# ------------------------------------------------------------------------------------------------------ journal-reported counts
def journal_source_spans(events: Sequence[Mapping[str, Any]], derivation: Any, as_of: datetime) -> SourceSpans:
    """Source-count evidence carried inside the journal rows (``source_events_expected`` on heartbeats, ``source_counter`` on the start row).

    This is the live route: a provider inside the recorder reports a cumulative issued count at each heartbeat. The comparison is exact (no
    lag allowance), which is conservative: an event in flight across a watermark makes the span uncovered rather than verified. A count whose
    start row carries no usable descriptor, or whose declaration makes it recorder-dependent, round-only or unverifiable, is recorded but never
    verifies a span.
    """
    verified: list = []
    mismatches: list = []
    claims: set[str] = set()
    independence: set[str] = set()
    rejections: list = []
    classifications: dict[str, Any] = {}
    state: dict[str, Any] = {"inst": None, "declared": False, "cert": False, "base": None}
    covered = derivation.covered
    for index, ev in enumerate(events, 1):
        if parse_utc(ev["t_utc"], "t_utc") > as_of:
            break
        kind, instance = ev["kind"], ev["collector_instance"]
        if kind == "start":
            declared = bool(ev.get("source_count_declared"))
            if declared:
                verdict = classify_descriptor(ev.get("source_counter"))
            else:
                verdict = SourceClassification(CLASS_NOT_AVAILABLE, False, RC_COUNTER_NOT_DECLARED, "the start row declared no source count")
            classifications[instance] = verdict.as_dict()
            if declared and not verdict.certifies_intervals:
                rejections.append((verdict.reason_code, instance, verdict.detail))
            if not declared:
                rejections.append((verdict.reason_code, instance, verdict.detail))
            start = derivation.instances[instance]["observation_start"]
            state.update(inst=instance, declared=declared, cert=declared and verdict.certifies_intervals,
                         base=(start, 0, 0) if declared and verdict.certifies_intervals else None)
        elif kind in ("error", "recovered"):
            state["base"] = None                                    # the span around an error is never source-verified
        elif kind in ("heartbeat", "stop"):
            watermark = parse_utc(ev["observed_through_utc"], "observed_through_utc")
            received = ev["events_received"]
            if ev.get("health", "healthy") != "healthy" or ev.get("reason") == "abort":
                state["base"] = None                                # an unhealthy report is never source-verified; the next one only re-establishes the baseline
                continue
            expected = ev.get("source_events_expected")
            if expected is None:
                continue
            if not state["declared"]:
                raise SourceEvidenceError("source_count_undeclared", f"journal record {index}: a source count appeared although the start did not declare one")
            if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
                raise SourceEvidenceError("event_field_invalid", f"journal record {index}: source_events_expected must be a non-negative integer")
            basis = ev.get("source_basis")
            if not isinstance(basis, str) or not basis.strip():
                raise SourceEvidenceError("event_field_missing", f"journal record {index}: a source count needs a source_basis")
            claims.add(basis)
            independence.add(str(bool(ev.get("source_independence_claim"))))
            if not state["cert"]:
                continue                                            # reported, recorded, never verifying
            if ev.get("source_independence_claim") is not True:
                rejections.append((RC_INDEPENDENCE_UNPROVEN, instance, f"journal record {index}: the heartbeat does not claim independence"))
                state["base"] = None
                continue
            previous = state["base"]
            if previous is not None:
                p_wm, p_exp, p_rec = previous
                if expected < p_exp or received < p_rec:
                    raise SourceEvidenceError("source_count_regressed", f"journal record {index}: a cumulative count moved backwards")
                d_exp, d_rec = expected - p_exp, received - p_rec
                if watermark == p_wm:
                    if d_exp or d_rec:
                        raise SourceEvidenceError("contradictory_health_evidence", f"journal record {index}: counts changed while the watermark did not")
                elif inside(covered, p_wm, watermark):
                    if d_exp == d_rec:
                        verified.append((p_wm, watermark))
                    else:
                        mismatches.append((p_wm, watermark, "interrupted_collection",
                                           f"source_count_mismatch: source counted {d_exp}, recorder {d_rec} (missed callbacks)"))
            state["base"] = (watermark, expected, received)
    return SourceSpans(tuple(merge_spans(verified)), tuple(mismatches), tuple(sorted(claims)), tuple(sorted(independence)), classifications,
                       tuple(rejections), "journal_reported_counts")


# ------------------------------------------------------------------------------------------------------------- dispatch log
DISPATCH_HEADER = ("seq", "intended_offset_ms", "intended_ms", "dispatch_ms", "lag_ms")
OVERFLOW_HEADER = ("requested_seq", "observed_ms")
DISPATCH_DESCRIPTOR_KIND = KIND_ISSUE_LOG
DISPATCH_DERIVED_FROM = "workload_module_dispatch_log"


@dataclass(frozen=True)
class DispatchLogEvidence:
    """Generator-side per-transaction issuance records of ONE worker for ONE round (the ``e1_trace_replay.js`` dispatch log).

    Written by the workload module BEFORE ``sendRequests``, so it does not pass through the recorder's callbacks. The row times are the
    generator's ``Date.now()`` at dispatch (``generator_issue_time``), in the ``clock_domain`` the caller declares. Independence from the
    recorder is a declaration of the architecture, not a measurement; ``max_callback_lag_ms`` is an unmeasured assumption.
    """

    descriptor: Mapping[str, Any]
    worker: str
    clock_domain: str
    run_id: str
    instance: str
    max_callback_lag_ms: int
    dispatch_ms: tuple                   # (seq, dispatch_ms) in FILE order
    overflow_rows: int = 0
    duplicates_dropped: int = 0
    torn_tail: bool = False
    log_sha256: str | None = None

    def __post_init__(self) -> None:
        for name in ("worker", "clock_domain", "run_id", "instance"):
            if _text(getattr(self, name)) is None:
                raise SourceEvidenceError("source_evidence_identity_missing", f"dispatch evidence needs a non-empty {name}")
        lag = self.max_callback_lag_ms
        if isinstance(lag, bool) or not isinstance(lag, int) or lag < 0:
            raise SourceEvidenceError("source_evidence_policy_invalid", "max_callback_lag_ms must be a non-negative whole number (declared, unmeasured)")
        verdict = classify_descriptor(self.descriptor)
        if not verdict.certifies_intervals or self.descriptor.get("kind") != KIND_ISSUE_LOG:
            raise SourceEvidenceError("source_descriptor_not_interval_suitable", f"{verdict.label}: {verdict.detail}")
        if self.descriptor.get("derived_from") != DISPATCH_DERIVED_FROM:
            raise SourceEvidenceError("source_descriptor_not_interval_suitable", f"derived_from must be {DISPATCH_DERIVED_FROM!r}")
        previous = None
        seen: dict[int, int] = {}
        for seq, stamp in self.dispatch_ms:
            if previous is not None and stamp < previous:
                raise SourceEvidenceError("dispatch_clock_regression", f"dispatch_ms moved backwards in file order at seq {seq}: a wall-clock step, or a reordered log")
            previous = stamp
            if seq in seen:
                raise SourceEvidenceError("dispatch_seq_duplicate", f"seq {seq} appears twice")
            seen[seq] = stamp
        if seen and sorted(seen) != list(range(len(seen))):
            raise SourceEvidenceError("dispatch_seq_gap", f"dispatch seq values are not contiguous from 0 (n={len(seen)}): a row is missing from the log")

    # ------------------------------------------------------------------------------------------------ construction
    @classmethod
    def from_text(cls, text: str, *, worker: str, clock_domain: str, run_id: str, instance: str, max_callback_lag_ms: int,
                  overflow_text: str | None = None, descriptor: Mapping[str, Any] | None = None) -> "DispatchLogEvidence":
        lines = text.split("\n")
        torn = bool(lines[-1])
        complete = lines[:-1]
        if not complete or complete[0].strip() != ",".join(DISPATCH_HEADER):
            raise SourceEvidenceError("dispatch_log_schema_mismatch", f"the dispatch log must start with the header {','.join(DISPATCH_HEADER)}")
        rows: dict[int, tuple] = {}
        ordered: list = []
        duplicates = 0
        for number, line in enumerate(complete[1:], 2):
            parts = line.strip().split(",")
            try:
                if len(parts) != len(DISPATCH_HEADER):
                    raise ValueError
                seq, offset, intended, dispatched, lag = (int(p) for p in parts)
            except ValueError as exc:
                raise SourceEvidenceError("dispatch_row_malformed", f"dispatch log line {number} is not five integers") from exc
            if seq < 0 or dispatched <= 0:
                raise SourceEvidenceError("dispatch_row_invalid", f"dispatch log line {number}: seq >= 0 and a positive epoch ms are required")
            if lag != dispatched - intended:
                raise SourceEvidenceError("dispatch_row_inconsistent", f"dispatch log line {number}: lag_ms is not dispatch_ms - intended_ms")
            row = (seq, offset, intended, dispatched, lag)
            if seq in rows:
                if rows[seq] != row:                              # a retry or a second send under one seq: the count is ambiguous
                    raise SourceEvidenceError("dispatch_seq_conflict", f"seq {seq} appears twice with different content (retry or double send)")
                duplicates += 1
                continue
            rows[seq] = row
            ordered.append((seq, dispatched))
        overflow = 0
        if overflow_text is not None:
            o_lines = overflow_text.split("\n")
            if not o_lines or o_lines[0].strip() != ",".join(OVERFLOW_HEADER):
                raise SourceEvidenceError("overflow_log_schema_mismatch", f"the overflow log must start with the header {','.join(OVERFLOW_HEADER)}")
            overflow = sum(1 for line in o_lines[1:-1] if line.strip())
        merged = dict(descriptor) if descriptor is not None else {
            "counter_id": f"dispatch-log:{run_id}:{worker}", "kind": DISPATCH_DESCRIPTOR_KIND, "independent_of_recorder": True,
            "derived_from": DISPATCH_DERIVED_FROM, "timestamp_semantics": SEMANTICS_ISSUE, "resolution": "interval"}
        return cls(merged, worker, clock_domain, run_id, instance, max_callback_lag_ms, tuple(ordered), overflow, duplicates, torn,
                   hashlib.sha256(text.encode("utf-8")).hexdigest())

    @classmethod
    def from_file(cls, path: Path, **kwargs: Any) -> "DispatchLogEvidence":
        path = Path(path)
        overflow = Path(f"{path}.overflow.csv")
        return cls.from_text(path.read_bytes().decode("utf-8"), overflow_text=overflow.read_bytes().decode("utf-8") if overflow.exists() else None, **kwargs)

    # --------------------------------------------------------------------------------------------------- queries
    def issued_through(self, through_ms: int, *, start_ms: int = 0, as_of_ms: int | None = None) -> int:
        """Transactions dispatched in ``[start_ms, through_ms)`` that already existed in the log at ``as_of_ms`` (nothing from the future)."""
        stamps = [stamp for _seq, stamp in self.dispatch_ms]                      # non-decreasing: validated at construction
        if as_of_ms is not None:
            stamps = stamps[:bisect.bisect_right(stamps, as_of_ms)]
        return bisect.bisect_left(stamps, through_ms) - bisect.bisect_left(stamps, start_ms)

    def last_complete_dispatch_ms(self) -> int | None:
        return self.dispatch_ms[-1][1] if self.dispatch_ms else None

    def per_second_counts(self) -> dict[int, int]:
        """Issued transactions per UTC second (key = epoch second). Issue time, never callback time."""
        counts: dict[int, int] = {}
        for _seq, stamp in self.dispatch_ms:
            counts[stamp // 1000] = counts.get(stamp // 1000, 0) + 1
        return counts

    def timestamp_provenance(self) -> dict[str, Any]:
        return {"event_time": SEMANTICS_ISSUE, "clock_domain": self.clock_domain, "worker": self.worker, "raw_unit": "epoch_ms (workload module Date.now())",
                "not_a_callback_time": True, "log_sha256": self.log_sha256, "assumption": "max_callback_lag_ms is declared, not measured"}

    # ----------------------------------------------------------------------------------------------- reconciliation
    def spans(self, derivation: Any, as_of: datetime, events: Sequence[Mapping[str, Any]] | None = None) -> SourceSpans:
        """Reconcile the recorder's cumulative counts with the generator's, at settled heartbeat boundaries only (see the module docstring)."""
        if self.instance not in derivation.instances:
            raise SourceEvidenceError("source_evidence_instance_unknown", f"the dispatch log names collector instance {self.instance!r}, which has no start record")
        origin_ms = int(derivation.instances[self.instance]["observation_start"].timestamp() * 1000)
        as_of_ms = int(as_of.timestamp() * 1000)
        stamps = [stamp for _seq, stamp in self.dispatch_ms]
        stamps = stamps[:bisect.bisect_right(stamps, as_of_ms)]                   # a row written after the decision time does not exist yet
        origin_index = bisect.bisect_left(stamps, origin_ms)
        verified: list = []
        mismatches: list = []
        base: tuple | None = None                                   # (watermark, issued, received) at the last settled boundary
        if bisect.bisect_left(stamps, origin_ms - self.max_callback_lag_ms) == origin_index:
            base = (derivation.instances[self.instance]["observation_start"], 0, 0)        # nothing was in flight at the observation start
        for instance, watermark, received, _seq, _t in derivation.heartbeats:
            if instance != self.instance:
                continue
            wm_ms = int(watermark.timestamp() * 1000)
            if wm_ms > as_of_ms + 1:
                continue
            through = bisect.bisect_left(stamps, wm_ms)
            issued = through - origin_index
            settled = bisect.bisect_left(stamps, wm_ms - self.max_callback_lag_ms) == through      # nothing dispatched in the last max_callback_lag_ms
            if not settled:
                continue
            if base is not None:
                b_wm, b_issued, b_received = base
                d_issued, d_received = issued - b_issued, received - b_received
                if d_received > d_issued:
                    raise SourceEvidenceError("recorder_exceeds_source", f"between {b_wm.isoformat()} and {watermark.isoformat()} the recorder counted "
                                                                         f"{d_received} transaction(s), the generator issued {d_issued}")
                if watermark > b_wm:
                    if inside(derivation.covered, b_wm, watermark):
                        if d_issued == d_received:
                            verified.append((b_wm, watermark))
                        else:
                            mismatches.append((b_wm, watermark, "interrupted_collection",
                                               f"source_count_mismatch: source counted {d_issued}, recorder {d_received} (missed callbacks)"))
            base = (watermark, issued, received)
        rejections: list = []
        if self.torn_tail and self.dispatch_ms:
            tail = datetime.fromtimestamp(self.dispatch_ms[-1][1] / 1000.0, tz=timezone.utc)
            mismatches.append((tail.replace(microsecond=0), datetime(9999, 12, 31, tzinfo=timezone.utc), "interrupted_collection",
                               "dispatch_log_torn_tail: an interrupted final write; nothing after the last complete row is certified"))
        classification = classify_descriptor(self.descriptor)
        return SourceSpans(tuple(merge_spans(verified)), tuple(mismatches), (DISPATCH_DERIVED_FROM,), ("True",),
                           {self.instance: classification.as_dict()}, tuple(rejections), "dispatch_log")


# ------------------------------------------------------------------------------------------------------- feasibility table
SOURCE_CANDIDATES: tuple = (
    {"id": "trace_replay_dispatch_log", "class": CLASS_INTERVAL, "worker_limit": 1,
     "evidence": ["fabric-testbed/caliper/workloads/e1_trace_replay.js:77-107 (appendFileSync to dispatchLog before sutAdapter.sendRequests)",
                  "fabric-testbed/caliper/workloads/e1_trace_replay.js:60 (exactly 1 worker)", "p4_replay/fidelity.py:100-108 (read_dispatch_log)"],
     "limits": ["post-hoc file, not a live provider; the recorder cannot read it without a reader that is not written",
                "independence from the recorder is architectural (written before sendRequests), not measured",
                "same worker process and Date.now() as the recorder by the createTxObserver(options, messenger, workerIndex) signature; not verified against Caliper",
                "a log with no closing marker cannot tell a stopped generator from a crashed one except through a torn tail",
                "used by the P4 replay harness, not by the pilot renderer e1_la2_render.py"]},
    {"id": "create_asset_in_memory_seq", "class": CLASS_NOT_AVAILABLE, "worker_limit": 1,
     "evidence": ["fabric-testbed/caliper/workloads/e1_create_asset.js:28,38,46-48 (this.seq incremented per submitTransaction, never exposed or logged)",
                  "scripts/e1/e1_la2_render.py:119 (the pilot renderer uses this module)"],
     "limits": ["a counter exists in memory only; adding a log or a provider is instrumentation that does not exist"]},
    {"id": "configured_round_total", "class": CLASS_ROUND_ONLY, "worker_limit": 1,
     "evidence": ["scripts/e1/e1_la2_render.py:120-121 (txNumber or txDuration with fixed-rate tps)", "p4_replay/caliper_check.py F5-F8 (txNumber/worker, no wall-clock stop)",
                  "fabric-testbed/caliper/workloads/e1_trace_replay.js:78-83 (Caliper 0.6 can issue one trailing callback)"],
     "limits": ["configuration is intent; the issued count can differ", "no interval resolution"]},
    {"id": "recorder_submit_callbacks", "class": CLASS_RECORDER_DEPENDENT, "worker_limit": 1,
     "evidence": ["the live recorder (A0c, scripts/e1/caliper/): txSubmitted(count) -> live.submits.csv", "the health recorder (A0j, scripts/e1/caliper/): txSubmitted lines 188-204"],
     "limits": ["the observer's own stream; a missed callback is missing from every derived count"]},
    {"id": "recorder_progress_counters", "class": CLASS_RECORDER_DEPENDENT, "worker_limit": 1,
     "evidence": ["the health recorder (A0j, scripts/e1/caliper/) lines 257-261: progress.submitted_total"], "limits": ["same stream as the callbacks"]},
    {"id": "caliper_transaction_statistics", "class": CLASS_UNVERIFIABLE, "worker_limit": None,
     "evidence": ["p4_replay/caliper_check.py docstring: TransactionStatisticsCollector NOT READ; connector TxsSubmitted emission NOT READ (U1)"],
     "limits": ["Caliper is not in this repository; its counters are fed by the same TxsSubmitted events the recorder sees"]},
    {"id": "tx_ids_at_completion", "class": CLASS_UNVERIFIABLE, "worker_limit": None,
     "evidence": ["tests/e1/js/fake_caliper.js (TxStatus.GetID at txFinished)", "the health recorder (A0j, scripts/e1/caliper/): txSubmitted(count) has no id"],
     "limits": ["ids exist only at completion; a completion is not an issuance and a never-completing transaction has no id at all"]},
)


# ---------------------------------------------------------------------------------------------------------- timestamp inventory
TIMESTAMP_INVENTORY: tuple = (
    {"name": "workload issuance (dispatch_ms)", "semantics": SEMANTICS_ISSUE, "clock": "worker process Date.now()", "status": "AVAILABLE",
     "where": "e1_trace_replay.js dispatch log only; e1_create_asset.js records none"},
    {"name": "transaction submission (Caliper txsSubmitted)", "semantics": "caliper_submit_event", "clock": "not read", "status": "NOT AVAILABLE",
     "where": "the connector's emission point is unread (p4_replay/caliper_check.py U1)"},
    {"name": "recorder callback (t_submit_evt_ms)", "semantics": SEMANTICS_CALLBACK, "clock": "worker process Date.now()", "status": "AVAILABLE",
     "where": "live.submits.csv; the moment the observer saw TxSubmitted, never the issue time"},
    {"name": "transaction completion (create_ms / final_ms)", "semantics": SEMANTICS_COMPLETION, "clock": "Caliper TxStatus times", "status": "AVAILABLE",
     "where": "recorder.csv; never converted to an arrival time"},
    {"name": "health-spool write (t_utc, mono_ms)", "semantics": "observation_time", "clock": "worker Date.now() and process.hrtime", "status": "AVAILABLE",
     "where": "recorder_health_spool/1; mono_ms is relative to the recorder's activate"},
    {"name": "collector-journal append", "semantics": "observation_time", "clock": "the spool's own t_utc (replay adds no ingest time)", "status": "INFERRED",
     "where": "the time a row became available to a decision process is not recorded"},
    {"name": "ledger observation", "semantics": "ledger_commit_observation", "clock": "ledger collector", "status": "AVAILABLE",
     "where": "ledger_evidence.py; committed blocks, not arrivals"},
    {"name": "Stage 3 decision", "semantics": "decision_time", "clock": "decision process", "status": "AVAILABLE", "where": "stage3_shadow_runner.py; the as_of of every coverage read"},
)


def require_issue_time(evidence_semantics: str, *, claim: str) -> None:
    """Refuse to present a non-issue timestamp as the source's issue time."""
    if evidence_semantics != SEMANTICS_ISSUE:
        raise SourceEvidenceError("timestamp_semantics_conflated",
                                  f"{claim} needs {SEMANTICS_ISSUE!r}; the evidence carries {evidence_semantics!r}, which is a different quantity")


def epoch_ms_to_utc(value: Any, name: str) -> datetime:
    """A raw epoch-millisecond integer as a timezone-aware UTC time. Booleans, floats and non-positive values are refused."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SourceEvidenceError("timestamp_malformed", f"{name} must be a positive integer epoch millisecond: {value!r}")
    return datetime.fromtimestamp(value // 1000, tz=timezone.utc) + timedelta(milliseconds=value % 1000)
