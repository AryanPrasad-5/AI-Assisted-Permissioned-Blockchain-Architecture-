"""Captured workload-history adapter for the Stage 3 predictive interface (task A0h, 2026-11-21).

Turns AUTHENTIC, already-stored transaction-arrival observations into the history the frozen forecasting interface expects, without
adding any forecasting algorithm. It reuses ``workload_history.ArrivalHistoryStore`` (the shared append-only UTC-day store that
``StreamingWorkload`` subclasses) for ordering, contiguity, content-addressed prefix digests and the strict gap policy.

What the sources are (stated, not assumed)
------------------------------------------
The only stored arrival observations in this repository are the two Flashbots mempool days (2026-09-21 and 2026-10-12): FIRST-SEEN
(reception) timestamps of Ethereum mainnet transactions, millisecond precision, UTC, one unique transaction hash per row. They are
Ethereum arrivals. They are NOT Fabric submissions, endorsements or commits, and every record produced here says so
(``CapturedSource.represents``).

Zero versus unknown coverage (the central rule)
-----------------------------------------------
A stored 1 s series cannot tell "no transaction arrived" from "the collector was not listening": a missing second and a genuine zero
both become a 0 count (the benchmark series were built with ``bincount``). The strict live gap policy of ``workload_history`` says a
missing second is never zero. This adapter therefore classifies every zero-count second:

* inside a ``CoverageAttestation`` interval (the capture source was verified healthy there): a GENUINE ZERO, added as 0;
* otherwise, under the default ``ZERO_POLICY_VERIFIED_ONLY``: UNKNOWN COVERAGE, recorded as ``unavailable``. The UTC-day prefix is
  then incomplete from that second on and forecasting abstains, exactly as it would live;
* only if the caller explicitly chooses ``ZERO_POLICY_BENCHMARK_ZERO_FILL``: counted as 0, with the label that coverage is unverified.
  That reproduces the zero-fill semantics of the stored benchmark series (the input of the frozen forecast). It is never the default
  and the evidence record says ``live_semantics: false``.

A second absent from the series is a hole (``missing``), never a zero. A positive count proves the source was listening, so it is
always OBSERVED.

A0i (2026-11-21) adds a third, separate answer: ``coverage=VerifiedCoverage`` (``workload_coverage``), evidence derived from a hash-chained collector-health
journal. Then EVERY bin, positive or zero, must lie inside verified coverage; evidence is read as of each decision cutoff and never later; a second newer than
the last evidence is PENDING (deferred, not recorded) so a later heartbeat can still certify it. ``evidence()`` always carries ``coverage_mode`` and
``coverage_provenance`` (``contract_satisfied`` is true only for verified collector-health coverage). The A0h routes are unchanged.

No network, process, Fabric, Docker, model or forecaster is used. Files are only read.
"""

from __future__ import annotations

import bisect
import csv
import gzip
import hashlib
import io
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .workload_coverage import (ACCEPTED_CLASSES, MODE_ATTESTED, MODE_BENCHMARK, MODE_LEGACY_ASSERTED, MODE_NO_EVIDENCE, PROVENANCE_SCHEMA,
                                CoverageClass, VerifiedCoverage)
from .workload_history import ArrivalHistoryStore, WorkloadHistoryError

ADAPTER_SCHEMA = "captured_workload_history/1"
ATTESTATION_SCHEMA = "captured_history_coverage/1"
SERIES_SCHEMA = "flashbots_series_1s/1"        # columns timestamp_utc, arrival_count, date_utc, frequency_seconds
BIN_SECONDS = 1
SECONDS_PER_DAY = 86_400
MIN_HISTORY_BINS = 600
SOURCE_KIND_ETHEREUM_MEMPOOL = "ethereum_mempool_first_seen"
SOURCE_KIND_CALIPER_CLIENT = "caliper_client_submit_events"      # recorder-derived arrivals (``recorder_coverage.RecorderArrivalSource``)
REPRESENTS = ("first-seen (reception) times of Ethereum mainnet transactions seen by one mempool collector; NOT Fabric "
              "submissions, endorsements or commits")
UNIT = "arrivals per 1 s bin (= transactions/s)"
DUPLICATE_HANDLING = ("by transaction id: the first occurrence is kept and counted; later occurrences are dropped and "
                      "counted in duplicates_dropped; a row without a transaction id is refused")
ZERO_POLICY_VERIFIED_ONLY = "verified_only"
ZERO_POLICY_BENCHMARK_ZERO_FILL = "benchmark_zero_fill"
ZERO_POLICIES = (ZERO_POLICY_VERIFIED_ONLY, ZERO_POLICY_BENCHMARK_ZERO_FILL)
ZERO_UNVERIFIED_REASON = "zero_count_coverage_unverified"

VALID_COMPLETE = "complete_prefix"
VALID_INSUFFICIENT = "insufficient_history"
VALID_INCOMPLETE = "incomplete_unknown_coverage"
VALID_EMPTY = "empty"
VALID_STALE_COVERAGE = "stale_coverage_evidence"          # A0i: the collector evidence available at the decision time ends before the cutoff


class CapturedHistoryError(ValueError):
    """A source, a record or a request violates the captured-history contract. ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _utc(value: Any, name: str) -> datetime:
    try:
        moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise CapturedHistoryError("timestamp_malformed", f"{name} is not an ISO-8601 timestamp: {value!r}") from exc
    if moment.tzinfo is None:
        raise CapturedHistoryError("timestamp_naive", f"{name} has no timezone: {value!r}")
    return moment.astimezone(timezone.utc)


def _whole(moment: datetime, name: str) -> datetime:
    if moment.microsecond:
        raise CapturedHistoryError("timestamp_not_whole_second", f"{name} must be a whole UTC second: {moment.isoformat()}")
    return moment


def _day_start(day: str) -> datetime:
    try:
        parsed = datetime.strptime(day, "%Y-%m-%d")
    except ValueError as exc:
        raise CapturedHistoryError("utc_day_malformed", f"utc_day must be YYYY-MM-DD: {day!r}") from exc
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)


def sha256_stream(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------------------------------------------- identity
@dataclass(frozen=True)
class CapturedSource:
    """Identity of a captured dataset. Carried into every evidence record; never inferred."""

    source_id: str
    utc_day: str
    dataset_path: str | None = None
    dataset_sha256: str | None = None
    kind: str = SOURCE_KIND_ETHEREUM_MEMPOOL
    schema: str = SERIES_SCHEMA
    timestamp_precision: str = "bins of 1 s built from millisecond-precision first-seen timestamps"
    timezone_name: str = "UTC"

    def __post_init__(self) -> None:
        if not self.source_id or not isinstance(self.source_id, str):
            raise CapturedHistoryError("source_identity_missing", "a captured source needs a source_id")
        _day_start(self.utc_day)

    def as_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "kind": self.kind, "represents": REPRESENTS, "utc_day": self.utc_day,
                "dataset_path": self.dataset_path, "dataset_sha256": self.dataset_sha256, "source_schema": self.schema,
                "timestamp_precision": self.timestamp_precision, "timezone": self.timezone_name, "unit": UNIT,
                "bin_seconds": BIN_SECONDS, "is_fabric_observation": False}


@dataclass(frozen=True)
class BinnedDay:
    """One UTC day of 1 s counts. ``counts[i]`` is the count of second ``i`` of the day, or None when the series has no row for it."""

    source: CapturedSource
    counts: tuple
    report: Mapping[str, Any]

    @property
    def day_start(self) -> datetime:
        return _day_start(self.source.utc_day)


# --------------------------------------------------------------------------------------------------------------- binning
def bin_transactions(records: Iterable[tuple[Any, Any]], source: CapturedSource) -> BinnedDay:
    """Aggregate raw ``(transaction_id, timestamp_ms)`` records of ONE UTC day into 1 s counts.

    Deterministic: the result does not depend on record order. Duplicates are resolved by transaction id (first kept). A missing
    id, a missing, non-integer, negative or out-of-day timestamp raises: dropping such a record would silently change a second's
    count and certify a coverage nobody checked.
    """
    start_ms = int(_day_start(source.utc_day).timestamp() * 1000)
    counts = [0] * SECONDS_PER_DAY
    seen: set[str] = set()
    read = duplicates = 0
    for tx_id, stamp in records:
        read += 1
        if tx_id is None or str(tx_id).strip() == "":
            raise CapturedHistoryError("transaction_id_missing", f"record {read} has no transaction id")
        if stamp is None or (isinstance(stamp, str) and not stamp.strip()):
            raise CapturedHistoryError("timestamp_missing", f"record {read} ({tx_id}) has no timestamp")
        if isinstance(stamp, bool) or isinstance(stamp, float) and (not math.isfinite(stamp) or not float(stamp).is_integer()):
            raise CapturedHistoryError("timestamp_malformed", f"record {read} ({tx_id}) timestamp is not an integer: {stamp!r}")
        try:
            millis = int(stamp)
        except (TypeError, ValueError) as exc:
            raise CapturedHistoryError("timestamp_malformed", f"record {read} ({tx_id}) timestamp is not an integer: {stamp!r}") from exc
        offset = millis - start_ms
        if offset < 0 or offset >= SECONDS_PER_DAY * 1000:
            raise CapturedHistoryError("timestamp_outside_day", f"record {read} ({tx_id}) lies outside UTC day {source.utc_day}")
        key = str(tx_id)
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        counts[offset // 1000] += 1
    report = {"input": "transaction records", "records_read": read, "unique_transactions": read - duplicates,
              "duplicates_dropped": duplicates, "duplicate_handling": DUPLICATE_HANDLING, "rows_without_series_row": 0,
              "zero_count_seconds": sum(1 for c in counts if c == 0), "bins": SECONDS_PER_DAY}
    return BinnedDay(source, tuple(counts), report)


def load_binned_series(path: Path, source: CapturedSource, *, expected_sha256: str) -> BinnedDay:
    """Read a stored ``flashbots_series_1s/1`` file (plain or gzip CSV). The file's SHA-256 must equal ``expected_sha256``.

    Rows must be whole-second UTC bins of ``source.utc_day`` in strictly increasing order with integer counts and, when the column
    is present, ``frequency_seconds == 1``. A second with no row is kept as a hole (None). The file is only read.
    """
    path = Path(path)
    actual = sha256_stream(path)
    if actual.lower() != str(expected_sha256).lower():
        raise CapturedHistoryError("dataset_hash_mismatch", f"{path.name}: sha256 {actual} is not the expected {expected_sha256}")
    opener = gzip.open if path.suffix == ".gz" else open
    day = _day_start(source.utc_day)
    counts: list = [None] * SECONDS_PER_DAY
    previous = -1
    rows = 0
    with opener(path, "rt", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if not {"timestamp_utc", "arrival_count"} <= set(reader.fieldnames or ()):
            raise CapturedHistoryError("series_schema_mismatch", "columns timestamp_utc and arrival_count are required")
        for row in reader:
            rows += 1
            moment = _whole(_utc(row["timestamp_utc"], f"row {rows} timestamp_utc"), f"row {rows} timestamp_utc")
            index = int((moment - day).total_seconds())
            if not 0 <= index < SECONDS_PER_DAY:
                raise CapturedHistoryError("timestamp_outside_day", f"row {rows} lies outside UTC day {source.utc_day}")
            if index <= previous:
                raise CapturedHistoryError("series_not_chronological", f"row {rows} is a duplicate or out of order")
            frequency = row.get("frequency_seconds")
            if frequency not in (None, "") and str(frequency).strip() != "1":
                raise CapturedHistoryError("bin_duration_mismatch", f"row {rows} has frequency_seconds={frequency!r}, expected 1")
            try:
                value = float(row["arrival_count"])
            except (TypeError, ValueError) as exc:
                raise CapturedHistoryError("count_invalid", f"row {rows} arrival_count is not numeric: {row['arrival_count']!r}") from exc
            if not math.isfinite(value) or value < 0 or not value.is_integer():
                raise CapturedHistoryError("count_invalid", f"row {rows} arrival_count must be a finite non-negative integer")
            counts[index] = int(value)
            previous = index
    holes = sum(1 for c in counts if c is None)
    report = {"input": "binned series", "rows_read": rows, "series_rows_absent": holes, "duplicates_dropped": 0,
              "duplicate_handling": "none possible in a binned series; a repeated or out-of-order row is refused",
              "zero_count_seconds": sum(1 for c in counts if c == 0), "bins": SECONDS_PER_DAY}
    return BinnedDay(CapturedSource(**{**source.__dict__, "dataset_path": str(path), "dataset_sha256": actual}), tuple(counts), report)


# ------------------------------------------------------------------------------------------------------------- coverage
@dataclass(frozen=True)
class CoverageAttestation:
    """Intervals ``[start, end)`` in which the capture source is VERIFIED to have been listening, with who verified it.

    A zero-count second inside an interval is a genuine zero. Nothing is inferred: no interval is derived from the counts.
    """

    intervals: tuple = ()
    attested_by: str = ""
    basis: str = ""

    def __post_init__(self) -> None:
        if self.intervals and (not self.attested_by or not self.basis):
            raise CapturedHistoryError("attestation_unsourced", "an attestation must say who attested it and on what basis")
        parsed, previous_end = [], None
        for start, end in self.intervals:
            s, e = _whole(_utc(start, "attested start"), "attested start"), _whole(_utc(end, "attested end"), "attested end")
            if e <= s:
                raise CapturedHistoryError("attestation_invalid", f"interval {start}..{end} is empty or reversed")
            if previous_end is not None and s < previous_end:
                raise CapturedHistoryError("attestation_invalid", "intervals must be ordered and non-overlapping")
            parsed.append((s, e))
            previous_end = e
        object.__setattr__(self, "_parsed", tuple(parsed))
        object.__setattr__(self, "_starts", tuple(s for s, _ in parsed))

    def covers(self, second: datetime) -> bool:
        index = bisect.bisect_right(self._starts, second) - 1
        return index >= 0 and second < self._parsed[index][1]

    @classmethod
    def from_file(cls, path: Path) -> "CoverageAttestation":
        doc = json.loads(Path(path).read_text(encoding="utf-8"))
        if doc.get("schema") != ATTESTATION_SCHEMA:
            raise CapturedHistoryError("attestation_schema", f"schema must be {ATTESTATION_SCHEMA!r}")
        return cls(tuple((i["start_utc"], i["end_utc"]) for i in doc.get("intervals", ())), str(doc.get("attested_by", "")),
                   str(doc.get("basis", "")))

    def as_dict(self) -> dict[str, Any]:
        return {"schema": ATTESTATION_SCHEMA, "attested_by": self.attested_by, "basis": self.basis,
                "intervals": [[s.isoformat(), e.isoformat()] for s, e in self._parsed]}


# ------------------------------------------------------------------------------------------------------------- history
class CapturedHistory:
    """A captured day fed, second by second and never ahead of a decision cutoff, into an ``ArrivalHistoryStore``.

    ``advance_to(cutoff)`` adds every bin whose END is at or before the cutoff (so no bin that was not yet complete at the cutoff
    is ever visible: no future-data leakage) and returns nothing the store does not already hold. ``store`` is handed to
    ``ProductionTelemetryProvider(workload=...)``, which reads it through its atomic ``snapshot``.
    """

    def __init__(self, binned: BinnedDay, *, zero_policy: str = ZERO_POLICY_VERIFIED_ONLY,
                 attestation: CoverageAttestation | None = None, max_age_s: float = 5.0, forecast_horizon_s: int = 60,
                 min_history_bins: int = MIN_HISTORY_BINS, coverage: VerifiedCoverage | None = None):
        if zero_policy not in ZERO_POLICIES:
            raise CapturedHistoryError("zero_policy_invalid", f"zero_policy must be one of {ZERO_POLICIES}")
        # A0i: ``coverage`` is the verified collector-health evidence (workload_coverage). It is a third, separate answer to "may a zero be
        # believed?", so it cannot be combined with the benchmark zero-fill or with the A0h asserted-intervals attestation.
        if coverage is not None:
            if zero_policy == ZERO_POLICY_BENCHMARK_ZERO_FILL or (attestation is not None and attestation.intervals):
                raise CapturedHistoryError("zero_policy_conflict", "verified coverage cannot be combined with the benchmark zero-fill policy "
                                                                   "or an asserted-intervals attestation; choose one")
            if coverage.source_id != binned.source.source_id:
                raise CapturedHistoryError("coverage_source_mismatch",
                                           f"coverage evidence is for source {coverage.source_id!r}, the captured day is {binned.source.source_id!r}")
            # A0k: arrivals seen by a Caliper recorder may only be certified by recorder-scope coverage (whose zeros need independent source
            # evidence). A journal that declares another scope, or none, cannot be attached to them, so no wrapper reaches a heartbeat-only zero.
            if binned.source.kind == SOURCE_KIND_CALIPER_CLIENT:
                scopes = coverage.declared_scopes()
                if scopes != {"recorder_callback"}:
                    raise CapturedHistoryError("coverage_scope_source_mismatch",
                                               f"recorder-derived arrivals need coverage declaring scope 'recorder_callback', found {sorted(map(str, scopes))}")
        self.coverage = coverage
        self.coverage_mode = (coverage.mode if coverage is not None else MODE_BENCHMARK if zero_policy == ZERO_POLICY_BENCHMARK_ZERO_FILL
                              else MODE_LEGACY_ASSERTED if attestation is not None and attestation.intervals else MODE_NO_EVIDENCE)
        self.coverage_counts = {cls.value: 0 for cls in CoverageClass}
        self._deferred: dict[str, Any] | None = None
        if zero_policy == ZERO_POLICY_BENCHMARK_ZERO_FILL and attestation is not None and attestation.intervals:
            raise CapturedHistoryError("zero_policy_conflict", "an attestation and the benchmark zero-fill policy are two different "
                                                               "answers to the same question; choose one")
        if len(binned.counts) != SECONDS_PER_DAY:
            raise CapturedHistoryError("bin_duration_mismatch", "a captured day must hold 86,400 one-second bins")
        self.binned, self.zero_policy = binned, zero_policy
        self.attestation = attestation if attestation is not None else CoverageAttestation()
        self.forecast_horizon_s, self.min_history_bins, self.max_age_s = forecast_horizon_s, min_history_bins, float(max_age_s)
        self.store = ArrivalHistoryStore(binned.source.source_id, max_age_s=self.max_age_s)
        self._next = 0
        self._cutoff: datetime | None = None
        self.counts_fed = {"observed": 0, "zero_attested": 0, "zero_assumed_benchmark": 0, "zero_unverified_unavailable": 0,
                           "absent_from_series": 0}

    @property
    def source(self) -> CapturedSource:
        return self.binned.source

    def day_bounds(self) -> tuple[datetime, datetime]:
        start = self.binned.day_start
        return start, start + timedelta(seconds=SECONDS_PER_DAY)

    def advance_to(self, cutoff_utc: Any) -> int:
        """Feed every bin that has ended by ``cutoff_utc``. Returns the number of bins newly accounted for."""
        cutoff = _utc(cutoff_utc, "decision cutoff")
        start, end = self.day_bounds()
        if not start <= cutoff <= end:
            raise CapturedHistoryError("cutoff_outside_captured_day", f"{cutoff.isoformat()} is outside {self.source.utc_day}")
        if self._cutoff is not None and cutoff < self._cutoff:
            raise CapturedHistoryError("cutoff_regressed", "decision cutoffs must not move backwards")
        self._cutoff = cutoff
        ready = int((cutoff - start).total_seconds())          # bins [0, ready) have ended; a partial second is not a bin yet
        if self.coverage is not None:
            return self._advance_covered(cutoff, ready, start)
        fed = 0
        while self._next < ready:
            index, self._next = self._next, self._next + 1
            moment = start + timedelta(seconds=index)
            count = self.binned.counts[index]
            collected = (moment + timedelta(seconds=BIN_SECONDS)).isoformat()      # modelled collection time: the bin's end
            if count is None:
                self.counts_fed["absent_from_series"] += 1
                continue                                                           # a hole; the store marks it missing, never zero
            if count > 0 or self.zero_policy == ZERO_POLICY_BENCHMARK_ZERO_FILL or self.attestation.covers(moment):
                self.store.add(moment.isoformat(), count, collected_at_utc=collected)
                if count > 0:
                    self.counts_fed["observed"] += 1
                elif self.zero_policy == ZERO_POLICY_BENCHMARK_ZERO_FILL:
                    self.counts_fed["zero_assumed_benchmark"] += 1
                else:
                    self.counts_fed["zero_attested"] += 1
            else:
                self.store.mark_unavailable(moment.isoformat(), moment.isoformat(), reason=ZERO_UNVERIFIED_REASON,
                                            reported_at_utc=collected)
                self.counts_fed["zero_unverified_unavailable"] += 1
            fed += 1
        return fed

    def _advance_covered(self, cutoff: datetime, ready: int, start: datetime) -> int:
        """A0i route: every bin is classified against the coverage evidence AVAILABLE AT ``cutoff`` (never later evidence).

        Accepted (observed positive, verified zero) -> added. Missing -> a hole. Interrupted, gap, invalid or unknown -> recorded
        ``unavailable`` with the class as the reason. STALE (newer than the last collector evidence) -> the loop STOPS and the bin stays
        pending: nothing is fed, marked or guessed, so a later heartbeat can still certify it, and until then the history is not current.
        """
        snapshot = self.coverage.snapshot(cutoff)
        self._deferred = None
        fed = 0
        while self._next < ready:
            index = self._next
            moment = start + timedelta(seconds=index)
            count = self.binned.counts[index]
            cls, why = snapshot.classify(moment, count)
            if cls == CoverageClass.STALE_COLLECTOR_EVIDENCE:
                self._deferred = {"second_utc": moment.isoformat(), "class": cls.value, "cause": why}
                break
            self._next += 1
            self.coverage_counts[cls.value] += 1
            if cls == CoverageClass.MISSING_OBSERVATION:
                continue                                       # a hole; the store marks it missing, never zero
            collected = (moment + timedelta(seconds=BIN_SECONDS)).isoformat()
            if cls in ACCEPTED_CLASSES:
                self.store.add(moment.isoformat(), count, collected_at_utc=collected)
            else:
                self.store.mark_unavailable(moment.isoformat(), moment.isoformat(), reason=cls.value, reported_at_utc=collected)
            fed += 1
        return fed

    def coverage_provenance(self, decision_cutoff_utc: Any) -> dict[str, Any]:
        """What the coverage evidence says at ``decision_cutoff_utc``, in one block that every decision record can carry."""
        cutoff = _utc(decision_cutoff_utc, "decision cutoff")
        block: dict[str, Any] = {"schema": PROVENANCE_SCHEMA, "mode": self.coverage_mode, "source_id": self.source.source_id}
        if self.coverage is None:
            block["contract_satisfied"] = False
            block["live_quality"] = False
            fed = self.counts_fed                           # the A0h counters, mapped onto the coverage classes (the A0h loop is unchanged)
            block["class_counts"] = {name: n for name, n in (
                (CoverageClass.OBSERVED_POSITIVE.value, fed["observed"]), (CoverageClass.VERIFIED_ZERO.value, fed["zero_attested"] if self.coverage_mode == MODE_LEGACY_ASSERTED else 0),
                (CoverageClass.UNVERIFIED_ZERO_FILL.value, fed["zero_assumed_benchmark"]),
                (CoverageClass.UNKNOWN_COVERAGE_ZERO.value, fed["zero_unverified_unavailable"]),
                (CoverageClass.MISSING_OBSERVATION.value, fed["absent_from_series"])) if n}
            block["note"] = {MODE_BENCHMARK: "benchmark reproduction: reception-free seconds are zero-filled and coverage is UNVERIFIED; never live-quality",
                             MODE_LEGACY_ASSERTED: "intervals asserted by a named party (A0h); no collector-health journal verifies them",
                             MODE_NO_EVIDENCE: "no coverage evidence was supplied: every zero-count second is unknown"}[self.coverage_mode]
            return block
        snapshot = self.coverage.snapshot(cutoff)
        rejected = sum(n for name, n in self.coverage_counts.items() if name not in {c.value for c in ACCEPTED_CLASSES})
        block.update(snapshot.summary())
        block.update({"class_counts": {k: v for k, v in self.coverage_counts.items() if v},
                      "deferred": None if self._deferred is None else dict(self._deferred),
                      "contract_satisfied": bool(self.coverage_mode == MODE_ATTESTED and snapshot.valid and not rejected and self._deferred is None
                                                 and self.coverage_counts[CoverageClass.OBSERVED_POSITIVE.value]
                                                 + self.coverage_counts[CoverageClass.VERIFIED_ZERO.value] > 0),
                      "note": "no running recorder writes collector-health journals yet (A0i report, section 7); a journal is an offline fixture "
                              "until one does. A0k: a recorder-health spool can be bridged (A0j) but has never been produced by a real run, and "
                              "heartbeats are never source completeness (scope rule in workload_coverage)"})
        return block

    # ------------------------------------------------------------------------------------------------------- evidence
    def evidence(self, decision_cutoff_utc: Any) -> dict[str, Any]:
        """The history evidence for one decision: what the store would hand the forecaster at ``decision_cutoff_utc``."""
        cutoff = _utc(decision_cutoff_utc, "decision cutoff")
        times, counts, reference = self.store.snapshot(now_utc=cutoff.isoformat())
        reasons: list[str] = []
        if reference.status == "empty":
            outcome = VALID_EMPTY
            reasons.append("no bin has been recorded for this UTC day")
        elif reference.status == "incomplete":
            outcome = VALID_INCOMPLETE
            if reference.missing_seconds:
                reasons.append(f"{reference.missing_seconds} second(s) missing from the series (never zero-filled)")
            if reference.unavailable_seconds and self.coverage is not None:      # A0i: say WHICH coverage class excluded them (positives can be partial too)
                outside = {k: v for k, v in self.coverage_counts.items()
                           if v and k not in {c.value for c in ACCEPTED_CLASSES} and k != CoverageClass.MISSING_OBSERVATION.value}
                reasons.append(f"{reference.unavailable_seconds} second(s) outside verified coverage {outside}; never assumed genuine")
            elif reference.unavailable_seconds:
                reasons.append(f"{reference.unavailable_seconds} zero-count second(s) of unverified coverage (never assumed genuine)")
            reasons.append(f"complete prefix stops after {reference.complete_prefix_bins} bin(s); the frozen forecast needs the complete UTC-day prefix")
        elif len(times) < self.min_history_bins:
            outcome = VALID_INSUFFICIENT
            reasons.append(f"{len(times)} bin(s); {self.min_history_bins} required")
        else:
            outcome = VALID_COMPLETE
        if self._deferred is not None:                         # A0i: recoverable (a later heartbeat may certify it), so it never outranks an incomplete prefix
            reasons.append(f"second {self._deferred['second_utc']} is pending: {self._deferred['cause']}")
            if outcome in (VALID_COMPLETE, VALID_INSUFFICIENT):
                outcome = VALID_STALE_COVERAGE
        last_end = None if not times else _utc(times[-1], "last bin") + timedelta(seconds=BIN_SECONDS)
        return {
            "schema": ADAPTER_SCHEMA, "source": self.source.as_dict(), "zero_policy": self.zero_policy,
            "zero_semantics": ("live: a zero is genuine only inside a verified-coverage interval" if self.zero_policy == ZERO_POLICY_VERIFIED_ONLY
                               else "historical benchmark: reception-free seconds are zero-filled and coverage is UNVERIFIED"),
            "live_semantics": self.zero_policy == ZERO_POLICY_VERIFIED_ONLY,
            "coverage_attestation": self.attestation.as_dict() if self.attestation.intervals else None,
            "history_start_utc": times[0] if times else None, "history_end_utc": times[-1] if times else None,
            "decision_cutoff_utc": cutoff.isoformat(), "bin_seconds": BIN_SECONDS,
            "valid_bin_count": len(times), "min_history_bins": self.min_history_bins,
            "missing_interval_count": len(reference.gap_intervals) + (1 if reference.gap_intervals_truncated else 0),
            "missing_seconds": reference.missing_seconds, "unavailable_seconds": reference.unavailable_seconds,
            "forecast_horizon_seconds": self.forecast_horizon_s, "duplicate_handling": self.binned.report.get("duplicate_handling"),
            "input_report": dict(self.binned.report), "bins_fed": dict(self.counts_fed),
            "no_future_data": last_end is None or last_end <= cutoff,
            "history_reference": reference.as_dict(), "validation_outcome": outcome, "reasons": reasons,
            "coverage_mode": self.coverage_mode, "coverage_provenance": self.coverage_provenance(cutoff),
            "note": "Ethereum mempool arrivals, not Fabric observations; a valid history is an input contract check, not forecast skill",
        }
