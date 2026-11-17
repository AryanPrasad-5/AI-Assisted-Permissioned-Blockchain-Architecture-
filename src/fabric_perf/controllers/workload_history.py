"""Shared append-only arrival history with the strict live workload gap policy.

Independent audit 2026-11-17, defect D3, and owner Decision 2 (strict live workload gap policy):

* Each timestamped 1-second observation is stored once, in one store per workload source.
  Controller records carry an :class:`ArrivalHistoryReference` (day, boundaries, bin count,
  SHA-256 of the exact prefix) instead of a copy of the history.
* A healthy, complete interval with zero arrivals is a genuine zero (``add(..., 0)``).
* A missing interval is *not* zero. A disconnected or unhealthy source is recorded as
  unavailable (``mark_unavailable``). Neither is interpolated, forward-filled or zero-filled.
* A missing/unavailable interval may be reconstructed only from verified historical
  observations (``backfill`` with a named source and artifact SHA-256). Otherwise the UTC
  day stays incomplete and forecasting abstains until the next complete UTC-day prefix.

The historical benchmark series (``data/processed/flashbots/series_*_1s.csv.gz``) were built by
zero-filling reception-free seconds. That semantics is preserved unchanged for offline replay;
it is not the live semantics implemented here.

No network, Fabric, Docker or actuator access happens in this module.
"""
from __future__ import annotations

import hashlib
import json
import math
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .base import ArrivalHistoryReference
from .telemetry import parse_observation_time

GAP_POLICY = "strict-live-v1"
ZERO_SEMANTICS = "live: 0 = verified healthy complete second with no arrivals; missing/unavailable seconds are never 0"
HISTORICAL_BENCHMARK_ZERO_SEMANTICS = "historical benchmark: reception-free seconds were zero-filled by bincount (unchanged)"
HASH_METHOD = "sha256 over UTF-8 lines '<bin_timestamp_utc>|<integer count>\\n', one per bin, in time order"
MAX_GAP_INTERVALS_IN_REFERENCE = 16
SECONDS_PER_DAY = 86_400

MISSING, OBSERVED, BACKFILLED, UNAVAILABLE = 0, 1, 2, 3
_KIND_NAMES = {MISSING: "missing", UNAVAILABLE: "unavailable"}
_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


class WorkloadHistoryError(ValueError):
    """An observation, outage report or backfill violates the live workload contract."""


def _line(timestamp_utc: str, count: float) -> bytes:
    return f"{timestamp_utc}|{int(count)}\n".encode("utf-8")


def prefix_sha256(timestamps_utc: Iterable[str], counts: Iterable[float]) -> str:
    """Recompute a prefix digest independently of the store (see ``HASH_METHOD``)."""
    digest = hashlib.sha256()
    for timestamp, count in zip(timestamps_utc, counts, strict=True):
        digest.update(_line(timestamp, count))
    return digest.hexdigest()


def _count(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) \
            or value < 0 or not float(value).is_integer():
        raise WorkloadHistoryError("arrival count must be a finite non-negative integer")
    return float(value)


def _whole_second(value: str, field_name: str) -> datetime:
    try:
        parsed = parse_observation_time(value, field=field_name)
    except ValueError as exc:
        raise WorkloadHistoryError(str(exc)) from exc
    if parsed.microsecond:
        raise WorkloadHistoryError(f"{field_name} must be a whole UTC second")
    return parsed


def _midnight(value: datetime) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


@dataclass
class _DaySegment:
    start: datetime
    times: list = field(default_factory=list)       # ISO strings; None where no value is held
    counts: list = field(default_factory=list)      # floats; None where no value is held
    collected: list = field(default_factory=list)   # live collection time; backfill batch id for backfills
    kinds: bytearray = field(default_factory=bytearray)
    latest: int = -1                                 # latest second accounted (observed or unavailable)
    complete_len: int = 0                            # bins [0, complete_len) all observed/backfilled
    backfilled_in_prefix: int = 0
    hasher: Any = field(default_factory=hashlib.sha256)
    backfill_batches: list = field(default_factory=list)   # (batch_id, min_index)

    @property
    def day(self) -> str:
        return self.start.date().isoformat()

    def ensure(self, index: int) -> None:
        grow = index + 1 - len(self.kinds)
        if grow > 0:
            self.times.extend([None] * grow)
            self.counts.extend([None] * grow)
            self.collected.extend([None] * grow)
            self.kinds.extend(bytes(grow))

    def stamp(self, index: int) -> str:
        return (self.start + timedelta(seconds=index)).isoformat()

    def advance(self) -> None:
        kinds, n = self.kinds, self.complete_len
        while n <= self.latest and kinds[n] in (OBSERVED, BACKFILLED):
            self.hasher.update(_line(self.times[n], self.counts[n]))
            self.backfilled_in_prefix += kinds[n] == BACKFILLED
            n += 1
        self.complete_len = n

    def gap_intervals(self, limit: int) -> tuple[list[tuple[str, str, str]], bool, int, int]:
        intervals, missing, unavailable, index = [], 0, 0, self.complete_len
        kinds = self.kinds
        while index <= self.latest:
            kind = kinds[index]
            if kind in (OBSERVED, BACKFILLED):
                index += 1
                continue
            end = index
            while end + 1 <= self.latest and kinds[end + 1] == kind:
                end += 1
            width = end - index + 1
            if kind == MISSING:
                missing += width
            else:
                unavailable += width
            if len(intervals) < limit:
                intervals.append((self.stamp(index), self.stamp(end), _KIND_NAMES[kind]))
            else:
                intervals.append(None)
            index = end + 1
        truncated = len(intervals) > limit
        return [item for item in intervals if item is not None], truncated, missing, unavailable


class ArrivalHistoryStore:
    """One workload source's append-only, per-UTC-day store of 1-second arrival counts.

    Thread-safe: every public method holds one lock, so a snapshot is atomic with respect to
    concurrent appends (independent audit D4). Memory is bounded by ``retain_days`` day segments
    (default 2: the current and previous UTC day). Evicted days remain reproducible only from
    the append-only ``log_path`` (JSON lines), when one is configured.
    """

    def __init__(self, source: str, *, max_age_s: float = 5.0, store_id: str | None = None,
                 log_path: Path | None = None, retain_days: int | None = 2):
        if not source or isinstance(max_age_s, bool) or not math.isfinite(max_age_s) or max_age_s < 1:
            raise ValueError("source and finite max_age_s >= 1 are required")
        if retain_days is not None and (isinstance(retain_days, bool) or not isinstance(retain_days, int) or retain_days < 1):
            raise ValueError("retain_days must be a positive integer or None")
        self.source, self.max_age_s = source, float(max_age_s)
        self.store_id = store_id or f"{source}:{uuid.uuid4().hex[:12]}"
        self.retain_days = retain_days
        self.log_path = None if log_path is None else Path(log_path)
        self._lock = threading.RLock()
        self._days: dict[str, _DaySegment] = {}
        self._latest_day: str | None = None
        self._sequence = 0
        self._evicted_days: list[str] = []
        self._last_collection: str | None = None
        self._log = None
        if self.log_path is not None:
            self._log = self.log_path.open("x", encoding="utf-8", newline="\n")  # never overwrite evidence
            self._write({"kind": "header", "store_id": self.store_id, "source": self.source,
                         "gap_policy": GAP_POLICY, "hash_method": HASH_METHOD, "zero_semantics": ZERO_SEMANTICS,
                         "max_age_s": self.max_age_s, "unit": "arrivals/second-bin"})

    # ------------------------------------------------------------------ persistence
    def _write(self, event: dict) -> None:
        if self._log is not None:
            self._log.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
            self._log.flush()

    def _event(self, event: dict) -> None:
        self._sequence += 1
        self._write({"seq": self._sequence, **event})

    def close(self) -> None:
        with self._lock:
            if self._log is not None:
                self._log.close()
                self._log = None

    # ------------------------------------------------------------------ segments
    def _segment_for(self, when: datetime, *, create: bool) -> tuple[_DaySegment, int]:
        day = when.date().isoformat()
        if create and self._latest_day is not None and day < self._latest_day:
            raise WorkloadHistoryError("duplicate or out-of-order bin: it belongs to an earlier UTC day than the store's latest")
        segment = self._days.get(day)
        if segment is None:
            if not create:
                raise WorkloadHistoryError(f"no retained history for UTC day {day}")
            segment = self._days[day] = _DaySegment(_midnight(when))
            self._latest_day = day
            if self.retain_days is not None:
                for old in sorted(self._days)[:-self.retain_days]:
                    del self._days[old]
                    self._evicted_days.append(old)
        return segment, int((when - segment.start).total_seconds())

    # ------------------------------------------------------------------ live input
    def add(self, timestamp_utc: str, count: float, *, collected_at_utc: str) -> None:
        """Record one healthy, complete, fresh 1-second bin. ``count=0`` is a genuine zero.

        Duplicates, out-of-order, stale, future and invalid bins are rejected without mutation.
        A bin after a hole leaves the hole explicitly missing (never zero) for the UTC day.
        """
        with self._lock:
            self._apply_observation(timestamp_utc, count, collected_at_utc, check_freshness=True)

    def _apply_observation(self, timestamp_utc, count, collected_at_utc, *, check_freshness: bool) -> None:
        t = _whole_second(timestamp_utc, "bin timestamp")
        try:
            now = parse_observation_time(collected_at_utc, field="collected_at_utc")
        except ValueError as exc:
            raise WorkloadHistoryError(str(exc)) from exc
        age = (now - t).total_seconds()
        if check_freshness and not 1 <= age <= self.max_age_s:
            raise WorkloadHistoryError("bin must be a completed, fresh whole UTC second")
        value = _count(count)
        segment, index = self._segment_for(t, create=True)
        if index <= segment.latest:
            raise WorkloadHistoryError("duplicate or out-of-order bin")
        segment.ensure(index)
        if index > segment.latest + 1:
            self._event({"kind": "gap_detected", "from": segment.stamp(segment.latest + 1), "to": segment.stamp(index - 1),
                         "detected_at": now.isoformat()})
        iso = t.isoformat()
        segment.times[index], segment.counts[index] = iso, value
        segment.collected[index], segment.kinds[index] = now.isoformat(), OBSERVED
        segment.latest = index
        segment.advance()
        self._last_collection = now.isoformat()
        self._event({"kind": "observation", "bin": iso, "count": int(value), "collected_at": now.isoformat()})

    def mark_unavailable(self, start_utc: str, end_utc: str, *, reason: str, reported_at_utc: str) -> None:
        """Record that the source was disconnected/unhealthy for [start, end] (inclusive seconds).

        These seconds become ``unavailable``: never zero, never interpolated. Only a verified
        ``backfill`` can later reconstruct them.
        """
        with self._lock:
            self._apply_unavailable(start_utc, end_utc, reason, reported_at_utc)

    def _apply_unavailable(self, start_utc, end_utc, reason, reported_at_utc) -> None:
        if not reason or not isinstance(reason, str):
            raise WorkloadHistoryError("an unavailable interval requires a reason")
        start, end = _whole_second(start_utc, "unavailable start"), _whole_second(end_utc, "unavailable end")
        try:
            reported = parse_observation_time(reported_at_utc, field="reported_at_utc")
        except ValueError as exc:
            raise WorkloadHistoryError(str(exc)) from exc
        if end < start or start.date() != end.date():
            raise WorkloadHistoryError("unavailable interval must be ordered and within one UTC day")
        if reported < end + timedelta(seconds=1):
            raise WorkloadHistoryError("an interval can be reported unavailable only after it has ended")
        segment, first = self._segment_for(start, create=True)
        last = int((end - segment.start).total_seconds())
        if first <= segment.latest:
            raise WorkloadHistoryError("unavailable interval overlaps already accounted seconds")
        segment.ensure(last)
        for index in range(first, last + 1):
            segment.kinds[index] = UNAVAILABLE
        if first > segment.latest + 1:
            self._event({"kind": "gap_detected", "from": segment.stamp(segment.latest + 1), "to": segment.stamp(first - 1),
                         "detected_at": reported.isoformat()})
        segment.latest = last
        self._event({"kind": "unavailable", "from": start.isoformat(), "to": end.isoformat(),
                     "reason": reason, "reported_at": reported.isoformat()})

    def backfill(self, bins: Iterable[tuple[str, float]], *, provenance: Mapping[str, Any]) -> str:
        """Reconstruct missing/unavailable seconds from verified historical observations.

        ``provenance`` must name the historical ``source`` and the SHA-256 of the verified
        artifact the bins were read from. The whole batch is validated first and applied
        atomically. Seconds that already hold a value are never overwritten. Returns the batch id.
        """
        with self._lock:
            return self._apply_backfill(list(bins), dict(provenance), batch_id=None)

    def _apply_backfill(self, bins: list, provenance: dict, *, batch_id: str | None) -> str:
        source, digest = provenance.get("source"), provenance.get("sha256")
        if not isinstance(source, str) or not source:
            raise WorkloadHistoryError("backfill provenance requires the verified historical source")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdefABCDEF" for c in digest):
            raise WorkloadHistoryError("backfill provenance requires the verified artifact SHA-256")
        if not bins:
            raise WorkloadHistoryError("backfill batch is empty")
        planned, segment = [], None
        for item in bins:
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                raise WorkloadHistoryError("backfill bins must be (timestamp_utc, count) pairs")
            t = _whole_second(item[0], "backfill bin timestamp")
            value = _count(item[1])
            seg, index = self._segment_for(t, create=False)
            if segment is None:
                segment = seg
            elif seg is not segment:
                raise WorkloadHistoryError("a backfill batch must lie within one UTC day")
            if index > segment.latest:
                raise WorkloadHistoryError("backfill may only fill holes before the latest accounted second")
            if segment.kinds[index] not in (MISSING, UNAVAILABLE):
                raise WorkloadHistoryError("backfill cannot overwrite a second that already holds an observation")
            planned.append((index, t.isoformat(), value))
        if len({index for index, _, _ in planned}) != len(planned):
            raise WorkloadHistoryError("backfill batch repeats a second")
        batch_id = batch_id or f"backfill-{self._sequence + 1}"
        for index, iso, value in planned:
            segment.times[index], segment.counts[index] = iso, value
            segment.collected[index], segment.kinds[index] = batch_id, BACKFILLED
        segment.backfill_batches.append((batch_id, min(index for index, _, _ in planned)))
        segment.advance()
        self._event({"kind": "backfill", "batch_id": batch_id, "provenance": provenance,
                     "bins": [[iso, int(value)] for _, iso, value in sorted(planned)]})
        return batch_id

    # ------------------------------------------------------------------ reading
    def _reference(self, segment: _DaySegment | None, *, day: str | None, complete: bool) -> ArrivalHistoryReference:
        common = dict(store_id=self.store_id, source=self.source, utc_day=day, hash_method=HASH_METHOD,
                      log_sequence=self._sequence, gap_policy=GAP_POLICY, zero_semantics=ZERO_SEMANTICS,
                      history_log=None if self.log_path is None else str(self.log_path))
        if segment is None:
            return ArrivalHistoryReference(status="empty", bin_count=0, first_bin_timestamp_utc=None,
                                           last_bin_timestamp_utc=None, prefix_sha256=_EMPTY_SHA256, **common)
        intervals, truncated, missing, unavailable = segment.gap_intervals(MAX_GAP_INTERVALS_IN_REFERENCE)
        n = segment.complete_len
        backfilled = segment.backfilled_in_prefix
        diagnostics = dict(latest_accounted_timestamp_utc=None if segment.latest < 0 else segment.stamp(segment.latest),
                           complete_prefix_bins=n, missing_seconds=missing, unavailable_seconds=unavailable,
                           gap_intervals=tuple(intervals), gap_intervals_truncated=truncated)
        if complete:
            return ArrivalHistoryReference(
                status="complete", bin_count=n, first_bin_timestamp_utc=segment.times[0],
                last_bin_timestamp_utc=segment.times[n - 1], prefix_sha256=segment.hasher.hexdigest(),
                observed_bins=n - backfilled, backfilled_bins=backfilled,
                backfill_batch_ids=tuple(batch for batch, first in segment.backfill_batches if first < n),
                **diagnostics, **common)
        return ArrivalHistoryReference(status="incomplete", bin_count=0, first_bin_timestamp_utc=None,
                                       last_bin_timestamp_utc=None, prefix_sha256=_EMPTY_SHA256, **diagnostics, **common)

    def snapshot(self, now_utc: str | None = None) -> tuple[tuple[str, ...], tuple[float, ...], ArrivalHistoryReference]:
        """Atomically return the current UTC day's complete prefix and its reference.

        With ``now_utc`` the day is the UTC day of ``now`` (so at 00:00:00 the previous day is
        never offered: independent audit D1). Without it, the latest day held. If the day has a
        missing or unavailable second, no bins are returned (status ``incomplete``).
        """
        with self._lock:
            if now_utc is not None:
                day = parse_observation_time(now_utc, field="now_utc").date().isoformat()
            else:
                day = self._latest_day
            segment = None if day is None else self._days.get(day)
            if segment is None or segment.latest < 0:
                return (), (), self._reference(None, day=day, complete=False)
            n = segment.complete_len
            if n != segment.latest + 1:
                return (), (), self._reference(segment, day=day, complete=False)
            return tuple(segment.times[:n]), tuple(segment.counts[:n]), self._reference(segment, day=day, complete=True)

    def history(self) -> tuple[tuple[str, ...], tuple[float, ...]]:
        """Legacy reader: the latest day's complete prefix, or empty if that day is incomplete."""
        times, counts, _ = self.snapshot()
        return times, counts

    def prefix(self, reference: ArrivalHistoryReference | Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[float, ...]]:
        """Return the exact bins a reference names, after re-verifying their SHA-256."""
        ref = ArrivalHistoryReference.from_mapping(reference)
        if ref.store_id != self.store_id or ref.source != self.source:
            raise WorkloadHistoryError("reference belongs to a different history store")
        if ref.status != "complete":
            return (), ()
        with self._lock:
            segment = self._days.get(ref.utc_day)
            if segment is None:
                raise WorkloadHistoryError(f"UTC day {ref.utc_day} is not retained in memory; load the history log")
            if segment.complete_len < ref.bin_count:
                raise WorkloadHistoryError("store does not hold the referenced prefix")
            times, counts = tuple(segment.times[:ref.bin_count]), tuple(segment.counts[:ref.bin_count])
        if times[0] != ref.first_bin_timestamp_utc or times[-1] != ref.last_bin_timestamp_utc \
                or prefix_sha256(times, counts) != ref.prefix_sha256:
            raise WorkloadHistoryError("stored prefix does not match the reference digest")
        return times, counts

    def provenance(self, reference: ArrivalHistoryReference | None = None) -> dict:
        """Bounded provenance summary; never the per-bin history."""
        with self._lock:
            return {
                "source": self.source, "store_id": self.store_id, "unit": "arrivals/second-bin",
                "gap_policy": GAP_POLICY, "zero_semantics": ZERO_SEMANTICS, "hash_method": HASH_METHOD,
                "history_log": None if self.log_path is None else str(self.log_path),
                "retained_days": sorted(self._days), "evicted_days": list(self._evicted_days[-8:]),
                "evicted_day_count": len(self._evicted_days), "event_count": self._sequence,
                "last_collection_timestamp_utc": self._last_collection,
                "reference": None if reference is None else reference.as_dict(),
            }

    def retained_bins(self) -> int:
        with self._lock:
            return sum(len(segment.kinds) for segment in self._days.values())

    # ------------------------------------------------------------------ replay of the log
    @classmethod
    def from_log(cls, path: Path) -> "ArrivalHistoryStore":
        """Rebuild a store (all days retained) from its append-only log, re-validating every event.

        Freshness of live observations is re-checked against their recorded collection time.
        """
        lines = Path(path).read_text(encoding="utf-8").splitlines()
        header = json.loads(lines[0])
        if header.get("kind") != "header" or header.get("gap_policy") != GAP_POLICY or header.get("hash_method") != HASH_METHOD:
            raise WorkloadHistoryError("not a strict-live-v1 workload history log")
        store = cls(header["source"], max_age_s=header["max_age_s"], store_id=header["store_id"], retain_days=None)
        for expected, line in enumerate(lines[1:], 1):
            event = json.loads(line)
            if event.get("seq") != expected:
                raise WorkloadHistoryError(f"history log sequence broken at event {expected}")
            kind = event["kind"]
            if kind == "observation":
                store._apply_observation(event["bin"], event["count"], event["collected_at"], check_freshness=True)
            elif kind == "unavailable":
                store._apply_unavailable(event["from"], event["to"], event["reason"], event["reported_at"])
            elif kind == "backfill":
                store._apply_backfill(event["bins"], event["provenance"], batch_id=event["batch_id"])
            elif kind == "gap_detected":
                # Informational: replaying the next observation/outage re-emits it with this
                # sequence number, which the check after that event confirms.
                continue
            else:
                raise WorkloadHistoryError(f"unknown history log event {kind!r}")
            if store._sequence != expected:
                raise WorkloadHistoryError(f"history log event {expected} replays to a different sequence")
        return store


def reproduce_ewma_forecast(store: ArrivalHistoryStore, reference: ArrivalHistoryReference | Mapping[str, Any],
                            *, alpha: float = 0.005) -> float:
    """Recompute the frozen EWMA state from the referenced, digest-verified prefix."""
    from fabric_perf.forecasting.horizon_benchmark import ewma_states

    times, counts = store.prefix(reference)
    if not counts:
        raise WorkloadHistoryError("an incomplete or empty reference has no forecast to reproduce")
    return float(ewma_states(counts, alpha)[-1])
