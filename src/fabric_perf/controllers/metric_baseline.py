"""Baseline and delta handling for Prometheus histogram counters (owner decision D3, 2026-11-17).

A Prometheus histogram series is created at its first observation. The block cutter's
``blockcutter_block_fill_duration`` therefore does not exist on an orderer until that orderer
has cut a block for the channel. The earlier tooling had two ways to read an absent
"before" value: report no delta (the live adapter, and the file summarizer by default) or
treat it as zero (``--fresh-channel``). Treating it as zero hides blocks cut before the
baseline, so it is no longer an accepted reading for a validation run.

This module classifies every observation into one of six states and computes a counter delta
only when both observations are valid and comparable:

==================  ====================================================================
FAMILY_ABSENT       a valid scrape contains no sample of the metric family at all
SERIES_ABSENT       the family exists (for other channels) but not for the channel asked about
PRESENT_ZERO        the histogram series exists with count 0 and sum 0
PRESENT_NONZERO     the histogram series exists with a positive count
SCRAPE_FAILED       the endpoint could not be read (transport error, HTTP status, no scrape)
MALFORMED           the body or the series is unusable (bad syntax, missing _sum/_count, ...)
==================  ====================================================================

A missing series is never converted to zero, and a delta is never reported when the baseline
is absent, failed or malformed, or when a counter reset is seen.

Counter delta versus ledger block-height delta
----------------------------------------------
``Δ blockcutter_block_fill_duration_count`` is the number of batches the block cutter of one
orderer process observed between two readings. It restarts at zero when that process restarts
and exists only on the orderer that cut. ``Δ ledger height`` is the number of blocks appended to
the channel ledger and is independent of any orderer process. The two usually agree for
endorser-transaction traffic, but the counter does not count configuration blocks and cannot
cover blocks cut before an absent baseline. The ledger height delta is therefore the
authoritative block count, and the counter delta is cross-checked against it only when every
orderer that could have cut has a valid delta. Pure standard library; no package-relative imports.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping

FAMILY = "blockcutter_block_fill_duration"


class BaselineState(str, Enum):
    FAMILY_ABSENT = "family_absent"
    SERIES_ABSENT = "series_absent"
    PRESENT_ZERO = "present_zero"
    PRESENT_NONZERO = "present_nonzero"
    SCRAPE_FAILED = "scrape_failed"
    MALFORMED = "malformed"


VALID_STATES = frozenset({BaselineState.PRESENT_ZERO, BaselineState.PRESENT_NONZERO})
ABSENT_STATES = frozenset({BaselineState.FAMILY_ABSENT, BaselineState.SERIES_ABSENT})

LabelSet = tuple[tuple[str, str], ...]
Row = tuple[str, LabelSet, float]


@dataclass(frozen=True)
class HistogramObservation:
    """One reading of a histogram's ``_count`` and ``_sum`` for one series, or why there is none."""

    state: BaselineState
    count: float | None = None
    total: float | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if self.state in VALID_STATES:
            for name, value in (("count", self.count), ("sum", self.total)):
                if (isinstance(value, bool) or not isinstance(value, (int, float))
                        or not math.isfinite(value) or value < 0):
                    raise ValueError(f"a valid observation needs a finite non-negative {name}")
            if not float(self.count).is_integer():
                raise ValueError("a histogram count must be a whole number")
            if (self.state is BaselineState.PRESENT_ZERO) != (self.count == 0):
                raise ValueError("PRESENT_ZERO means count 0; PRESENT_NONZERO means count above 0")
        elif self.count is not None or self.total is not None:
            raise ValueError("an observation that is not valid carries no value")

    @property
    def valid(self) -> bool:
        return self.state in VALID_STATES

    @classmethod
    def from_values(cls, count: Any, total: Any) -> "HistogramObservation":
        """Classify a count/sum pair; invalid content becomes MALFORMED, never a number."""
        try:
            c, s = float(count), float(total)
        except (TypeError, ValueError):
            return cls(BaselineState.MALFORMED, detail="count or sum is not numeric")
        if not (math.isfinite(c) and math.isfinite(s)) or c < 0 or s < 0:
            return cls(BaselineState.MALFORMED, detail="count or sum is negative or not finite")
        if not c.is_integer():
            return cls(BaselineState.MALFORMED, detail="histogram count is not a whole number")
        if c == 0 and s != 0:
            return cls(BaselineState.MALFORMED, detail="histogram sum is positive but count is 0")
        return cls(BaselineState.PRESENT_ZERO if c == 0 else BaselineState.PRESENT_NONZERO, c, s)

    def as_dict(self) -> dict[str, Any]:
        return {"state": self.state.value, "count": self.count, "sum": self.total, "detail": self.detail}


def observe_histogram(rows: Iterable[Row], *, channel: str, family: str = FAMILY) -> HistogramObservation:
    """Classify one successfully parsed scrape for one channel's histogram series."""
    wanted = {f"{family}_sum", f"{family}_count", f"{family}_bucket"}
    family_rows = [row for row in rows if row[0] in wanted]
    if not family_rows:
        return HistogramObservation(BaselineState.FAMILY_ABSENT, detail=f"no {family} sample in the scrape")
    on_channel = [row for row in family_rows if dict(row[1]).get("channel") == channel]
    if not on_channel:
        others = sorted({dict(row[1]).get("channel", "") for row in family_rows})
        return HistogramObservation(
            BaselineState.SERIES_ABSENT, detail=f"{family} present for channels {others} but not for {channel!r}")
    counts = [row[2] for row in on_channel if row[0] == f"{family}_count"]
    sums = [row[2] for row in on_channel if row[0] == f"{family}_sum"]
    if len(counts) != 1 or len(sums) != 1:
        return HistogramObservation(
            BaselineState.MALFORMED,
            detail=f"expected one _count and one _sum for the channel, found {len(counts)} and {len(sums)}")
    return HistogramObservation.from_values(counts[0], sums[0])


class DeltaStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class CounterDelta:
    """Result of comparing two readings. ``final_*`` are absolute values, never deltas."""

    status: DeltaStatus
    count_delta: float | None
    sum_delta: float | None
    reason: str | None
    baseline_state: BaselineState | None
    final_state: BaselineState
    final_count: float | None = None
    final_sum: float | None = None

    @property
    def mean(self) -> float | None:
        if self.status is not DeltaStatus.AVAILABLE or not self.count_delta:
            return None
        return self.sum_delta / self.count_delta

    @property
    def never_observed(self) -> bool:
        """Absent before and after: no series ever existed, so there is nothing to difference."""
        return self.baseline_state in ABSENT_STATES and self.final_state in ABSENT_STATES

    def as_dict(self) -> dict[str, Any]:
        return {"status": self.status.value, "count_delta": self.count_delta, "sum_delta": self.sum_delta,
                "mean": self.mean, "reason": self.reason,
                "baseline_state": None if self.baseline_state is None else self.baseline_state.value,
                "final_state": self.final_state.value,
                "final_absolute": {"count": self.final_count, "sum": self.final_sum,
                                   "note": "absolute value since the process started; not a delta"}}


def assess_delta(before: HistogramObservation | None, after: HistogramObservation,
                 *, reset_seen: bool = False) -> CounterDelta:
    """Return a delta only when both observations are valid and comparable.

    ``before`` is None when there is no earlier observation at all (the first sample).
    ``reset_seen`` is True when any reading between the two decreased.
    """
    def unavailable(reason: str) -> CounterDelta:
        return CounterDelta(DeltaStatus.UNAVAILABLE, None, None, reason,
                            None if before is None else before.state, after.state,
                            after.count if after.valid else None, after.total if after.valid else None)

    if before is None:
        return unavailable("no_earlier_observation")
    if not before.valid:
        return unavailable(f"baseline_{before.state.value}")
    if not after.valid:
        return unavailable(f"final_{after.state.value}")
    if reset_seen:
        return unavailable("counter_reset_observed_between_readings")
    if after.count < before.count or after.total < before.total:
        return unavailable("counter_reset")
    return CounterDelta(DeltaStatus.AVAILABLE, after.count - before.count, after.total - before.total, None,
                        before.state, after.state, after.count, after.total)


class CrossCheck(str, Enum):
    CONSISTENT = "CONSISTENT"
    INCONSISTENT = "INCONSISTENT"
    NOT_COMPARABLE = "NOT_COMPARABLE"


@dataclass(frozen=True)
class BlockCountCrossCheck:
    verdict: CrossCheck
    blocks_by_ledger: int | None
    blocks_by_counter: float | None
    reason: str | None
    contributing_targets: tuple[str, ...] = ()
    unresolved_targets: tuple[str, ...] = ()
    note: str = ("the ledger height delta is the authoritative block count; the counter delta is "
                 "compared only when every orderer that could have cut has a valid delta")

    def as_dict(self) -> dict[str, Any]:
        return {"verdict": self.verdict.value, "blocks_by_ledger": self.blocks_by_ledger,
                "blocks_by_counter": self.blocks_by_counter, "reason": self.reason,
                "contributing_targets": list(self.contributing_targets),
                "unresolved_targets": list(self.unresolved_targets), "note": self.note}


def _height(name: str, value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def cross_check_block_count(deltas: Mapping[str, CounterDelta], *, ledger_height_before: Any,
                            ledger_height_after: Any) -> BlockCountCrossCheck:
    """Compare Σ fill-count deltas with the independently observed ledger height delta."""
    before, after = _height("before", ledger_height_before), _height("after", ledger_height_after)
    if before is None or after is None:
        return BlockCountCrossCheck(CrossCheck.NOT_COMPARABLE, None, None, "ledger_height_invalid")
    if after < before:
        return BlockCountCrossCheck(CrossCheck.NOT_COMPARABLE, None, None, "ledger_height_decreased")
    ledger = after - before
    unresolved = tuple(sorted(t for t, d in deltas.items()
                              if d.status is DeltaStatus.UNAVAILABLE and not d.never_observed))
    if unresolved:
        return BlockCountCrossCheck(
            CrossCheck.NOT_COMPARABLE, ledger, None,
            "counter_delta_unavailable: " + ", ".join(f"{t} ({deltas[t].reason})" for t in unresolved),
            unresolved_targets=unresolved)
    available = {t: d for t, d in deltas.items() if d.status is DeltaStatus.AVAILABLE}
    total = sum(d.count_delta for d in available.values())
    if total == ledger:
        return BlockCountCrossCheck(CrossCheck.CONSISTENT, ledger, total, None, tuple(sorted(available)))
    return BlockCountCrossCheck(
        CrossCheck.INCONSISTENT, ledger, total, f"counter_total_{total:g}_differs_from_ledger_delta_{ledger}",
        tuple(sorted(available)))
