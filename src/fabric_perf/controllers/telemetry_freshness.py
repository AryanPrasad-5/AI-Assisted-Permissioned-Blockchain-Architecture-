"""Explicit observation-freshness policy and classification (live-shadow fix 2, 2026-11-17).

The live path accepted an observation only if it was at most five seconds old, but the
five seconds were spelled out in five places (the provider, the metric, the frame
validator, each controller and the shadow CLI), negative ages were silently clamped to
zero in the provider, and a rejected frame carried only a generic message. This module
states the rule once:

* ``FreshnessPolicy`` holds the limits and refuses invalid or silently widened values;
* every observation is classified FRESH, STALE, MISSING, FUTURE_DATED or INCONSISTENT;
* each non-fresh class maps to one controller abstention reason, so a controller is told
  *why* it must not decide.

Boundary convention (unchanged from the existing code): an age exactly equal to
``max_age_s`` is FRESH; an age strictly greater is STALE.

The original observation timestamp is never replaced. Re-reading an old summary yields the
same timestamp and therefore a larger age. Only the standard library is used and there are
no package-relative imports.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping

# The limit the project has used since Phase 2A. Raising it is a protocol change.
DEFAULT_MAX_OBSERVATION_AGE_S = 5.0
# Sanity ceiling for validated configuration; catches a unit slip (5000 instead of 5).
MAX_ALLOWED_OBSERVATION_AGE_S = 30.0
# Largest tolerated producer-clock lead (the PF-10 clock gate is 1 s).
MAX_ALLOWED_FUTURE_SKEW_S = 1.0
DEFAULT_MAX_SOURCE_SKEW_S = 5.0
DEFAULT_MAX_CLOCK_DISCREPANCY_S = 1.0


class FreshnessStatus(str, Enum):
    FRESH = "FRESH"
    STALE = "STALE"
    MISSING = "MISSING"
    FUTURE_DATED = "FUTURE_DATED"
    INCONSISTENT = "INCONSISTENT"


# Reason strings end in ``fail_safe`` so that ``ControllerDecision.outcome`` reports "abstain".
ABSTENTION_REASON = {
    FreshnessStatus.STALE: "stale_telemetry_fail_safe",
    FreshnessStatus.MISSING: "telemetry_freshness_unknown_fail_safe",
    FreshnessStatus.FUTURE_DATED: "future_dated_telemetry_fail_safe",
    FreshnessStatus.INCONSISTENT: "telemetry_timestamp_inconsistent_fail_safe",
}
FRAME_CODE_PREFIX = {
    FreshnessStatus.STALE: "stale_observation",
    FreshnessStatus.MISSING: "observation_timestamp_missing",
    FreshnessStatus.FUTURE_DATED: "future_dated_observation",
    FreshnessStatus.INCONSISTENT: "observation_timestamp_inconsistent",
}
# When several sources fail differently, report the most serious integrity problem first.
SEVERITY = (FreshnessStatus.INCONSISTENT, FreshnessStatus.FUTURE_DATED, FreshnessStatus.MISSING,
            FreshnessStatus.STALE, FreshnessStatus.FRESH)


def _number(name: str, value: Any, *, low: float, high: float, low_inclusive: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number, got {value!r}")
    number = float(value)
    below = number < low if low_inclusive else number <= low
    if not math.isfinite(number) or below or number > high:
        bound = "[" if low_inclusive else "("
        raise ValueError(f"{name} must be finite and in {bound}{low}, {high}], got {value!r}")
    return number


@dataclass(frozen=True)
class FreshnessPolicy:
    """Validated freshness limits. Construct it only through this class or ``from_mapping``."""

    max_age_s: float = DEFAULT_MAX_OBSERVATION_AGE_S
    max_future_skew_s: float = 0.0
    max_source_skew_s: float = DEFAULT_MAX_SOURCE_SKEW_S
    max_clock_discrepancy_s: float = DEFAULT_MAX_CLOCK_DISCREPANCY_S

    def __post_init__(self) -> None:
        object.__setattr__(self, "max_age_s", _number(
            "max_age_s", self.max_age_s, low=0.0, high=MAX_ALLOWED_OBSERVATION_AGE_S, low_inclusive=False))
        object.__setattr__(self, "max_future_skew_s", _number(
            "max_future_skew_s", self.max_future_skew_s, low=0.0, high=MAX_ALLOWED_FUTURE_SKEW_S, low_inclusive=True))
        object.__setattr__(self, "max_source_skew_s", _number(
            "max_source_skew_s", self.max_source_skew_s, low=0.0, high=MAX_ALLOWED_OBSERVATION_AGE_S, low_inclusive=False))
        object.__setattr__(self, "max_clock_discrepancy_s", _number(
            "max_clock_discrepancy_s", self.max_clock_discrepancy_s, low=0.0, high=5.0, low_inclusive=False))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FreshnessPolicy":
        """The only route from configuration text to a policy: unknown keys are errors."""
        if not isinstance(value, Mapping):
            raise ValueError("freshness policy must be a mapping")
        allowed = {"max_age_s", "max_future_skew_s", "max_source_skew_s", "max_clock_discrepancy_s"}
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown freshness policy keys: {sorted(unknown)}")
        return cls(**dict(value))

    def as_dict(self) -> dict[str, float]:
        return {"max_age_s": self.max_age_s, "max_future_skew_s": self.max_future_skew_s,
                "max_source_skew_s": self.max_source_skew_s,
                "max_clock_discrepancy_s": self.max_clock_discrepancy_s}


@dataclass(frozen=True)
class FreshnessVerdict:
    source: str
    status: FreshnessStatus
    age_s: float | None
    observed_timestamp_utc: str | None
    reference_utc: str
    max_age_s: float
    detail: str | None = None

    @property
    def fresh(self) -> bool:
        return self.status is FreshnessStatus.FRESH

    @property
    def abstention_reason(self) -> str | None:
        return None if self.fresh else ABSTENTION_REASON[self.status]

    @property
    def frame_code(self) -> str | None:
        if self.fresh:
            return None
        code = f"{FRAME_CODE_PREFIX[self.status]}:{self.source}"
        return f"{code}:{self.detail}" if self.status is FreshnessStatus.INCONSISTENT and self.detail else code

    def as_dict(self) -> dict[str, Any]:
        return {"source": self.source, "status": self.status.value, "age_s": self.age_s,
                "observed_timestamp_utc": self.observed_timestamp_utc, "reference_utc": self.reference_utc,
                "max_age_s": self.max_age_s, "detail": self.detail}


def parse_utc(value: Any) -> datetime:
    """Parse a timezone-aware ISO-8601 timestamp; raise ValueError with a specific reason."""
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("unparseable_timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("timezone_missing")
    return parsed.astimezone(timezone.utc)


def classify_observation(source: str, timestamp: Any, reference_utc: Any, policy: FreshnessPolicy,
                         *, previous: datetime | None = None) -> FreshnessVerdict:
    """Classify one observation timestamp against the frame's reference time.

    Order: MISSING, unparseable (INCONSISTENT), FUTURE_DATED, out of order (INCONSISTENT),
    STALE, FRESH. ``previous`` is the latest timestamp already accepted from the same source;
    a source moving backwards in time is INCONSISTENT, a source repeating a timestamp is not.
    """
    reference = parse_utc(reference_utc)
    ref_text = reference.isoformat(timespec="milliseconds")

    def verdict(status: FreshnessStatus, age: float | None, text: str | None, detail: str | None = None):
        return FreshnessVerdict(source, status, age, text, ref_text, policy.max_age_s, detail)

    if timestamp is None or (isinstance(timestamp, str) and not timestamp.strip()):
        return verdict(FreshnessStatus.MISSING, None, None, "no_timestamp")
    text = timestamp if isinstance(timestamp, str) else str(timestamp)
    try:
        observed = parse_utc(timestamp)
    except ValueError as exc:
        return verdict(FreshnessStatus.INCONSISTENT, None, text, str(exc))
    age = (reference - observed).total_seconds()
    observed_text = observed.isoformat(timespec="milliseconds")
    if age < -policy.max_future_skew_s:
        return verdict(FreshnessStatus.FUTURE_DATED, age, observed_text, f"ahead_of_reference_by_{-age:.3f}s")
    if previous is not None and observed < previous:
        return verdict(FreshnessStatus.INCONSISTENT, age, observed_text, "out_of_order_source")
    if age > policy.max_age_s:
        return verdict(FreshnessStatus.STALE, age, observed_text, f"older_than_limit_by_{age - policy.max_age_s:.3f}s")
    return verdict(FreshnessStatus.FRESH, age, observed_text)


@dataclass(frozen=True)
class FrameFreshness:
    """Verdicts for every dated source of one frame, plus the worst class."""

    policy: FreshnessPolicy
    reference_utc: str
    verdicts: Mapping[str, FreshnessVerdict]

    @property
    def worst(self) -> FreshnessStatus:
        present = {v.status for v in self.verdicts.values()}
        return next((status for status in SEVERITY if status in present), FreshnessStatus.FRESH)

    @property
    def reason_codes(self) -> list[str]:
        return [v.frame_code for v in self.verdicts.values() if v.frame_code]

    @property
    def abstention_reason(self) -> str | None:
        return None if self.worst is FreshnessStatus.FRESH else ABSTENTION_REASON[self.worst]

    def as_dict(self) -> dict[str, Any]:
        return {"policy": self.policy.as_dict(), "reference_utc": self.reference_utc, "worst": self.worst.value,
                "reason_codes": self.reason_codes,
                "sources": {name: v.as_dict() for name, v in self.verdicts.items()}}

    def compact_dict(self) -> dict[str, Any]:
        """Like ``as_dict`` but lists only sources that are not fresh, to keep records small."""
        fresh = sum(1 for v in self.verdicts.values() if v.fresh)
        return {"policy": self.policy.as_dict(), "reference_utc": self.reference_utc, "worst": self.worst.value,
                "reason_codes": self.reason_codes, "sources_checked": len(self.verdicts), "sources_fresh": fresh,
                "non_fresh": {name: v.as_dict() for name, v in self.verdicts.items() if not v.fresh}}


def assess_sources(sources: Mapping[str, Any], reference_utc: Any, policy: FreshnessPolicy,
                   *, previous: Mapping[str, datetime] | None = None) -> FrameFreshness:
    previous = previous or {}
    verdicts = {name: classify_observation(name, ts, reference_utc, policy, previous=previous.get(name))
                for name, ts in sources.items()}
    return FrameFreshness(policy, parse_utc(reference_utc).isoformat(timespec="milliseconds"), verdicts)


class ClockConsistencyMonitor:
    """Detect a wall-clock step by comparing it with the monotonic clock between two frames.

    ``check`` returns the discrepancy in seconds when the UTC clock advanced differently from
    the monotonic clock by more than the limit, else None. It then re-bases on the new pair, so
    one step is reported once and the next frame is judged normally (recovery).
    """

    def __init__(self, max_discrepancy_s: float):
        self.max_discrepancy_s = _number("max_discrepancy_s", max_discrepancy_s, low=0.0, high=5.0, low_inclusive=False)
        self._last: tuple[datetime, int] | None = None

    def check(self, wall: datetime, monotonic_ns: int) -> float | None:
        wall = parse_utc(wall)
        previous, self._last = self._last, (wall, int(monotonic_ns))
        if previous is None:
            return None
        elapsed_wall = (wall - previous[0]).total_seconds()
        elapsed_mono = (int(monotonic_ns) - previous[1]) / 1e9
        discrepancy = abs(elapsed_wall - elapsed_mono)
        return discrepancy if discrepancy > self.max_discrepancy_s else None
