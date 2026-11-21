"""Clock-agreement evidence between two hosts, evaluated offline (task A0e, 2026-11-21).

Why. The ledger collector may run in WSL while the telemetry provider runs on Windows. Each stamps its own wall clock, and
the A0d report left their agreement UNVERIFIED. Both being "UTC" is a label, not evidence: two clocks can both be labelled UTC
and differ by seconds. This module turns explicitly sampled readings into an offset interval, and says AGREE, DISAGREE or
INCONCLUSIVE against an operator-supplied tolerance.

What this module does NOT do: it measures nothing. It never calls ``wsl``, ``docker``, ``ssh`` or any process, and it has no
default probe. Samples come from the operator (a future, separately authorized measurement, documented in the runbook) or, in
tests, from synthetic fixtures. A verdict is evidence about the samples it was given.

Sample (one round trip, taken on the REFERENCE host, e.g. Windows, against the REMOTE host, e.g. WSL)::

    ref_wall_before   reference wall clock just before asking the remote host for its time
    remote_wall       the remote host's wall clock reading
    ref_wall_after    reference wall clock just after the answer arrived
    ref_mono_before / ref_mono_after   reference MONOTONIC clock at the same two instants (elapsed time only)
    remote_resolution_ns               the remote reading's resolution, declared by the operator (e.g. 1_000_000 for ms)

A wall reading is given either as an integer ``<name>_ns`` (nanoseconds since the Unix epoch, UTC) or as a timezone-AWARE
ISO-8601 string ``<name>_iso``. A naive timestamp is refused, never assumed to be UTC. Monotonic readings are integer
nanoseconds and are used only for elapsed time, never compared with wall readings as instants.

Method (as the existing handshake in ``scripts/e1/e1_clock_handshake.py``)::

    offset      = (remote + resolution/2) - (ref_before + ref_after)/2        remote minus reference
    uncertainty = (ref_after - ref_before)/2 + resolution/2

so the true offset lies in [offset - uncertainty, offset + uncertainty] if the remote reading was taken between the two
reference readings and the reference wall clock did not step. The second condition is tested: the reference wall elapsed
time and monotonic elapsed time must agree within an operator-supplied bound, else the sample is rejected. All arithmetic is
exact integer arithmetic in units of half a nanosecond.

Combining samples. Each valid sample bounds the offset; if the clocks were steady during the phase the true offset is in all
of the intervals, so their intersection is the tightest bound. An EMPTY intersection means the samples contradict each other
(a step, a bad sample, a drifting clock) and the phase is INCONCLUSIVE, never averaged away.

Verdict against ``tolerance_ms`` (operator-supplied, no default):
  AGREE         the whole interval lies within +-tolerance.
  DISAGREE      the whole interval lies beyond +tolerance or beyond -tolerance (the offset exceeds it even in the best case).
  INCONCLUSIVE  anything else: too few valid samples, contradictory samples, or an interval wide enough to straddle the
                tolerance (the measurement cannot establish agreement at that tolerance).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

CLOCK_PREFLIGHT_VERSION = "clock_preflight/1 (A0e, 2026-11-21)"
AGREE, DISAGREE, INCONCLUSIVE = "AGREE", "DISAGREE", "INCONCLUSIVE"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_NS = 1_000_000

# What an authorized future measurement has to collect (documented here so the runbook and the code cannot drift apart).
REQUIRED_MEASUREMENTS = (
    "At least `min_valid_samples` round trips per phase, taken on the reference host (Windows).",
    "Per round trip: reference wall clock (UTC, timezone-aware) immediately before and after the request, and the reference "
    "monotonic clock at the same two instants.",
    "Per round trip: the remote host's (WSL's) wall clock reading, and the declared resolution of that reading.",
    "One phase before the live round and one after it, so a clock that drifts or is stepped during the run is visible.",
    "The operator-supplied tolerance (ms), minimum valid sample count, and the largest accepted disagreement between wall "
    "and monotonic elapsed time (ms). None has a default.",
)


class ClockSampleError(ValueError):
    """A sample cannot be used. ``code`` is a stable machine-readable cause."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ClockSample:
    ref_wall_before_ns: int
    remote_wall_ns: int
    ref_wall_after_ns: int
    ref_mono_before_ns: int
    ref_mono_after_ns: int
    remote_resolution_ns: int

    @property
    def offset_x2(self) -> int:
        """2 * offset in ns (remote minus reference), exact."""
        return 2 * self.remote_wall_ns + self.remote_resolution_ns - (self.ref_wall_before_ns + self.ref_wall_after_ns)

    @property
    def uncertainty_x2(self) -> int:
        """2 * uncertainty in ns, exact."""
        return (self.ref_wall_after_ns - self.ref_wall_before_ns) + self.remote_resolution_ns


def _wall_ns(sample: Mapping[str, Any], name: str) -> int:
    ns_key, iso_key = f"{name}_ns", f"{name}_iso"
    present = [k for k in (ns_key, iso_key) if sample.get(k) is not None]
    if not present:
        raise ClockSampleError("missing_timestamp", f"{name}: give exactly one of {ns_key} (integer) or {iso_key} (aware ISO-8601)")
    if len(present) == 2:
        raise ClockSampleError("ambiguous_timestamp", f"{name}: both {ns_key} and {iso_key} were given; give exactly one")
    key = present[0]
    value = sample[key]
    if key == ns_key:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ClockSampleError("malformed_timestamp", f"{ns_key} must be an integer number of nanoseconds, got {value!r}")
        if value < 0:
            raise ClockSampleError("malformed_timestamp", f"{ns_key} is before the Unix epoch: {value}")
        return value
    if not isinstance(value, str):
        raise ClockSampleError("malformed_timestamp", f"{iso_key} must be a string, got {type(value).__name__}")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ClockSampleError("malformed_timestamp", f"{iso_key} is not ISO-8601: {value!r}") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ClockSampleError("naive_timestamp", f"{iso_key} carries no timezone ({value!r}); it is refused, not assumed to be UTC")
    delta: timedelta = parsed - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _int_field(sample: Mapping[str, Any], key: str, *, positive: bool = False) -> int:
    value = sample.get(key)
    if value is None:
        raise ClockSampleError("missing_field", f"{key} is missing")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ClockSampleError("malformed_field", f"{key} must be an integer, got {value!r}")
    if value < 0 or (positive and value == 0):
        raise ClockSampleError("malformed_field", f"{key} must be {'positive' if positive else 'non-negative'}, got {value}")
    return value


def parse_sample(sample: Any) -> ClockSample:
    """Build a :class:`ClockSample` or raise :class:`ClockSampleError`. Nothing is repaired."""
    if not isinstance(sample, Mapping):
        raise ClockSampleError("malformed_sample", f"a sample must be an object, got {type(sample).__name__}")
    return ClockSample(
        ref_wall_before_ns=_wall_ns(sample, "ref_wall_before"), remote_wall_ns=_wall_ns(sample, "remote_wall"),
        ref_wall_after_ns=_wall_ns(sample, "ref_wall_after"),
        ref_mono_before_ns=_int_field(sample, "ref_mono_before_ns"), ref_mono_after_ns=_int_field(sample, "ref_mono_after_ns"),
        remote_resolution_ns=_int_field(sample, "remote_resolution_ns", positive=True))


def _require_positive_number(value: Any, name: str) -> float:
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero (operator-supplied, no default); got {value!r}")
    return float(value)


def _usable(sample: ClockSample, max_step_ms: float) -> None:
    """Raise ClockSampleError if the sample's own timing is not trustworthy."""
    wall = sample.ref_wall_after_ns - sample.ref_wall_before_ns
    mono = sample.ref_mono_after_ns - sample.ref_mono_before_ns
    if mono <= 0:
        raise ClockSampleError("monotonic_not_advancing", f"monotonic elapsed time is {mono} ns; it must be positive")
    if wall < 0:
        raise ClockSampleError("wall_clock_went_backwards", f"reference wall clock moved back {-wall} ns during the sample")
    if abs(wall - mono) > max_step_ms * _NS:
        raise ClockSampleError("wall_clock_stepped_during_sample",
                               f"wall elapsed {wall} ns differs from monotonic elapsed {mono} ns by more than {max_step_ms} ms")


def assess_clock_agreement(samples: Sequence[Any], *, tolerance_ms: Any, min_valid_samples: Any,
                           max_wall_monotonic_disagreement_ms: Any, phase: str = "phase") -> dict[str, Any]:
    """Assess one phase of samples. Raises ValueError for an unusable parameter; never for bad samples (they are reported)."""
    tolerance = _require_positive_number(tolerance_ms, "tolerance_ms")
    max_step = _require_positive_number(max_wall_monotonic_disagreement_ms, "max_wall_monotonic_disagreement_ms")
    if min_valid_samples is None or isinstance(min_valid_samples, bool) or not isinstance(min_valid_samples, int) or min_valid_samples < 1:
        raise ValueError(f"min_valid_samples must be a positive integer (operator-supplied, no default); got {min_valid_samples!r}")
    result: dict[str, Any] = {
        "phase": phase, "tolerance_ms": tolerance, "min_valid_samples": min_valid_samples,
        "max_wall_monotonic_disagreement_ms": max_step, "samples_given": len(samples) if isinstance(samples, Sequence) else None,
        "evidence_origin": "operator-supplied samples; this module measures nothing",
        "utc_label_is_not_evidence": True, "version": CLOCK_PREFLIGHT_VERSION}
    if not isinstance(samples, Sequence) or isinstance(samples, (str, bytes)):
        return {**result, "verdict": INCONCLUSIVE, "reason": "samples_not_a_list", "valid_samples": 0, "rejected": []}
    valid: list[tuple[int, ClockSample]] = []
    rejected: list[dict[str, Any]] = []
    for index, raw in enumerate(samples):
        try:
            parsed = parse_sample(raw)
            _usable(parsed, max_step)
        except ClockSampleError as exc:
            rejected.append({"index": index, "code": exc.code, "message": str(exc)})
            continue
        valid.append((index, parsed))
    result.update(valid_samples=len(valid), rejected=rejected)
    if len(valid) < min_valid_samples:
        return {**result, "verdict": INCONCLUSIVE, "reason": "insufficient_valid_samples" if samples else "no_samples"}
    lows = [s.offset_x2 - s.uncertainty_x2 for _, s in valid]
    highs = [s.offset_x2 + s.uncertainty_x2 for _, s in valid]
    low, high = max(lows), min(highs)
    best_index, best = min(valid, key=lambda item: item[1].uncertainty_x2)
    to_ms = lambda x2: x2 / 2 / _NS
    result.update(best_sample_index=best_index, best_sample_uncertainty_ms=to_ms(best.uncertainty_x2),
                  widest_sample_uncertainty_ms=to_ms(max(s.uncertainty_x2 for _, s in valid)))
    if low > high:
        return {**result, "verdict": INCONCLUSIVE, "reason": "samples_contradict_each_other",
                "interval_ms": None, "largest_low_ms": to_ms(low), "smallest_high_ms": to_ms(high)}
    tol_x2 = int(round(tolerance * _NS * 2))
    result["interval_ms"] = {"low": to_ms(low), "high": to_ms(high)}
    if max(abs(low), abs(high)) <= tol_x2:
        return {**result, "verdict": AGREE, "reason": "offset_interval_within_tolerance"}
    if low > tol_x2 or high < -tol_x2:
        return {**result, "verdict": DISAGREE, "reason": "offset_exceeds_tolerance_even_in_the_best_case"}
    return {**result, "verdict": INCONCLUSIVE, "reason": "uncertainty_straddles_tolerance"}


def assess_phases(phases: Mapping[str, Sequence[Any]], **parameters: Any) -> dict[str, Any]:
    """Assess named phases (for example ``pre_run`` and ``post_run``). AGREE only if every phase AGREEs."""
    if not isinstance(phases, Mapping) or not phases:
        raise ValueError("phases must be a non-empty mapping of phase name to samples")
    results = {name: assess_clock_agreement(samples, phase=name, **parameters) for name, samples in phases.items()}
    verdicts = {r["verdict"] for r in results.values()}
    overall = DISAGREE if DISAGREE in verdicts else (AGREE if verdicts == {AGREE} else INCONCLUSIVE)
    return {"verdict": overall, "phases": results, "version": CLOCK_PREFLIGHT_VERSION,
            "note": "evidence about the supplied samples only; it does not show that the clocks agree outside the sampled phases"}


class ClockProbe:
    """Interface for a FUTURE, separately authorized measurement. There is deliberately no implementation here.

    ``sample()`` must return one mapping in the sample format above. This module never constructs a probe.
    """

    def sample(self) -> Mapping[str, Any]:                                    # pragma: no cover - interface only
        raise NotImplementedError


def collect_samples(probe: Any, count: int, *, sleep: Callable[[float], None] | None = None, pause_s: float = 0.0) -> list[Mapping[str, Any]]:
    """Take ``count`` samples from an injected probe. Tests pass a synthetic probe; no probe exists in this module."""
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("count must be a positive integer")
    if not hasattr(probe, "sample"):
        raise TypeError("probe must provide sample()")
    out = []
    for index in range(count):
        if index and pause_s and sleep is not None:
            sleep(pause_s)
        out.append(probe.sample())
    return out


def main(argv: list[str] | None = None) -> int:
    """Evaluate a JSON file of samples. Reads that file and prints a verdict; contacts nothing."""
    parser = argparse.ArgumentParser(prog="clock_preflight", description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--samples", required=True, type=Path,
                        help='JSON file: {"<phase>": [sample, ...], ...} or a bare list (one phase named "phase")')
    parser.add_argument("--tolerance-ms", required=True, type=float)
    parser.add_argument("--min-valid-samples", required=True, type=int)
    parser.add_argument("--max-wall-monotonic-disagreement-ms", required=True, type=float)
    args = parser.parse_args(argv)
    try:
        data = json.loads(args.samples.read_text(encoding="utf-8"))
        phases = data if isinstance(data, Mapping) else {"phase": data}
        result = assess_phases(phases, tolerance_ms=args.tolerance_ms, min_valid_samples=args.min_valid_samples,
                               max_wall_monotonic_disagreement_ms=args.max_wall_monotonic_disagreement_ms)
    except (OSError, ValueError) as exc:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {exc}")
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return {AGREE: 0, DISAGREE: 1, INCONCLUSIVE: 3}[result["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
