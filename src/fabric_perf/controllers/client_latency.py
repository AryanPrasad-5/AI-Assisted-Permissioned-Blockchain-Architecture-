"""Client transaction latency: one definition, validated, never zero-filled (task A0c, G1).

Definition (the project's verified Caliper definition, see
``experiments/results/e1/hfbtp_source_to_e1_alignment_second_20261111/latency_definition.json``):

    latency_ms = final_ms - create_ms

Both timestamps are client wall-clock epoch milliseconds taken by Caliper (``Date.now()``) in the same worker
process: ``create_ms`` immediately before the proposal is endorsed, ``final_ms`` when the gateway reports the commit
status (or the failure). It is a measured interval. It is not a predicted latency, and it is not a Prometheus value.

Only successful transactions are latency-eligible (a failed transaction's interval is the time to the failure, a
different quantity). The percentile is nearest-rank, ``ceil(q * n)``-th smallest, as everywhere else in the project.
A p95 describes one window of raw samples. Percentiles of different windows must never be averaged; pool the raw
samples instead.

Standard library only. Nothing here reads a file or touches a network.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

LATENCY_SCHEMA_VERSION = "client_latency/1"
LATENCY_UNIT = "ms"
LATENCY_DEFINITION = "latency_ms = final_ms - create_ms (client wall clock, Caliper Date.now(), epoch milliseconds)"
PERCENTILE_METHOD = "nearest-rank: the ceil(q*n)-th smallest sample of one window; never averaged across windows"
ELIGIBLE_STATUSES = frozenset({"success", "succeeded", "valid"})
# An explicit latency column and the two timestamps it was computed from may differ only by float noise.
EXPLICIT_COLUMN_TOLERANCE_MS = 1.0


class LatencyState(str, Enum):
    OK = "ok"
    MISSING_CREATE = "missing_create_timestamp"
    MISSING_FINAL = "missing_final_timestamp"
    NOT_NUMERIC = "not_numeric"
    NONFINITE = "nonfinite"
    NONPOSITIVE_TIMESTAMP = "nonpositive_timestamp"
    NEGATIVE = "negative_latency"
    BOOLEAN = "boolean_value"


@dataclass(frozen=True)
class LatencySample:
    """One transaction's latency, or the reason there is none. ``latency_ms`` is None unless ``state`` is OK."""

    state: LatencyState
    latency_ms: float | None = None

    @property
    def ok(self) -> bool:
        return self.state is LatencyState.OK


def _number(value: object, missing: LatencyState) -> tuple[float | None, LatencyState | None]:
    """Parse one epoch-millisecond field. Blank is MISSING, never zero."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, missing
    if isinstance(value, bool):
        return None, LatencyState.BOOLEAN
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None, LatencyState.NOT_NUMERIC
    if not math.isfinite(number):
        return None, LatencyState.NONFINITE
    if number <= 0:
        return None, LatencyState.NONPOSITIVE_TIMESTAMP
    return number, None


def derive_latency_ms(create_ms: object, final_ms: object) -> LatencySample:
    """``final_ms - create_ms`` for one transaction, or the explicit reason it cannot be computed.

    A timestamp that is blank, not numeric, not finite or not positive is refused (a zero or negative epoch would
    otherwise yield an absurd interval against a real timestamp). A negative interval is refused. An interval of
    exactly zero is valid: both clocks have millisecond resolution.
    """
    create, bad = _number(create_ms, LatencyState.MISSING_CREATE)
    if bad is not None:
        return LatencySample(bad)
    final, bad = _number(final_ms, LatencyState.MISSING_FINAL)
    if bad is not None:
        return LatencySample(bad)
    latency = final - create  # type: ignore[operator]
    if latency < 0:
        return LatencySample(LatencyState.NEGATIVE)
    return LatencySample(LatencyState.OK, latency)


def is_eligible_status(status: object) -> bool:
    return str(status).strip().lower() in ELIGIBLE_STATUSES


def nearest_rank(values: Iterable[float], q: float = 0.95) -> float | None:
    """Nearest-rank percentile of one window of raw samples; None for an empty window (never 0)."""
    if isinstance(q, bool) or not 0 < q <= 1:
        raise ValueError("q must be in (0, 1]")
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]
