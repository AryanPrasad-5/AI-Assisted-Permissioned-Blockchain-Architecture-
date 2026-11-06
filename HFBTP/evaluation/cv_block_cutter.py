"""Deterministic Fabric block-cutting model for one fixed-count Caliper round.

This is a mechanism calculation, not a learned model. It has no fitted weights.
It must only ever be applied to HFBTP. MMBPD predictions are not authorised.

Assumptions (each one is a stated input, not a fact about HFBTP):
  * one open-loop sender; transaction i arrives at i / rate (Caliper fixed-rate schedule)
  * a block closes when it holds `block` transactions or when its timer expires
  * `commit_offset` is one constant added after every cut
  * Caliper throughput = n / (last_finish - first_create)

`simulate_round` uses integer ticks of 1/rate, so ties are exact. The tie rule is explicit.
`byte_cap` models a PreferredMaxBytes-style overflow as a constant transaction count.
Fabric's etcdraft `startTimer` is a no-op while the timer runs, so an overflow cut leaves
the timer running when `timer_survives_overflow` is True (source-derived; HYPOTHESIS for HFBTP).

`legacy_float_round` is the original exploratory code, kept verbatim. It compares floats at
timer ties, so one tied transaction can move between blocks. It exists only to reproduce the
numbers printed in the 2026-11-06 audit.
"""
from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from functools import lru_cache
from math import floor
from typing import Optional

import numpy as np


@dataclass(frozen=True)
class RoundResult:
    round_end: float
    mean_wait: float
    max_wait: float
    min_wait: float
    blocks: int

    def throughput(self, n: int, commit_offset: float) -> float:
        return n / (self.round_end + commit_offset)


@lru_cache(maxsize=None)
def simulate_round(
    rate: int,
    block: int,
    n: int = 1000,
    batch_timeout: Fraction = Fraction(2),
    byte_cap: Optional[int] = None,
    timer_survives_overflow: bool = True,
    tie_inclusive: bool = True,
) -> RoundResult:
    """Return the deterministic round outcome. Times are seconds from the first arrival."""
    if rate <= 0 or block <= 0 or n <= 0:
        raise ValueError("rate, block and n must be positive")
    window = Fraction(batch_timeout) * rate
    whole = floor(window)
    pending_first = None
    pending = 0
    deadline_first = None  # arrival index that started the running timer
    blocks = 0
    wait_sum = Fraction(0)
    wait_max = Fraction(-1)
    wait_min = None
    last_cut = Fraction(0)

    def close(cut: Fraction) -> None:
        nonlocal pending, pending_first, blocks, wait_sum, wait_max, wait_min, last_cut
        count = pending
        first = pending_first
        last = first + count - 1
        wait_sum += count * cut - Fraction(count * (first + last), 2)
        longest, shortest = cut - first, cut - last
        wait_max = max(wait_max, longest)
        wait_min = shortest if wait_min is None else min(wait_min, shortest)
        last_cut = max(last_cut, cut)
        blocks += 1
        pending, pending_first = 0, None

    for index in range(n):
        if deadline_first is not None:
            elapsed = index - deadline_first
            expired = elapsed > whole if tie_inclusive else elapsed >= window
            if expired:
                if pending:
                    close(deadline_first + window)
                deadline_first = None
        if byte_cap is not None and pending + 1 > byte_cap:
            close(Fraction(index))
            if not timer_survives_overflow:
                deadline_first = None
        if pending == 0:
            pending_first = index
        pending += 1
        if pending >= block:
            close(Fraction(index))
            deadline_first = None
        elif deadline_first is None:
            deadline_first = index
    if pending:
        close(deadline_first + window)

    return RoundResult(
        round_end=float(last_cut / rate),
        mean_wait=float(wait_sum / n / rate),
        max_wait=float(wait_max / rate),
        min_wait=float(wait_min / rate),
        blocks=blocks,
    )


def stair_boundaries(n: int, cycle: float, count: int = 12) -> list[float]:
    """Rates at which n/rate crosses a multiple of the timer cycle (timer-cut blocks only)."""
    return [n / (k * cycle) for k in range(1, count + 1)]


def legacy_float_round(lam, b, N=1000, bt=2.0):
    """Verbatim 2026-11-06 scratch function `sim` (a02_mechanism.py). Reproduction use only."""
    t = [i / lam for i in range(N)]
    i = 0
    cuts = []
    while i < N:
        start = t[i]
        deadline = start + bt
        j = min(i + b - 1, N - 1)
        if j == i + b - 1 and t[j] <= deadline:
            cut = t[j]
            e = j
        else:
            e = i
            while e + 1 < N and t[e + 1] <= deadline and e + 1 < i + b:
                e += 1
            cut = deadline
        cuts.append((cut, i, e))
        i = e + 1
    lat = np.zeros(N)
    for c, a, e in cuts:
        lat[a:e + 1] = c - np.array(t[a:e + 1])
    return cuts[-1][0], lat.mean(), lat.max(), lat.min(), len(cuts), N / len(cuts)
