"""Per-minute rate trace -> Caliper rounds. Pure python.

SCALING METHOD (documented, pre-registered): a window is described by its per-minute historical mean arrival rates m_i (tx/s).
Segment i is replayed at k * m_i tx/s as n_i = round(60 * k * m_i) arrivals spaced evenly in the segment (offset_j =
floor((2j+1) * 60000 / (2 n_i)) ms). What is preserved: the minute-to-minute shape of the historical load (all ratios between minutes).
What is NOT preserved: everything inside a minute (the 12-s block cycle, 1-s bursts of 100-800 tx/s, arrival over-dispersion). Every
arm gets the identical schedule, so the loss affects all arms alike, but the result is for this rate-trace replay, not for natural
Ethereum load. The control cadence is 60 s, so the controllers only ever see minute-level rates in any case.

Round 0 is the unmeasured 60 s pre-roll (rate k * m_pre); rounds 1..n are the measured segments. For the forecaster the history is
the same construction: 600 s of warm-up (rate k * m_warm for each of the 10 minutes before the pre-roll), then the replayed rounds.
"""

from __future__ import annotations

from Fabric_Live_Testbed.closed_loop.segments import Round, SegmentError, _check

SEG_MS = 60_000


def evenly_spaced(n: int, length_ms: int = SEG_MS) -> tuple[int, ...]:
    if n < 1:
        raise SegmentError("a replay round needs at least one arrival")
    out = tuple((2 * j + 1) * length_ms // (2 * n) for j in range(n))
    return _check(out, length_ms, f"even schedule n={n}")


def n_for_rate(rate_tps: float, length_ms: int = SEG_MS) -> int:
    return max(1, int(round(rate_tps * length_ms / 1000.0)))


def rounds_from_rates(pre_rate_tps: float, segment_rates_tps: list[float]) -> list[Round]:
    """Round 0 = pre-roll at pre_rate_tps; rounds 1..n = measured segments at the given rates."""
    rounds = [Round(0, "r00", False, None, -SEG_MS, SEG_MS, evenly_spaced(n_for_rate(pre_rate_tps)))]
    for i, rate in enumerate(segment_rates_tps):
        rounds.append(Round(i + 1, f"r{i + 1:02d}", True, i, i * SEG_MS, SEG_MS, evenly_spaced(n_for_rate(rate))))
    return rounds


def counts_per_second(offsets_ms: tuple[int, ...], length_ms: int = SEG_MS) -> list[int]:
    out = [0] * (length_ms // 1000)
    for v in offsets_ms:
        out[int(v) // 1000] += 1
    return out


def warmup_counts(warm_minute_rates_tps: list[float]) -> list[int]:
    """600 s of history: each warm-up minute replayed the same way as a segment (evenly spaced), counted per second."""
    out: list[int] = []
    for rate in warm_minute_rates_tps:
        out.extend(counts_per_second(evenly_spaced(n_for_rate(rate))))
    return out


def history_by_round(rounds: list[Round]) -> dict[int, list[int]]:
    return {r.index: counts_per_second(r.offsets_ms, r.length_ms) for r in rounds}


def scaled_rates(minute_rates_tps: list[float], k: float) -> list[float]:
    return [k * float(m) for m in minute_rates_tps]
