"""Window -> Caliper rounds. Pure python (no numpy) so the WSL runner and the tests can use it.

Round 0  label pr-<run>     the 60 s pre-roll before the window (unmeasured; its telemetry feeds decision 0)
Round k  label sKK-<run>    measured segment k-1 = window [60(k-1), 60k) s, offsets re-based to the segment start (k = 1..10)
Decision d (d = 0..9) is taken when round d has finished and applies to round d+1 (= segment d, which starts at window + 60d s).
"""

from __future__ import annotations

from dataclasses import dataclass


class SegmentError(ValueError):
    pass


@dataclass(frozen=True)
class Round:
    index: int              # Caliper round index (0 = pre-roll)
    name: str               # r00 .. r10 (file stems)
    measured: bool
    segment: int | None     # 0..9 for measured rounds, None for the pre-roll
    window_start_ms: int    # window-relative start of this round (pre-roll: -pre_roll_ms)
    length_ms: int
    offsets_ms: tuple[int, ...]   # relative to the round start, non-decreasing, in [0, length_ms)

    def label(self, run_id: str) -> str:
        return (f"pr-{run_id}" if self.segment is None else f"s{self.segment + 1:02d}-{run_id}")


def _check(offsets, length_ms: int, name: str) -> tuple[int, ...]:
    out = tuple(int(v) for v in offsets)
    if not out:
        raise SegmentError(f"{name}: empty round (an empty Caliper round cannot be replayed)")
    if any(v != int(v) for v in offsets):
        raise SegmentError(f"{name}: offsets must be whole milliseconds")
    if out[0] < 0 or out[-1] >= length_ms:
        raise SegmentError(f"{name}: offsets outside [0, {length_ms})")
    if any(b < a for a, b in zip(out, out[1:])):
        raise SegmentError(f"{name}: offsets must be non-decreasing")
    return out


def build_rounds(measured_ms, pre_ms_rel, length_s: int, pre_roll_s: int, segment_s: int = 60) -> list[Round]:
    """measured_ms: window-relative offsets in [0, length_s*1000); pre_ms_rel: in [-pre_roll_s*1000, 0)."""
    if length_s % segment_s:
        raise SegmentError(f"window length {length_s} s is not a multiple of the {segment_s} s segment")
    seg_ms, length_ms, pre_ms = segment_s * 1000, length_s * 1000, pre_roll_s * 1000
    measured = [int(v) for v in measured_ms]
    if any(b < a for a, b in zip(measured, measured[1:])) or (measured and (measured[0] < 0 or measured[-1] >= length_ms)):
        raise SegmentError("measured offsets must be non-decreasing inside the window")
    pre = [int(v) + pre_ms for v in pre_ms_rel]
    rounds = [Round(0, "r00", False, None, -pre_ms, pre_ms, _check(pre, pre_ms, "pre-roll"))]
    for k in range(length_s // segment_s):
        lo, hi = k * seg_ms, (k + 1) * seg_ms
        part = [v - lo for v in measured if lo <= v < hi]
        rounds.append(Round(k + 1, f"r{k + 1:02d}", True, k, lo, seg_ms, _check(part, seg_ms, f"segment {k}")))
    if sum(len(r.offsets_ms) for r in rounds if r.measured) != len(measured):
        raise SegmentError("segmentation lost or duplicated arrivals")
    return rounds


def schedule_text(offsets: tuple[int, ...]) -> bytes:
    """Caliper replay-rate TEXT body: one integer per line, LF, no trailing newline (as p4_replay.offsets.schedule_bytes)."""
    return b"\n".join(str(int(v)).encode("ascii") for v in offsets)
