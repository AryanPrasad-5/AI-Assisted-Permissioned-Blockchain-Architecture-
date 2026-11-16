"""Written E1 predictions from the block cutter and HFBTP reference cells. Runs nothing live.

Boundary rates are where n/rate crosses a multiple of the timer cycle. They matter only for
blocks cut by the timer. Size-cut blocks (for example 40 and 45 at 160-170 tx/s) have no stair.
Duration-mode reporting bias is computed from the same Caliper formula.
"""
from __future__ import annotations

from fractions import Fraction

from cv_block_cutter import simulate_round
from cv_common import ACTUAL, BLOCK, MAX_LAT, SET, TPUT


def predicted_fixed_count(rate: int, block: int, offset: float, n=1000, cap=None) -> dict:
    outcome = simulate_round(rate, block, n, Fraction(2), byte_cap=cap)
    return {"throughput": round(outcome.throughput(n, offset), 2), "blocks": outcome.blocks,
            "max_wait": round(outcome.max_wait + offset, 3),
            "drain": round(outcome.round_end + offset - (n - 1) / rate, 3)}


def predicted_duration(rate: int, seconds: int = 120, drain_range=(0.0, 2.2)) -> dict:
    """Below capacity. Windowed commit rate equals offered; Caliper's report includes the drain."""
    sent = rate * seconds
    return {"windowed_throughput": float(rate),
            "caliper_report_range": [round(sent / (seconds + d), 2) for d in reversed(drain_range)]}


def hfbtp_reference(h, rates, blocks) -> dict:
    data = h[h[SET].isin(rates) & h[BLOCK].isin(blocks)]
    table = data.groupby([SET, BLOCK]).agg(median_t=(TPUT, "median"), min_t=(TPUT, "min"), max_t=(TPUT, "max"),
                                           median_max_latency=(MAX_LAT, "median"), mean_actual=(ACTUAL, "mean"))
    return {f"{int(s)}|{int(b)}": row.round(2).to_dict() for (s, b), row in table.iterrows()}


def design_table(h, offset, cap) -> dict:
    pairs = {"k3_primary": (160, 170), "k4_fallback": (120, 130), "k5_fallback": (95, 105), "k6_fallback": (80, 85)}
    blocks = (40, 45, 500)
    table = {}
    for label, (low, high) in pairs.items():
        table[label] = {
            "rates": [low, high],
            "fixed_count_no_cap": {f"{r}|{b}": predicted_fixed_count(r, b, offset) for r in (low, high) for b in blocks},
            "fixed_count_cap": {f"{r}|{b}": predicted_fixed_count(r, b, offset, cap=cap) for r in (low, high) for b in blocks},
            "duration_mode": {str(r): predicted_duration(r) for r in (low, high)},
            "hfbtp_reference": hfbtp_reference(h, (low, high), blocks),
        }
    return table
