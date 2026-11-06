"""Mechanism scoring on HFBTP only. Selection is restricted to levels <= 145.

Levels >= 150 are never used to choose N, timeout, cap or timer rule. The hypotheses were still
formed after seeing the whole dataset, so a good held-out score is EVIDENCE, not confirmation.
"""
from __future__ import annotations

from fractions import Fraction
from itertools import product

import numpy as np
import pandas as pd

from cv_block_cutter import legacy_float_round, simulate_round
from cv_common import AVG_LAT, BLOCK, MAX_LAT, MIN_LAT, ORDERERS, PARTITIONS, SET, TPUT, partition_keys


def commit_offset(h) -> float:
    """Median Min Latency on levels <= 145. Probably understates the offset in timer-cut rows."""
    return float(h.loc[h[SET] <= 145, MIN_LAT].median())


def _selection_cells(h, max_block=None):
    grouped = h[h[SET] <= 145].groupby([SET, BLOCK])[[TPUT, AVG_LAT, MAX_LAT]].mean().reset_index()
    if max_block is not None:
        return grouped[grouped[BLOCK] <= max_block]
    return grouped[(grouped[BLOCK] % 15 == 0) | (grouped[BLOCK] <= 130)]


def _score(cells, offset, n, timeout, **variant) -> dict:
    outcomes = [simulate_round(int(a), int(b), n, Fraction(timeout), **variant)
                for a, b in zip(cells[SET], cells[BLOCK])]
    end = np.array([o.round_end for o in outcomes])
    wait = np.array([o.mean_wait for o in outcomes])
    peak = np.array([o.max_wait for o in outcomes])
    return {"throughput_mae": float(np.abs(n / (end + offset) - cells[TPUT]).mean()),
            "latency_mae": float(np.abs(wait + offset - cells[AVG_LAT]).mean()),
            "max_latency_mae": float(np.abs(peak + offset - cells[MAX_LAT]).mean())}


def fill_regime_scan(h, offset) -> pd.DataFrame:
    """N and timeout are separable here because size cuts occur. Blocks <= 125, levels <= 145."""
    cells = _selection_cells(h, max_block=125)
    rows = [{"n": n, "timeout": str(t), **_score(cells, offset, n, t)}
            for n, t in product((400, 500, 800, 900, 950, 1000, 1050, 1100, 1200, 1500, 2000),
                                (Fraction(1), Fraction(3, 2), Fraction(9, 5), Fraction(19, 10), Fraction(2),
                                 Fraction(21, 10), Fraction(11, 5), Fraction(5, 2)))]
    frame = pd.DataFrame(rows)
    frame["joint"] = frame.throughput_mae / frame.throughput_mae.min() + frame.max_latency_mae / frame.max_latency_mae.min()
    return frame.sort_values("joint")


def all_block_scan(h, offset) -> pd.DataFrame:
    """The pre-declared 2026-11-06 rule. It selected N=500, timeout=1: only N/timeout is identified."""
    cells = _selection_cells(h)
    rows = [{"n": n, "timeout": str(t), **_score(cells, offset, n, t)}
            for n, t in product((500, 800, 900, 950, 1000, 1050, 1100, 1200),
                                (Fraction(1), Fraction(3, 2), Fraction(19, 10), Fraction(2), Fraction(21, 10), Fraction(5, 2)))]
    return pd.DataFrame(rows).sort_values("throughput_mae")


def timer_cap_scan(h, offset) -> pd.DataFrame:
    """N=1000, timeout=2. Compares timer rules across constant byte-style caps."""
    cells = _selection_cells(h)
    rows = [{"cap": 0, "timer_survives": True, **_score(cells, offset, 1000, 2)}]
    for cap, survives in product(range(100, 205, 2), (True, False)):
        rows.append({"cap": cap, "timer_survives": survives,
                     **_score(cells, offset, 1000, 2, byte_cap=cap, timer_survives_overflow=survives)})
    frame = pd.DataFrame(rows)
    frame["joint"] = sum(frame[c] / frame[c].min() for c in ("throughput_mae", "latency_mae", "max_latency_mae"))
    return frame.sort_values("joint")


def prediction_table(h, offset, n=1000, timeout=2, **variant) -> pd.DataFrame:
    rows = []
    for rate in sorted(h[SET].unique()):
        for block in sorted(h[BLOCK].unique()):
            o = simulate_round(int(rate), int(block), n, Fraction(timeout), **variant)
            rows.append((rate, block, n / (o.round_end + offset), o.mean_wait + offset, o.max_wait + offset))
    return h.merge(pd.DataFrame(rows, columns=[SET, BLOCK, "t_hat", "l_hat", "max_hat"]), on=[SET, BLOCK])


def legacy_prediction_table(h, offset) -> pd.DataFrame:
    rows = []
    for rate in sorted(h[SET].unique()):
        for block in sorted(h[BLOCK].unique()):
            o = legacy_float_round(float(rate), int(block))
            rows.append((rate, block, 1000 / (o[0] + offset), o[1] + offset, o[2] + offset))
    return h.merge(pd.DataFrame(rows, columns=[SET, BLOCK, "t_hat", "l_hat", "max_hat"]), on=[SET, BLOCK])


def partition_scores(table, manifests) -> dict:
    scores = {}
    for name in PARTITIONS:
        part = partition_keys(manifests, name).merge(table, on=[SET, BLOCK, ORDERERS])
        scores[name] = {"n": int(len(part)),
                        "throughput_mae": float((part.t_hat - part[TPUT]).abs().mean()),
                        "latency_mae": float((part.l_hat - part[AVG_LAT]).abs().mean()),
                        "max_latency_mae": float((part.max_hat - part[MAX_LAT]).abs().mean())}
    return scores


def phase1c_reference(results) -> dict:
    reference = {}
    for name in PARTITIONS:
        part = results[results.manifest == name]
        for target, key in (("Throughput", "throughput"), ("Avg Latency", "latency")):
            rows = part[part.target == target]
            selected = rows[rows.val_mae.notna()].sort_values("val_mae").iloc[0]
            best = rows.sort_values("test_mae").iloc[0]
            reference.setdefault(name, {})[key] = {
                "selected_family": selected.family, "selected_mae": float(selected.test_mae),
                "best_family": best.family, "best_mae": float(best.test_mae)}
    return reference
