"""MMBPD observed-column evidence. Generates no predictions of any MMBPD target.

Hard boundary: this module must not import cv_block_cutter, cv_mechanism, or any model.
tests/construct_validity/test_guards.py enforces that.
"""
from __future__ import annotations

import pandas as pd

from cv_common import BLOCK, M_SEND, M_SET, MAX_LAT, ORDERERS, SET, TPUT, AVG_LAT
from cv_identities import divisor_contrast, divisor_scan, n_bound_violation_rate, timer_reading


def mmbpd_summary(mmbpd) -> dict:
    scan = divisor_scan(mmbpd, M_SET, BLOCK, TPUT)
    levels = mmbpd.groupby(M_SET)[TPUT].mean()
    return {
        "n_bound_violation": {str(n): n_bound_violation_rate(mmbpd[TPUT], mmbpd[M_SEND], mmbpd[MAX_LAT], n)
                              for n in (500, 1000, 1050, 1100, 1200, 2000)},
        "divisor_scan_top3": scan[:3],
        "divisor_rank_of_1000": next(i for i, row in enumerate(scan) if row["n"] == 1000),
        "timer_reading": timer_reading(mmbpd, M_SET),
        "largest_level_mean_steps": {str(int(k)): round(float(v), 2)
                                     for k, v in levels.diff().nlargest(6).items()},
        "divisor_contrast_by_level": {str(int(k)): float(v) for k, v in divisor_contrast(mmbpd, M_SET).items()},
        "send_rate_at_set_200_blocks_10_70": [float(mmbpd.loc[(mmbpd[M_SET] == 200) & (mmbpd[BLOCK] <= 70), M_SEND].min()),
                                              float(mmbpd.loc[(mmbpd[M_SET] == 200) & (mmbpd[BLOCK] <= 70), M_SEND].max())],
    }


def orderer_gap(h, mmbpd) -> dict:
    """Observed-to-observed distance only. Shows Orderers is not identifiable from MMBPD."""
    shared = h[h[BLOCK] <= 400]
    wide = shared.pivot_table(index=[SET, BLOCK], columns=ORDERERS, values=TPUT)
    total = shared[TPUT].var()
    observed = mmbpd[~((mmbpd[M_SET] == 65) & (mmbpd[BLOCK] == 35))].set_index([M_SET, BLOCK])
    distance = {}
    for orderers in (3, 5, 7, 9):
        side = shared[shared[ORDERERS] == orderers].set_index([SET, BLOCK])
        side.index.names = [M_SET, BLOCK]
        joined = observed.join(side, lsuffix="_m", rsuffix="_h", how="inner")
        distance[str(orderers)] = {
            "cells": len(joined),
            "throughput": round(float((joined[f"{TPUT}_m"] - joined[f"{TPUT}_h"]).abs().mean()), 3),
            "latency": round(float((joined[f"{AVG_LAT}_m"] - joined[f"{AVG_LAT}_h"]).abs().mean()), 4),
        }
    spread = wide.max(axis=1) - wide.min(axis=1)
    bands = spread.groupby(pd.cut(wide.index.get_level_values(0), [0, 55, 145, 160, 200]), observed=True).mean()
    return {
        "orderer_main_effect_variance_share": float(shared.groupby(ORDERERS)[TPUT].transform("mean").var() / total),
        "mean_abs_gap_to_orderers_3": {str(o): round(float((wide[o] - wide[3]).abs().mean()), 3) for o in (5, 7, 9)},
        "spread_by_band": {str(k): round(float(v), 2) for k, v in bands.items()},
        "distance_by_assumed_orderers": distance,
    }
