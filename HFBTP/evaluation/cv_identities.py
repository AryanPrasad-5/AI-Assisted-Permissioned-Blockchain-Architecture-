"""Observed-column checks. No simulator, no model, no fitted parameter.

The identity comes from Caliper v0.5.0 report.js:
    Send rate  A = n / (lastCreate - firstCreate)
    Throughput T = n / (lastFinish - firstCreate)
so drain = lastFinish - lastCreate = n * (1/T - 1/A), and drain <= Max Latency because the
transaction finishing last was created no later than lastCreate. Max Latency here is over
successful transactions, while lastFinish covers all finished ones, so a heavy failure tail could
weaken the bound. HFBTP and MMBPD publish no failure count.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from cv_common import ACTUAL, AVG_LAT, BLOCK, MAX_LAT, MIN_LAT, ORDERERS, SET, TPUT


def n_bound_violation_rate(throughput, send_rate, max_latency, n: int) -> float:
    """Share of rows where the drain implied by n exceeds Max Latency, allowing printed rounding."""
    t, a, m = map(lambda v: np.asarray(v, float), (throughput, send_rate, max_latency))
    drain = n * (1 / t - 1 / a)
    slack = n * (0.05 / t**2 + 0.05 / a**2) + 0.005
    return float(np.mean(drain > m + slack))


def divisor_scan(frame, set_col, block_col, tput_col, n_values=range(100, 3005, 5), max_block=125, min_level=100):
    """Rank candidate N by level-adjusted mean T of divisor blocks minus non-divisor blocks."""
    data = frame[(frame[block_col] <= max_block) & (frame[set_col] >= min_level)].copy()
    data["residual"] = data[tput_col] - data.groupby(set_col)[tput_col].transform("mean")
    by_block = data.groupby(block_col)["residual"].mean()
    rows = []
    for n in n_values:
        divisors = [b for b in by_block.index if n % int(b) == 0]
        if 2 <= len(divisors) <= len(by_block) - 2:
            contrast = float(by_block[divisors].mean() - by_block.drop(divisors).mean())
            rows.append({"n": n, "contrast": contrast, "divisors": [int(b) for b in divisors]})
    return sorted(rows, key=lambda row: -row["contrast"])


def timer_reading(frame, set_col, block_col=BLOCK, bands=(0, 55, 100, 145, 200)):
    """Median Max and Min Latency for small non-divisor blocks, whose last block must wait."""
    data = frame[frame[block_col].isin([15, 30, 35, 45, 55])]
    grouped = data.groupby(pd.cut(data[set_col], list(bands)), observed=True)[[MAX_LAT, MIN_LAT]].median()
    return {str(index): row.round(3).to_dict() for index, row in grouped.iterrows()}


def divisor_contrast(frame, set_col, block_col=BLOCK, tput_col=TPUT, min_level=100):
    """Mean T by level in the fill regime for blocks dividing 1000 vs the rest."""
    data = frame[(frame[block_col] <= 2 * frame[set_col]) & (frame[set_col] >= min_level)].copy()
    data["divides"] = 1000 % data[block_col] == 0
    table = data.groupby([set_col, "divides"])[tput_col].mean().unstack()
    return (table[True] - table[False]).round(2)


def level_165_structure(h) -> dict:
    data = h[h[SET] == 165]
    upper = data[TPUT] > 140
    ordered = data.assign(upper=upper).sort_index()["upper"].astype(int).to_numpy()
    runs = 1 + int((ordered[1:] != ordered[:-1]).sum())
    p = float(ordered.mean())
    wide = data.assign(upper=upper.astype(float)).pivot_table(index=BLOCK, columns=ORDERERS, values="upper")
    agree = wide.mean(axis=1).isin([0.0, 1.0]).mean()
    return {
        "below_130": int((data[TPUT] < 130).sum()),
        "between_130_150": int(((data[TPUT] >= 130) & (data[TPUT] < 150)).sum()),
        "at_least_150": int((data[TPUT] >= 150).sum()),
        "upper_share_by_orderers": data.assign(upper=upper).groupby(ORDERERS)["upper"].mean().round(3).to_dict(),
        "runs_in_file_order": runs,
        "runs_expected_if_random": round(1 + 2 * len(ordered) * p * (1 - p), 1),
        "same_class_across_orderers_share": round(float(agree), 3),
        "median_max_latency_lower_upper": [
            float(data.loc[~upper, MAX_LAT].median()), float(data.loc[upper, MAX_LAT].median())],
        "mean_actual_rate_lower_upper": [
            round(float(data.loc[~upper, ACTUAL].mean()), 2), round(float(data.loc[upper, ACTUAL].mean()), 2)],
    }


def block_10_stalls(h) -> dict:
    wide = h[(h[BLOCK] == 10) & (h[SET] >= 150)].pivot_table(index=SET, columns=ORDERERS, values=TPUT)
    peak = h[(h[BLOCK] == 10) & (h[SET] >= 150)].pivot_table(index=SET, columns=ORDERERS, values=MAX_LAT)
    near = h[(h[BLOCK] == 15) & (h[SET] >= 150)].pivot_table(index=SET, columns=ORDERERS, values=TPUT)
    return {
        "throughput": {str(int(k)): v for k, v in wide.round(1).to_dict("index").items()},
        "max_latency": {str(int(k)): v for k, v in peak.round(2).to_dict("index").items()},
        "block_15_throughput": {str(int(k)): v for k, v in near.round(1).to_dict("index").items()},
    }


def heavy_decomposition(h) -> dict:
    train = h[h[SET] <= 145]
    heavy = h[h[SET] >= 165]
    copy145 = h[h[SET] == 145].set_index([BLOCK, ORDERERS])[TPUT]
    copied = heavy.set_index([BLOCK, ORDERERS]).index.map(copy145).to_numpy()
    own_max = heavy.set_index([BLOCK, ORDERERS]).index.map(train.groupby([BLOCK, ORDERERS])[TPUT].max()).to_numpy()
    total = heavy[TPUT].var()
    share = lambda column: float(heavy.groupby(column)[TPUT].transform("mean").var() / total)
    return {
        "mean_t_level_145": float(h.loc[h[SET] == 145, TPUT].mean()),
        "mean_t_heavy": float(heavy[TPUT].mean()),
        "mae_identity": float((heavy[SET] - heavy[TPUT]).abs().mean()),
        "mae_copy_level_145": float(np.abs(copied - heavy[TPUT].to_numpy()).mean()),
        "share_above_own_config_train_max": float((heavy[TPUT].to_numpy() > own_max).mean()),
        "share_above_global_train_max": float((heavy[TPUT] > train[TPUT].max()).mean()),
        "variance_share_level": share(SET),
        "variance_share_block": share(BLOCK),
        "variance_share_orderers": share(ORDERERS),
    }


def large_block_flatness(h, threshold: int = 130) -> dict:
    """Quasi-replicate spread among blocks >= threshold. A proxy, never a noise estimate."""
    data = h[h[BLOCK] >= threshold]
    grouped = data.groupby(SET)[TPUT]
    iqr = grouped.quantile(0.75) - grouped.quantile(0.25)
    return {"median_iqr": float(iqr.median()), "iqr_level_165": float(iqr.loc[165])}


def interpolation_scores(h, manifests, partition_keys) -> dict:
    """`interp_set`: linear interpolation along Set within each (Block, Orderers), train levels only."""
    scores = {}
    for name in ["T2D0", "T2D1", "T2D2", "T2D3"]:
        manifest = manifests[name]
        columns = manifest["config_key_columns"]
        levels = np.array(sorted(pd.DataFrame(manifest["train_keys"], columns=columns)[SET].unique()), float)
        test = partition_keys(manifests, name).merge(h, on=[SET, BLOCK, ORDERERS])
        throughput = h.pivot_table(index=[BLOCK, ORDERERS], columns=SET, values=TPUT)
        latency = h.pivot_table(index=[BLOCK, ORDERERS], columns=SET, values=AVG_LAT)
        t_hat = [np.interp(s, levels, throughput.loc[(b, o), levels].to_numpy())
                 for s, b, o in zip(test[SET], test[BLOCK], test[ORDERERS])]
        l_hat = [np.interp(s, levels, latency.loc[(b, o), levels].to_numpy())
                 for s, b, o in zip(test[SET], test[BLOCK], test[ORDERERS])]
        scores[name] = {"throughput_mae": float(np.abs(np.array(t_hat) - test[TPUT]).mean()),
                        "latency_mae": float(np.abs(np.array(l_hat) - test[AVG_LAT]).mean())}
    return scores
