"""HFBTP all-row forensic test.

Implements docs/research/forensics/hfbtp_forensic_prereg_v0.1_2026-11-09.md as amended by
docs/research/forensics/hfbtp_forensic_prereg_v0.2_amendment_2026-11-09.md.

Computation class (owner brief, 2026-11-09): closed-form arithmetic on observed HFBTP columns.
Not used: the block-cutter simulator, Monte Carlo, bootstrap, ML models, MMBPD.
HFBTP.csv is opened read-only. Outputs go only to a new directory given by --out, and the script
refuses to reuse an existing directory.

n = 1000 is a CANDIDATE reconstruction, not a documented HFBTP setting. c (effective timer cycle)
is a nuisance range around the Fabric default BatchTimeout of 2 s, not a documented HFBTP setting.

Usage, from the repository root:
    .venv/Scripts/python.exe scripts/forensics/hfbtp_allrow_forensics.py \
        --out docs/research/forensics/results_2026-11-09/run_01
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import platform
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
HFBTP_CSV = REPO_ROOT / "data" / "raw" / "hfbtp" / "HFBTP.csv"
FORENSICS_DOCS = REPO_ROOT / "docs" / "research" / "forensics"
PREREGS = {
    "hfbtp_forensic_prereg_v0.1_2026-11-09.md": "588015540311686e08fa892cd30a59aea9a43fe1be6722fa9cb1be97fe0e345b",
    "hfbtp_forensic_prereg_v0.2_amendment_2026-11-09.md": "530b8e27ae40b1c9401d9fa4beaed7e734ed68f6bf91ca210836b18e0eac0fea",
}
HFBTP_MD5 = "e9275c49a2d7dc1f2a4015b6d8b28482"
HFBTP_SHA256 = "ce295d6451787fefcfe2101579238cda4bede8539bc302e6a482aa4e5782bd5e"

SET, ACT, BLK, ORD = "Set Transaction Arrival Rate", "Actual Transaction Arrival Rate", "Block Size", "Orderers"
TPUT, AVG, MIN, MAX = "Throughput", "Avg Latency", "Min Latency", "Max Latency"
COLUMNS = [SET, ACT, BLK, ORD, TPUT, AVG, MIN, MAX]

# ---- pre-registered constants (v0.1 + v0.2) -------------------------------------------------
N0 = 1000
C_GRID = np.round(np.arange(2.001, 2.035 + 1e-9, 0.0005), 4)
C_NARROW = np.round(np.arange(2.001, 2.005 + 1e-9, 0.0005), 4)
C_TIE_IN, C_TIE_OUT = 2.001, 1.999
C_EXPLORE = np.round(np.arange(1.950, 2.100 + 1e-9, 0.005), 3)
HALF_T, HALF_A, HALF_M = 0.05, 0.05, 0.005
ALLOW = 2.0
JITTER_S = 0.1
L_TOL_S = 0.15
PACE_LO, PACE_HI = 0.98, 1.02
BANDS = [("20-45", 20, 45), ("50-95", 50, 95), ("100-145", 100, 145), ("150-200", 150, 200)]
COUNT_BANDS = ["50-95", "100-145", "150-200"]
N_SENS = [900, 950, 990, 995, 999, 1000, 1001, 1005, 1010, 1050, 1100]
N_PROFILE = np.arange(100, 5001)
CORNERS = [(a, b, c) for a in (-HALF_T, HALF_T) for b in (-HALF_A, HALF_A) for c in (-HALF_M, HALF_M)]


# ---- small helpers ---------------------------------------------------------------------------
def digests(path: Path) -> dict:
    data = path.read_bytes()
    return {"bytes": len(data), "md5": hashlib.md5(data).hexdigest(), "sha256": hashlib.sha256(data).hexdigest()}


def clean(obj):
    if isinstance(obj, dict):
        return {str(k): clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return clean(obj.tolist())
    if isinstance(obj, (bool, np.bool_)):
        return bool(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        value = float(obj)
        return None if math.isnan(value) else round(value, 6)
    return obj


def share(mask) -> float:
    mask = np.asarray(mask, bool)
    return float(mask.mean()) if mask.size else float("nan")


def band_of(level: float) -> str:
    for name, lo, hi in BANDS:
        if lo <= level <= hi:
            return name
    return "below-20"


def strata(frame: pd.DataFrame, flag: str, bands=None) -> dict:
    out = {"overall": {"n": int(len(frame)), "share": share(frame[flag])}, "by_orderers": {}, "by_band": {}}
    for o, g in frame.groupby("O"):
        out["by_orderers"][int(o)] = {"n": int(len(g)), "share": share(g[flag])}
    for b, g in frame.groupby("band"):
        if bands is None or b in bands:
            out["by_band"][b] = {"n": int(len(g)), "share": share(g[flag])}
    return out


def min_stratum(st: dict, bands=None) -> float:
    values = [v["share"] for v in st["by_orderers"].values()]
    values += [v["share"] for k, v in st["by_band"].items() if bands is None or k in bands]
    return float(min(values)) if values else float("nan")


# ---- closed-form quantities ------------------------------------------------------------------
def drain(n, tput, act):
    return n / tput - n / act


def spacing(n, act):
    return n / ((n - 1.0) * act)


def rhat(n, tput, act, mx):
    return 1.0 + (mx - drain(n, tput, act)) / spacing(n, act)


def bounds(fn, n, tput, act, mx):
    values = np.stack([fn(n, tput + a, act + b, mx + c) for a, b, c in CORNERS])
    return values.min(axis=0), values.max(axis=0)


def drain_fn(n, tput, act, mx):
    return drain(n, tput, act)


def tol_t(t_pred, n):
    return np.maximum(1.0, np.asarray(t_pred) ** 2 * L_TOL_S / n)


def cycle_struct(n, c, level):
    """m from the scheduled spacing 1/S (v0.2); ties at c*S integer are inclusive."""
    m = np.floor(np.round(np.asarray(c) * np.asarray(level), 9)) + 1
    k = np.ceil(n / m)
    q = n - (k - 1) * m
    return m, k, q


def t_cycle(n, c, level, s_obs, lat):
    m, k, _ = cycle_struct(n, c, level)
    return n / ((k - 1) * m * s_obs + c + lat)


def union_length(intervals, lo, hi):
    pieces = sorted((max(a, lo), min(b, hi)) for a, b in intervals if min(b, hi) > max(a, lo))
    total, cur_a, cur_b = 0.0, None, None
    for a, b in pieces:
        if cur_b is None or a > cur_b:
            if cur_b is not None:
                total += cur_b - cur_a
            cur_a, cur_b = a, b
        else:
            cur_b = max(cur_b, b)
    if cur_b is not None:
        total += cur_b - cur_a
    return total


# ---- self-check of the algebra on constructed values (not data) -------------------------------
def algebra_self_check() -> dict:
    n, level, c, lat = 1000, 100.0, 2.0, 0.1
    s = 1.0 / level
    act = n / ((n - 1) * s)
    out = {}
    for block in (45, 40):
        r = n % block
        d = lat if r == 0 else c + lat - (r - 1) * s
        tput = n / ((n - 1) * s + d)
        mx = (block - 1) * s + lat if r == 0 else c + lat
        out[f"count_B{block}_rhat"] = float(rhat(n, tput, act, mx))
        assert abs(out[f"count_B{block}_rhat"] - (r if r else block)) < 1e-6
    level, c = 50.0, 2.0105
    s = 1.0 / level
    act = n / ((n - 1) * s)
    m, k, q = cycle_struct(n, c, level)
    assert (m, k, q) == (101, 10, 91)
    tput = float(t_cycle(n, c, level, s, lat))
    mx = c + lat
    out["cycle_S50_rhat"] = float(rhat(n, tput, act, mx))
    assert abs(out["cycle_S50_rhat"] - 91) < 1e-6
    return out


# ---- loading and integrity -------------------------------------------------------------------
def load():
    dig = digests(HFBTP_CSV)
    if dig["md5"] != HFBTP_MD5 or dig["sha256"] != HFBTP_SHA256:
        raise SystemExit(f"HFBTP checksum mismatch: {dig}")
    text = pd.read_csv(HFBTP_CSV, dtype=str, keep_default_na=False)
    frame = pd.read_csv(HFBTP_CSV)
    return frame, text, dig


def decimals(series) -> dict:
    parts = series.astype(str).str.strip().str.split(".", n=1)
    return parts.str[1].fillna("").str.len().value_counts().sort_index().to_dict()


def integrity(frame, text, dig) -> tuple[pd.DataFrame, dict]:
    header = list(frame.columns)
    stripped = [c.strip() for c in header]
    f = frame.copy()
    f.columns = stripped
    t = text.copy()
    t.columns = stripped
    keys = [SET, BLK, ORD]
    levels, blocks, ords = (sorted(f[c].unique()) for c in keys)
    full = pd.MultiIndex.from_product([levels, blocks, ords], names=keys)
    missing = full.difference(pd.MultiIndex.from_frame(f[keys])).to_frame(index=False)
    ratio = f[ACT] / f[SET]
    out = {
        "checksums": dig,
        "header_exact": header,
        "header_has_padding": header != stripped,
        "header_matches_expected": stripped == COLUMNS,
        "shape": list(f.shape),
        "dtypes": {c: str(v) for c, v in f.dtypes.items()},
        "nulls": int(f.isna().sum().sum()),
        "empty_strings": int((t == "").sum().sum()),
        "duplicate_rows": int(f.duplicated().sum()),
        "duplicate_keys": int(f.duplicated(keys).sum()),
        "grid": {
            "levels": len(levels), "level_range": [float(levels[0]), float(levels[-1])],
            "level_steps": sorted({float(x) for x in np.diff(levels)}),
            "blocks": len(blocks), "block_range": [int(blocks[0]), int(blocks[-1])],
            "block_steps": sorted({int(x) for x in np.diff(blocks)}),
            "orderers": [int(o) for o in ords], "full_grid": len(levels) * len(blocks) * len(ords),
        },
        "missing_cells": {
            "count": int(len(missing)),
            "per_level_count_values": {int(k): int(v) for k, v in missing.groupby(SET).size().value_counts().items()},
            "slots_block_orderers": sorted({(int(b), int(o)) for b, o in zip(missing[BLK], missing[ORD])}),
        },
        "decimals": {c: {int(k): int(v) for k, v in decimals(t[c]).items()} for c in COLUMNS},
        "ranges": {c: [float(f[c].min()), float(f[c].max())] for c in COLUMNS},
        "pacing": {
            "median_A_over_S": float(ratio.median()), "min": float(ratio.min()), "max": float(ratio.max()),
            "rows_below_0_98": int((ratio < PACE_LO).sum()), "rows_above_1_02": int((ratio > PACE_HI).sum()),
            "levels_of_rows_below_0_98": sorted({float(x) for x in f.loc[ratio < PACE_LO, SET]}),
        },
        "rows_T_above_A_plus_0_05": int((f[TPUT] > f[ACT] + 0.05).sum()),
        "share_max_latency_2_0_to_2_3": float(((f[MAX] >= 2.0) & (f[MAX] <= 2.3)).mean()),
        "rows_max_latency_above_3": int((f[MAX] > 3).sum()),
        "rows_avg_latency_above_2": int((f[AVG] > 2).sum()),
    }
    previous = {  # values reported in the 2026-11-09 measurement-validity audit and the project context
        "shape": [24687, 8], "missing_count": 117, "nulls": 0, "duplicate_rows": 0,
        "rows_T_above_A_plus_0_05": 0, "rows_below_0_98": 25, "median_A_over_S": 1.0014,
        "share_max_latency_2_0_to_2_3": 0.5015, "header_has_padding_per_context_file": True,
        "header_has_padding_per_2026_11_09_audit": False,
    }
    out["comparison_with_previous"] = {
        "previous": previous,
        "matches": {
            "shape": out["shape"] == previous["shape"],
            "missing_count": out["missing_cells"]["count"] == previous["missing_count"],
            "nulls": out["nulls"] == 0, "duplicate_rows": out["duplicate_rows"] == 0,
            "T_above_A": out["rows_T_above_A_plus_0_05"] == 0,
            "rows_below_0_98": out["pacing"]["rows_below_0_98"] == 25,
            "median_A_over_S_4dp": round(out["pacing"]["median_A_over_S"], 4) == 1.0014,
            "max_shelf_4dp": round(out["share_max_latency_2_0_to_2_3"], 4) == 0.5015,
            "header_padding_audit_2026_11_09": out["header_has_padding"] is False,
        },
    }
    return f, out


def working(f: pd.DataFrame) -> pd.DataFrame:
    w = pd.DataFrame({
        "S": f[SET].astype(float), "A": f[ACT].astype(float), "B": f[BLK].astype(int), "O": f[ORD].astype(int),
        "TP": f[TPUT].astype(float), "LAVG": f[AVG].astype(float), "LMIN": f[MIN].astype(float),
        "LMAX": f[MAX].astype(float),
    })
    w["pace"] = w["A"] / w["S"]
    w["pace_ok"] = (w["pace"] >= PACE_LO) & (w["pace"] <= PACE_HI)
    w["band"] = w["S"].map(band_of)
    w["fill"] = (w["B"] - 1) / w["S"]
    return w


def count_mask(w):
    return w["pace_ok"] & (w["S"] >= 50) & (w["B"] <= 115) & (w["fill"] <= 1.0)


def p4_mask(w):
    return w["pace_ok"] & (w["S"] >= 20) & (w["S"] <= 55) & (w["B"] >= np.floor(2.035 * w["S"]) + 6)


def p5_mask(w):
    return w["pace_ok"] & (w["S"] >= 20) & ((w["B"] >= 410) | (w["fill"] >= 2.5))


def p6_mask(w):
    return w["pace_ok"] & (w["S"] >= 50) & (w["B"] >= 120) & (w["B"] <= 405) & (w["fill"] <= 1.5)


# ---- count regime (P1-P3) --------------------------------------------------------------------
def count_frame(w, n=N0, allow=ALLOW) -> pd.DataFrame:
    d = w[count_mask(w)].copy()
    tp, a, mx, b = (d[c].to_numpy(float) for c in ("TP", "A", "LMAX", "B"))
    d["D"] = drain(n, tp, a)
    d["D_lo"], d["D_hi"] = bounds(drain_fn, n, tp, a, mx)
    d["s"] = spacing(n, a)
    d["rhat"] = rhat(n, tp, a, mx)
    lo, hi = bounds(rhat, n, tp, a, mx)
    d["rhat_lo"], d["rhat_hi"] = lo, hi
    r = np.mod(n, d["B"].to_numpy(int))
    d["r"] = r
    pos = r > 0
    d["agree"] = np.where(pos, (r >= lo - allow) & (r <= hi + allow),
                          (b >= lo - allow - JITTER_S / d["s"].to_numpy()) & (b <= hi + allow))
    a_lo = np.maximum(1, np.ceil(lo - allow))
    a_hi = np.minimum(b - 1, np.floor(hi + allow))
    d["chance"] = np.where(pos, np.clip(a_hi - a_lo + 1, 0, None) / np.maximum(b - 1, 1), np.nan)
    d["K"] = np.where(pos, d["D"] + (r - 1) * d["s"], np.nan)
    return d


def slope(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    vx = np.var(x)
    if x.size < 3 or vx == 0:
        return float("nan"), float("nan")
    b = float(np.cov(x, y, bias=True)[0, 1] / vx)
    return b, float(y.mean() - b * x.mean())


def p3_summary(d: pd.DataFrame) -> dict:
    pos = d[d["r"] > 0]
    u = (pos["r"] - 1) * pos["s"]
    b, a = slope(u, pos["D"])
    out = {"n_rows": int(len(pos)), "slope_D_on_u": b, "intercept": a,
           "K_median": float(pos["K"].median()), "K_iqr": float(pos["K"].quantile(0.75) - pos["K"].quantile(0.25)),
           "K_p05_p95": [float(pos["K"].quantile(0.05)), float(pos["K"].quantile(0.95))],
           "max_minus_K_median": float((pos["LMAX"] - pos["K"]).median()),
           "by_orderers": {}, "by_band": {}}
    for key, col in (("by_orderers", "O"), ("by_band", "band")):
        for v, g in pos.groupby(col):
            bb, aa = slope((g["r"] - 1) * g["s"], g["D"])
            out[key][str(v)] = {"n": int(len(g)), "slope": bb, "intercept": aa, "K_median": float(g["K"].median()),
                                "K_iqr": float(g["K"].quantile(0.75) - g["K"].quantile(0.25))}
    return out


def level_params(w, n=N0):
    d = count_frame(w, n)
    lat = d[d["r"] == 0].groupby("S")["D"].median()
    kk = d[d["r"] > 0].groupby("S")["K"].median()
    pooled_low = float(d[(d["r"] == 0) & (d["S"] <= 60)]["D"].median())
    c_hat = float(d[d["r"] > 0]["K"].median() - d[d["r"] == 0]["D"].median())
    return lat, kk, pooled_low, c_hat, d


def lat_for(levels, lat_series, pooled_low):
    mapped = pd.Series(levels).map(lat_series)
    return np.where(np.asarray(levels) < 50, pooled_low, mapped.to_numpy(float))


# ---- cycle regime (P4, P5) -------------------------------------------------------------------
def classes_for_row(n, level, s_obs, lat, c_grid):
    m, k, q = cycle_struct(n, c_grid, level)
    tput = n / ((k - 1) * m * s_obs + c_grid + lat)
    by_k, by_m = [], []
    for kv in np.unique(k):
        idx = np.flatnonzero(k == kv)
        mid = idx[len(idx) // 2]
        by_k.append({"k": int(kv), "c_lo": float(c_grid[idx[0]]), "c_hi": float(c_grid[idx[-1]]),
                     "m": int(m[mid]), "q": int(q[mid]), "T": float(tput[mid])})
    for mv in np.unique(m):
        idx = np.flatnonzero(m == mv)
        by_m.append({"m": int(mv), "k": int(k[idx[0]]), "q": int(q[idx[0]]),
                     "c_lo": float(c_grid[idx[0]]), "c_hi": float(c_grid[idx[-1]])})
    return by_k, by_m


def p5_frame(w, lat_series, pooled_low, n=N0, c_grid=C_GRID, tol_scale=1.0, mask=None) -> pd.DataFrame:
    d = w[p5_mask(w) if mask is None else mask].copy()
    d["s"] = spacing(n, d["A"].to_numpy(float))
    d["L"] = lat_for(d["S"].to_numpy(float), lat_series, pooled_low)
    member, nearest_k, nearest_t, n_classes, chance, between = [], [], [], [], [], []
    for level, s_obs, lat, tp, a in zip(d["S"], d["s"], d["L"], d["TP"], d["A"]):
        by_k, _ = classes_for_row(n, level, s_obs, lat, c_grid)
        t_vals = np.array([c["T"] for c in by_k])
        tols = tol_t(t_vals, n) * tol_scale
        dist = np.abs(tp - t_vals)
        j = int(np.argmin(dist))
        member.append(bool(np.any(dist <= tols)))
        nearest_k.append(by_k[j]["k"])
        nearest_t.append(t_vals[j])
        n_classes.append(len(by_k))
        chance.append(min(1.0, union_length([(t - e, t + e) for t, e in zip(t_vals, tols)], 0.5 * a, a) / (0.5 * a)))
        between.append(bool(len(by_k) > 1 and t_vals.min() < tp < t_vals.max() and not np.any(dist <= tols)))
    d["member"] = member
    d["k_near"] = nearest_k
    d["T_near"] = nearest_t
    d["rel_err"] = (d["TP"] - d["T_near"]) / d["T_near"]
    d["n_classes"] = n_classes
    d["chance"] = chance
    d["intermediate"] = between
    d["no_tail_null"] = np.abs(d["TP"] - d["A"]) <= tol_t(d["A"].to_numpy(), n) * tol_scale
    return d


def strict_cycle_agreement(d, n, c, tol_scale=1.0):
    t_pred = t_cycle(n, c, d["S"].to_numpy(float), d["s"].to_numpy(float), d["L"].to_numpy(float))
    return np.abs(d["TP"].to_numpy() - t_pred) <= tol_t(t_pred, n) * tol_scale


def p4_frame(w, n=N0, c_grid=C_GRID, allow=ALLOW) -> pd.DataFrame:
    d = w[p4_mask(w)].copy()
    tp, a, mx = (d[c].to_numpy(float) for c in ("TP", "A", "LMAX"))
    d["s"] = spacing(n, a)
    d["rhat"] = rhat(n, tp, a, mx)
    lo, hi = bounds(rhat, n, tp, a, mx)
    d["rhat_lo"], d["rhat_hi"] = lo, hi
    member, matched_q, matched_m, n_cand, chance, strict_in, strict_out = [], [], [], [], [], [], []
    for level, s_obs, l, h, rh in zip(d["S"], d["s"], lo, hi, d["rhat"]):
        _, by_m = classes_for_row(n, level, s_obs, 0.0, c_grid)
        lower, upper = l - allow - JITTER_S / s_obs, h + allow
        ok = [c for c in by_m if lower <= c["q"] <= upper]
        member.append(bool(ok))
        best = min(ok or by_m, key=lambda c: abs(c["q"] - rh))
        matched_q.append(best["q"])
        matched_m.append(best["m"])
        n_cand.append(len(by_m))
        m_max = max(c["m"] for c in by_m)
        width = max(0.0, min(m_max, math.floor(upper)) - max(1, math.ceil(lower)) + 1)
        p = min(1.0, width / m_max)
        chance.append(1 - (1 - p) ** len(by_m))
        for c_val, store in ((C_TIE_IN, strict_in), (C_TIE_OUT, strict_out)):
            _, _, q = cycle_struct(n, c_val, level)
            store.append(bool(lower <= float(q) <= upper))
    d["member"], d["q_match"], d["m_match"], d["n_candidates"] = member, matched_q, matched_m, n_cand
    d["chance"], d["strict_2_001"], d["strict_1_999"] = chance, strict_in, strict_out
    return d


# ---- main analysis ---------------------------------------------------------------------------
def analyse(w: pd.DataFrame, out_dir: Path) -> dict:
    res = {}
    lat, kk, pooled_low, c_hat, cnt = level_params(w)
    res["level_parameters"] = {
        "L_hat_by_level": {float(k): float(v) for k, v in lat.items()},
        "K_hat_by_level": {float(k): float(v) for k, v in kk.items()},
        "L_hat_pooled_S50_60_for_S_below_50": pooled_low, "c_hat": c_hat,
        "note": "L_hat = median D of count-eligible r=0 rows; K_hat = median of D+(r-1)s over r>0 rows; n=1000",
    }
    pd.DataFrame({"L_hat": lat, "K_hat": kk}).rename_axis("S").to_csv(out_dir / "level_parameters.csv")

    # ---- P1 ----
    pos = cnt[cnt["r"] > 0]
    zero = cnt[cnt["r"] == 0]
    res["P1"] = {
        "eligible_rows": int(len(cnt)), "r_pos_rows": int(len(pos)), "r_zero_rows": int(len(zero)),
        "r_pos": strata(pos, "agree", COUNT_BANDS), "r_zero": strata(zero, "agree", COUNT_BANDS),
        "combined_share": share(cnt["agree"]),
        "median_signed_error_r_pos": float((pos["rhat"] - pos["r"]).median()),
        "median_abs_error_r_pos": float((pos["rhat"] - pos["r"]).abs().median()),
        "p90_abs_error_r_pos": float((pos["rhat"] - pos["r"]).abs().quantile(0.9)),
        "mean_chance_r_pos": float(pos["chance"].mean()),
        "median_rhat_minus_B_r_zero": float((zero["rhat"] - zero["B"]).median()),
        "allowance_1_share_r_pos": share(count_frame(w, N0, 1.0).query("r > 0")["agree"]),
        "disagreements_by_block": pos[~pos["agree"]].groupby("B").size().to_dict(),
        "disagreements_by_level": pos[~pos["agree"]].groupby("S").size().to_dict(),
        "disagreements_by_orderers": pos[~pos["agree"]].groupby("O").size().to_dict(),
        "disagreement_examples": pos[~pos["agree"]].assign(err=lambda x: x["rhat"] - x["r"])
        .reindex(columns=["S", "B", "O", "r", "rhat", "rhat_lo", "rhat_hi", "LMAX", "LAVG", "D"])
        .head(40).round(3).to_dict("records"),
    }
    p1_n = {}
    for n in N_SENS:
        c = count_frame(w, n)
        p1_n[n] = {"r_pos_share": share(c[c["r"] > 0]["agree"]), "combined_share": share(c["agree"]),
                   "r_pos_rows": int((c["r"] > 0).sum())}
    res["P1"]["n_sensitivity"] = p1_n

    # ---- P1 profile and N-bound ----
    tp, a, mx, b = (cnt[c].to_numpy(float) for c in ("TP", "A", "LMAX", "B"))
    bi = cnt["B"].to_numpy(int)
    profile = []
    for n in N_PROFILE:
        lo, hi = bounds(rhat, int(n), tp, a, mx)
        s = spacing(n, a)
        r = np.mod(int(n), bi)
        agree = np.where(r > 0, (r >= lo - ALLOW) & (r <= hi + ALLOW), (b >= lo - ALLOW - JITTER_S / s) & (b <= hi + ALLOW))
        rh = rhat(n, tp, a, mx)
        signed = float(np.median((rh - r)[r > 0])) if np.any(r > 0) else float("nan")
        profile.append((int(n), float(agree.mean()), float(agree[r > 0].mean()) if np.any(r > 0) else float("nan"), signed))
    prof = pd.DataFrame(profile, columns=["n", "combined_share", "r_pos_share", "median_signed_error"])
    all_tp, all_a, all_mx = (w[c].to_numpy(float) for c in ("TP", "A", "LMAX"))
    coef = 1 / (all_tp + HALF_T) - 1 / (all_a - HALF_A)
    thresh = np.where(coef > 0, (all_mx + HALF_M) / np.where(coef > 0, coef, 1), np.inf)
    prof["nbound_violation_share"] = [float(np.mean(n > thresh)) for n in prof["n"]]
    prof.to_csv(out_dir / "n_profile.csv", index=False)
    best = prof.loc[prof["combined_share"].idxmax()]
    at1000 = prof.set_index("n").loc[1000]
    outside = prof[(prof["n"] < 990) | (prof["n"] > 1010)]
    top_outside = outside.sort_values("combined_share", ascending=False).head(10)
    res["n_profile"] = {
        "best_n": int(best["n"]), "best_share": float(best["combined_share"]),
        "share_at_1000": float(at1000["combined_share"]), "r_pos_share_at_1000": float(at1000["r_pos_share"]),
        "best_outside_990_1010": top_outside[["n", "combined_share"]].to_dict("records"),
        "top20": prof.sort_values("combined_share", ascending=False).head(20)[["n", "combined_share", "r_pos_share"]].to_dict("records"),
        "fine_990_1010": prof[(prof["n"] >= 990) & (prof["n"] <= 1010)].to_dict("records"),
        "nbound_at_1000": float(at1000["nbound_violation_share"]),
        "nbound_first_n_above_1pct": int(prof.loc[prof["nbound_violation_share"] > 0.01, "n"].min()),
        "nbound_first_n_above_5pct": int(prof.loc[prof["nbound_violation_share"] > 0.05, "n"].min()),
        "nbound_at": {int(n): float(prof.set_index("n").loc[n, "nbound_violation_share"]) for n in (900, 999, 1000, 1001, 1010, 1050, 1100, 2000)},
    }

    # ---- P2 ----
    p2 = {"median_D_r_pos": float(pos["D"].median()), "median_D_r_zero": float(zero["D"].median()),
          "contrast_D": float(pos["D"].median() - zero["D"].median()),
          "median_T_over_A_r_pos": float((pos["TP"] / pos["A"]).median()),
          "median_T_over_A_r_zero": float((zero["TP"] / zero["A"]).median()),
          "median_D_width_r_pos": float((pos["D_hi"] - pos["D_lo"]).median()), "by_orderers": {}, "by_band": {}}
    for key, col in (("by_orderers", "O"), ("by_band", "band")):
        for v, g in cnt.groupby(col):
            gp, gz = g[g["r"] > 0], g[g["r"] == 0]
            p2[key][str(v)] = {"n_pos": int(len(gp)), "n_zero": int(len(gz)),
                               "contrast_D": float(gp["D"].median() - gz["D"].median()),
                               "T_over_A_pos": float((gp["TP"] / gp["A"]).median()),
                               "T_over_A_zero": float((gz["TP"] / gz["A"]).median())}
    res["P2"] = p2

    # ---- P3 ----
    res["P3"] = p3_summary(cnt)
    best_n = int(best["n"])
    res["P3"]["slope_at_profile_best_n"] = p3_summary(count_frame(w, best_n))["slope_D_on_u"] if best_n != N0 else res["P3"]["slope_D_on_u"]

    # ---- P4 ----
    p4 = p4_frame(w)
    res["P4"] = {"rows": int(len(p4)), "membership": strata(p4, "member", ["20-45", "50-95"]),
                 "mean_chance": float(p4["chance"].mean()), "strict_c_2_001_share": share(p4["strict_2_001"]),
                 "strict_c_1_999_share": share(p4["strict_1_999"]),
                 "narrow_range_share": share(p4_frame(w, c_grid=C_NARROW)["member"]),
                 "allowance_1_share": share(p4_frame(w, allow=1.0)["member"]),
                 "by_level": {}}
    for level, g in p4.groupby("S"):
        classes = g[g["member"]].groupby("m_match").size()
        _, by_m = classes_for_row(N0, level, 1 / level, 0.0, C_GRID)
        res["P4"]["by_level"][float(level)] = {
            "rows": int(len(g)), "member_share": share(g["member"]), "strict_2_001": share(g["strict_2_001"]),
            "strict_1_999": share(g["strict_1_999"]),
            "candidate_classes": by_m, "matched_class_counts_by_m": {int(k): int(v) for k, v in classes.items()},
            "median_rhat": float(g["rhat"].median()), "mean_chance": float(g["chance"].mean())}
    p4_n = {}
    for n in N_SENS:
        p4_n[n] = share(p4_frame(w, n=n)["member"])
    res["P4"]["n_sensitivity"] = p4_n

    # ---- P5 ----
    p5 = p5_frame(w, lat, pooled_low)
    res["P5"] = {"rows": int(len(p5)), "rows_B_ge_410": int((p5["B"] >= 410).sum()),
                 "rows_low_load_small_block": int((p5["B"] < 410).sum()),
                 "membership": strata(p5, "member"),
                 "median_rel_err": float(p5["rel_err"].median()),
                 "median_abs_rel_err": float(p5["rel_err"].abs().median()),
                 "mean_chance": float(p5["chance"].mean()),
                 "no_tail_null_share": share(p5["no_tail_null"]),
                 "multi_class_rows": int((p5["n_classes"] > 1).sum()),
                 "single_class_member_share": share(p5[p5["n_classes"] == 1]["member"]),
                 "multi_class_member_share": share(p5[p5["n_classes"] > 1]["member"]),
                 "strict_c_2_001_share": share(strict_cycle_agreement(p5, N0, C_TIE_IN)),
                 "strict_c_1_999_share": share(strict_cycle_agreement(p5, N0, C_TIE_OUT)),
                 "strict_c_hat_share": share(strict_cycle_agreement(p5, N0, c_hat)),
                 "tol_x2_member_share": share(p5_frame(w, lat, pooled_low, tol_scale=2.0)["member"]),
                 "narrow_range_member_share": share(p5_frame(w, lat, pooled_low, c_grid=C_NARROW)["member"]),
                 "rel_err_by_orderers": p5.groupby("O")["rel_err"].median().to_dict(),
                 "rel_err_by_band": p5.groupby("band")["rel_err"].median().to_dict()}
    res["P5"]["c_explore_strict_share"] = {float(c): share(strict_cycle_agreement(p5, N0, c)) for c in C_EXPLORE}
    p5_n = {}
    for n in N_SENS:
        lat_n, _, pooled_n, _, _ = level_params(w, n)
        p5_n[n] = share(p5_frame(w, lat_n, pooled_n, n=n)["member"])
    res["P5"]["n_sensitivity"] = p5_n

    # level class structure and c-interval intersection (EXPLORATORY identification)
    levels_out, intervals = {}, []
    for level, g in p5.groupby("S"):
        by_k, _ = classes_for_row(N0, level, 1 / level, float(g["L"].iloc[0]), C_GRID)
        counts = g[g["member"]].groupby("k_near").size()
        dominant = int(counts.idxmax()) if len(counts) else None
        dom_share = float(counts.max() / len(g)) if len(counts) else 0.0
        entry = {"rows": int(len(g)), "member_share": share(g["member"]), "classes": by_k,
                 "member_counts_by_k": {int(k): int(v) for k, v in counts.items()},
                 "dominant_k": dominant, "dominant_share_of_rows": dom_share,
                 "intermediate_share": share(g["intermediate"]),
                 "median_T": float(g["TP"].median()), "median_rel_err": float(g["rel_err"].median()),
                 "median_avg_latency": float(g["LAVG"].median()), "median_max_latency": float(g["LMAX"].median())}
        if dominant is not None:
            for c in by_k:
                if c["k"] == dominant:
                    entry["dominant_c_interval"] = [c["c_lo"], c["c_hi"]]
                    if len(by_k) > 1:
                        intervals.append((float(level), c["c_lo"], c["c_hi"]))
        if len(by_k) > 1:
            entry["by_class_max_latency_median"] = g[g["member"]].groupby("k_near")["LMAX"].median().to_dict()
            entry["by_class_avg_latency_median"] = g[g["member"]].groupby("k_near")["LAVG"].median().to_dict()
            entry["by_class_orderer_counts"] = {int(k): gg.groupby("O").size().to_dict() for k, gg in g[g["member"]].groupby("k_near")}
            entry["by_class_mean_A"] = g[g["member"]].groupby("k_near")["A"].mean().to_dict()
        levels_out[float(level)] = entry
    res["P5"]["by_level"] = levels_out
    lo_c = max([i[1] for i in intervals], default=float("nan"))
    hi_c = min([i[2] for i in intervals], default=float("nan"))
    res["P5"]["dominant_interval_intersection_multiclass_levels"] = {
        "levels": intervals, "intersection": [lo_c, hi_c], "non_empty": bool(intervals) and lo_c <= hi_c}
    pd.DataFrame([{"S": k, **{kk_: v for kk_, v in e.items() if not isinstance(v, (list, dict))}} for k, e in levels_out.items()]) \
        .to_csv(out_dir / "p5_by_level.csv", index=False)

    # ---- P6 ----
    p6 = w[p6_mask(w)].copy()
    p6["s"] = spacing(N0, p6["A"].to_numpy(float))
    p6["r"] = np.mod(N0, p6["B"].to_numpy(int))
    p6["L"] = lat_for(p6["S"].to_numpy(float), lat, pooled_low)
    p6["Khat"] = p6["S"].map(kk).to_numpy(float)
    d_cnt = np.where(p6["r"] == 0, p6["L"], p6["Khat"] - (p6["r"] - 1) * p6["s"])
    p6["T_cnt"] = N0 / (N0 / p6["A"] + d_cnt)
    p6["count_like"] = np.abs(p6["TP"] - p6["T_cnt"]) <= tol_t(p6["T_cnt"].to_numpy(), N0)
    cyc = p5_frame(w, lat, pooled_low, mask=p6_mask(w))
    p6["cycle_like"] = cyc["member"].to_numpy()
    p6["label"] = np.select([p6["count_like"] & ~p6["cycle_like"], p6["cycle_like"] & ~p6["count_like"],
                             p6["count_like"] & p6["cycle_like"]], ["count", "cycle", "both"], "neither")

    def switch(g):
        rows = []
        for blk, gb in g.groupby("B"):
            dec = gb[gb["label"].isin(["count", "cycle"])]
            rows.append({"B": int(blk), "rows": int(len(gb)), "decidable": int(len(dec)),
                         "count_share": share(dec["label"] == "count"), "cycle_share": share(dec["label"] == "cycle")})
        tab = pd.DataFrame(rows)
        b_count = None
        for _, row in tab.sort_values("B").iterrows():
            if row["decidable"] < 3:
                continue
            if row["count_share"] >= 0.8:
                b_count = int(row["B"])
            else:
                break
        b_cycle = None
        for _, row in tab.sort_values("B", ascending=False).iterrows():
            if row["decidable"] < 3:
                continue
            if row["cycle_share"] >= 0.8:
                b_cycle = int(row["B"])
            else:
                break
        return tab, b_count, b_cycle

    pooled_tab, pb_count, pb_cycle = switch(p6)
    pooled_tab.to_csv(out_dir / "p6_switch_pooled.csv", index=False)
    p6_res = {"rows": int(len(p6)), "label_counts": p6["label"].value_counts().to_dict(),
              "pooled": {"last_count_B": pb_count, "first_cycle_B": pb_cycle,
                         "table_B_120_to_200": pooled_tab[pooled_tab["B"] <= 200].round(3).to_dict("records")},
              "by_orderers": {}, "by_band": {}}
    for key, col in (("by_orderers", "O"), ("by_band", "band")):
        for v, g in p6.groupby(col):
            tab, bc, by = switch(g)
            p6_res[key][str(v)] = {"rows": int(len(g)), "last_count_B": bc, "first_cycle_B": by,
                                   "decidable_rows": int(tab["decidable"].sum())}
    res["P6"] = p6_res

    # ---- P7 ----
    big = w[w["pace_ok"] & (w["B"] >= 410)]
    by_level = big.groupby("S").agg(rows=("TP", "size"), median_T=("TP", "median"), median_avg=("LAVG", "median"),
                                    median_max=("LMAX", "median"), median_min=("LMIN", "median"))
    by_level.to_csv(out_dir / "p7_large_block_latency_by_level.csv")
    heavy = big[big["S"] >= 170]
    ref = big[big["S"] == 150]
    res["P7"] = {"delta_avg": float(heavy["LAVG"].median() - ref["LAVG"].median()),
                 "delta_max": float(heavy["LMAX"].median() - ref["LMAX"].median()),
                 "avg_150": float(ref["LAVG"].median()), "avg_170_200": float(heavy["LAVG"].median()),
                 "max_150": float(ref["LMAX"].median()), "max_170_200": float(heavy["LMAX"].median()),
                 "by_orderers": {int(o): {"delta_avg": float(g[g["S"] >= 170]["LAVG"].median() - g[g["S"] == 150]["LAVG"].median()),
                                          "delta_max": float(g[g["S"] >= 170]["LMAX"].median() - g[g["S"] == 150]["LMAX"].median())}
                                 for o, g in big.groupby("O")},
                 "by_level": by_level.round(3).reset_index().to_dict("records")}
    small_cap = w[w["pace_ok"] & w["B"].isin([40, 50]) & (w["S"] >= 170)]
    res["P7"]["capacity_lower_bound_B40_B50_S_ge_170"] = {
        "rows": int(len(small_cap)), "min_T": float(small_cap["TP"].min()),
        "median_T_over_A": float((small_cap["TP"] / small_cap["A"]).median()),
        "median_avg_latency": float(small_cap["LAVG"].median())}

    # ---- P8: level 165 and averaging ----
    l165 = w[w["S"] == 165]
    c165 = cnt[cnt["S"] == 165]
    b165 = p5[p5["S"] == 165]
    upper = b165["k_near"] == 3
    wide = b165.assign(up=upper.astype(int)).pivot_table(index="B", columns="O", values="up")
    complete = wide.dropna()
    p_up = float(upper.mean()) if len(b165) else float("nan")
    res["P8"] = {
        "level_165_all_rows": int(len(l165)),
        "histogram_all_rows": dict(zip(["<120", "120-130", "130-140", "140-150", "150-160", "160-170", ">=170"],
                                       np.histogram(l165["TP"], bins=[0, 120, 130, 140, 150, 160, 170, 1e9])[0].tolist())),
        "count_rows": {"rows": int(len(c165)), "agree_share": share(c165["agree"]),
                       "upper_share_r_zero": share(c165[c165["r"] == 0]["TP"] >= 140),
                       "upper_share_r_pos": share(c165[c165["r"] > 0]["TP"] >= 140)},
        "cycle_rows": {"rows": int(len(b165)), "member_share": share(b165["member"]), "k3_share": p_up,
                       "k3_share_by_orderers": b165.assign(up=upper).groupby("O")["up"].mean().to_dict(),
                       "median_max_by_class": b165[b165["member"]].groupby("k_near")["LMAX"].median().to_dict(),
                       "median_avg_by_class": b165[b165["member"]].groupby("k_near")["LAVG"].median().to_dict(),
                       "mean_A_by_class": b165.groupby("k_near")["A"].mean().to_dict(),
                       "median_T_by_class": b165.groupby("k_near")["TP"].median().to_dict(),
                       "blocks_all_four_orderers": int(len(complete)),
                       "same_class_across_orderers_share": share(complete.isin([0, 1]).all(axis=1) & complete.nunique(axis=1).eq(1)),
                       "same_class_expected_if_independent": p_up ** 4 + (1 - p_up) ** 4 if not math.isnan(p_up) else None,
                       "k3_share_by_block_band": b165.assign(up=upper, bb=pd.cut(b165["B"], [0, 409, 500, 600, 700, 800]))
                       .groupby("bb", observed=True)["up"].mean().astype(float).to_dict()},
    }
    multi = p5[p5["n_classes"] > 1]
    res["P8"]["averaging"] = {"multi_class_rows": int(len(multi)), "intermediate_rows": int(multi["intermediate"].sum()),
                              "intermediate_share": share(multi["intermediate"]),
                              "non_member_rows": int((~multi["member"]).sum()),
                              "by_level": multi.groupby("S")["intermediate"].mean().to_dict()}
    res["P8"]["level_55_vs_165"] = {
        "k9_share_level_55_cycle_rows": share(p5[p5["S"] == 55]["k_near"] == 9),
        "k3_share_level_165_cycle_rows": p_up,
        "p4_level_55_matched_m_counts": res["P4"]["by_level"].get(55.0, {}).get("matched_class_counts_by_m")}
    res["P8"]["file_order_runs_165_cycle_rows"] = None
    if len(b165):
        ordered = b165.sort_index().assign(up=upper)["up"].astype(int).to_numpy()
        runs = 1 + int((ordered[1:] != ordered[:-1]).sum())
        res["P8"]["file_order_runs_165_cycle_rows"] = {"runs": runs, "expected_if_random": 1 + 2 * len(ordered) * p_up * (1 - p_up)}

    # ---- P9 ----
    c9 = cnt.copy()
    c9["Khat"] = c9["S"].map(kk)
    c9["Lhat"] = c9["S"].map(lat)
    d_pred = np.where(c9["r"] == 0, c9["Lhat"], c9["Khat"] - (c9["r"] - 1) * c9["s"])
    c9["T_cnt"] = N0 / (N0 / c9["A"] + d_pred)
    nxt = c9[["S", "O", "B", "TP", "T_cnt"]].copy()
    nxt["B"] = nxt["B"] - 5
    pairs = c9[["S", "O", "B", "TP", "T_cnt"]].merge(nxt, on=["S", "O", "B"], suffixes=("", "_next"))
    pairs["d_pred"] = pairs["T_cnt_next"] - pairs["T_cnt"]
    pairs["d_obs"] = pairs["TP_next"] - pairs["TP"]
    dec = pairs[pairs["d_pred"].abs() >= 3]
    dec_agree = np.sign(dec["d_obs"]) == np.sign(dec["d_pred"])
    res["P9"] = {"pairs": int(len(pairs)), "decisive_pairs": int(len(dec)), "sign_agreement": share(dec_agree),
                 "share_decisive_predicted_negative": share(dec["d_pred"] < 0),
                 "sign_agreement_predicted_negative": share(dec_agree[dec["d_pred"] < 0]),
                 "by_orderers": {int(o): share(dec_agree[dec["O"] == o]) for o in sorted(dec["O"].unique())},
                 "by_band": {b: share(dec_agree[dec["S"].map(band_of) == b]) for b in COUNT_BANDS},
                 "median_abs_error_of_pair_difference": float((dec["d_obs"] - dec["d_pred"]).abs().median())}
    b4045 = w[w["B"].isin([40, 45]) & (w["S"] >= 100)].pivot_table(index=["S", "O"], columns="B", values="TP")
    b4045["obs_gap_40_minus_45"] = b4045[40] - b4045[45]
    pred = c9[c9["B"].isin([40, 45])].pivot_table(index=["S", "O"], columns="B", values="T_cnt")
    b4045["pred_gap_40_minus_45"] = pred[40] - pred[45]
    b4045.round(2).to_csv(out_dir / "b40_b45_gap.csv")
    res["P9"]["b40_b45"] = {"cells": int(b4045["obs_gap_40_minus_45"].notna().sum()),
                            "obs_gap_min": float(b4045["obs_gap_40_minus_45"].min()),
                            "obs_gap_median": float(b4045["obs_gap_40_minus_45"].median()),
                            "share_obs_gap_positive": share(b4045["obs_gap_40_minus_45"].dropna() > 0),
                            "median_abs_obs_minus_pred": float((b4045["obs_gap_40_minus_45"] - b4045["pred_gap_40_minus_45"]).abs().median())}

    # ---- A/Set modal test ----
    rows = []
    for level, g in w[w["pace_ok"]].groupby("S"):
        entry = {"S": float(level), "rows": int(len(g)), "modal_A": float(g["A"].mode().iloc[0])}
        for nw in (500, 1000, 2000):
            exact = level * nw / (nw - 1)
            frac = (exact * 10) % 1
            entry[f"pred_{nw}"] = round(exact, 1)
            entry[f"ambiguous_{nw}"] = bool(abs(frac - 0.5) < 0.1)
            entry[f"share_{nw}"] = share(np.isclose(g["A"], round(exact, 1)))
        rows.append(entry)
    aset = pd.DataFrame(rows)
    aset.to_csv(out_dir / "aset_modal.csv", index=False)
    pooled = {}
    for nw in (500, 1000, 2000):
        ok = aset[~aset[f"ambiguous_{nw}"]]
        pooled[nw] = {"levels": int(len(ok)), "weighted_share": float((ok[f"share_{nw}"] * ok["rows"]).sum() / ok["rows"].sum())}
    res["A_over_S"] = {"pooled_unambiguous": pooled,
                       "modal_equals_pred_1000_levels": int((np.isclose(aset["modal_A"], aset["pred_1000"])).sum()),
                       "levels": int(len(aset))}

    # ---- regime map and all-row coverage ----
    regime = pd.Series("unclassified", index=w.index)
    regime[~w["pace_ok"]] = "pacing_excluded"
    regime[count_mask(w)] = "count"
    regime[p6_mask(w)] = "cap_switch"
    regime[p5_mask(w)] = "cycle"
    w2 = w.assign(regime=regime)
    agree = pd.Series(np.nan, index=w.index, dtype=object)
    agree.loc[cnt.index] = cnt["agree"].to_numpy()
    agree.loc[p5.index] = p5["member"].to_numpy()
    agree.loc[p6.index] = p6["label"].isin(["count", "cycle", "both"]).to_numpy()
    w2["mechanism_consistent"] = agree
    cover = {}
    for reg, g in w2.groupby("regime"):
        vals = g["mechanism_consistent"].dropna().astype(bool)
        cover[reg] = {"rows": int(len(g)), "tested": int(len(vals)), "consistent_share": share(vals) if len(vals) else None}
    tested = w2["mechanism_consistent"].dropna().astype(bool)
    res["all_rows"] = {"by_regime": cover, "total_rows": int(len(w2)), "tested_rows": int(len(tested)),
                       "consistent_share_of_tested": share(tested), "consistent_rows": int(tested.sum()),
                       "unclassified_by_band": w2[w2["regime"] == "unclassified"].groupby("band").size().to_dict()}
    derived = w2[["S", "B", "O", "regime", "mechanism_consistent"]].copy()
    derived = derived.join(cnt[["D", "rhat", "rhat_lo", "rhat_hi", "r", "agree"]].add_prefix("count_"))
    derived = derived.join(p5[["k_near", "T_near", "member", "rel_err", "n_classes", "intermediate"]].add_prefix("cycle_"))
    derived = derived.join(p6[["T_cnt", "label"]].add_prefix("cap_"))
    derived.to_csv(out_dir / "rows_derived_keys_only.csv", index=False)

    # ---- other descriptive context ----
    stall = w[(w["B"] <= 20) & (w["LAVG"] > 1.0)]
    res["context"] = {
        "small_block_high_avg_latency_rows": {"rows": int(len(stall)), "by_orderers": stall.groupby("O").size().to_dict(),
                                              "by_block": stall.groupby("B").size().to_dict(),
                                              "min_level": float(stall["S"].min()) if len(stall) else None},
        "max_latency_above_3": w[w["LMAX"] > 3].groupby(["B"]).size().to_dict(),
        "pacing_excluded_rows": w[~w["pace_ok"]][["S", "B", "O", "A", "TP", "LAVG", "LMAX"]].round(3).to_dict("records"),
        "pacing_excluded_T_over_A_median": float((w[~w["pace_ok"]]["TP"] / w[~w["pace_ok"]]["A"]).median()),
    }

    # ---- verdicts per pre-registered rules ----
    p1s = res["P1"]["r_pos"]
    h1_support = (p1s["overall"]["share"] >= 0.80 and min_stratum(p1s, COUNT_BANDS) >= 0.70
                  and -1.10 <= res["P3"]["slope_D_on_u"] <= -0.90 and -1 <= res["P1"]["median_signed_error_r_pos"] <= 1)
    h1_falsify = (res["n_profile"]["best_share"] < 0.50
                  or (not -1.30 <= res["P3"]["slope_D_on_u"] <= -0.70 and not -1.30 <= res["P3"]["slope_at_profile_best_n"] <= -0.70)
                  or res["P2"]["contrast_D"] < 0.5)
    s1000 = res["n_profile"]["share_at_1000"]
    best_out = float(outside["combined_share"].max())
    h2_support = (s1000 >= res["n_profile"]["best_share"] - 0.02 and best_out <= s1000 - 0.10 and res["n_profile"]["nbound_at_1000"] <= 0.01)
    h2_falsify = best_out >= s1000 + 0.05 or res["n_profile"]["nbound_at_1000"] > 0.05
    p5m, p4m = res["P5"]["membership"], res["P4"]["membership"]
    h3_support = (p4m["overall"]["share"] >= 0.80 and p5m["overall"]["share"] >= 0.80
                  and min_stratum(p5m) >= 0.70 and min(v["share"] for v in p4m["by_orderers"].values()) >= 0.70
                  and res["P4"]["mean_chance"] <= 0.30 and res["P5"]["mean_chance"] <= 0.30
                  and abs(res["P5"]["median_rel_err"]) <= 0.015)
    h3_falsify = p4m["overall"]["share"] < 0.50 or p5m["overall"]["share"] < 0.50 or abs(res["P5"]["median_rel_err"]) > 0.05
    strata_switch = [v for v in list(res["P6"]["by_orderers"].values()) + list(res["P6"]["by_band"].values()) if v["decidable_rows"] >= 30]
    sw_count = [v["last_count_B"] for v in strata_switch]
    sw_cycle = [v["first_cycle_B"] for v in strata_switch]
    if strata_switch and all(c in (120, 125) for c in sw_count) and all(c in (125, 130) for c in sw_cycle):
        h4 = "CONSISTENT"
    elif any(c is None for c in sw_cycle) or (sw_cycle and max(sw_cycle) - min(sw_cycle) > 10):
        h4 = "INCONSISTENT"
    else:
        h4 = "UNRESOLVED"
    if res["P7"]["delta_avg"] < 0.15 and res["P7"]["delta_max"] < 0.30:
        h5 = "AGAINST_SUSTAINED_BACKLOG"
    elif res["P7"]["delta_avg"] > 0.40:
        h5 = "CONSISTENT_WITH_BACKLOG"
    else:
        h5 = "INCONCLUSIVE"
    h6 = "AGAINST_INDEPENDENT_ROUND_AVERAGING" if res["P8"]["averaging"]["intermediate_share"] < 0.05 else "NOT_EXCLUDED"
    p9 = res["P9"]["sign_agreement"]
    res["verdicts"] = {
        "H1_small_block_final_partial_block": "SUPPORTED" if h1_support else ("FALSIFIED" if h1_falsify else "INCONCLUSIVE"),
        "H2_n_1000": "SUPPORTED" if h2_support else ("FALSIFIED" if h2_falsify else "INCONCLUSIVE"),
        "H3_timer_cycles": "SUPPORTED" if h3_support else ("FALSIFIED" if h3_falsify else "INCONCLUSIVE"),
        "H4_cap_120_129": h4, "H5_backlog": h5, "H6_averaging": h6,
        "P9_sign_test": "SUPPORTS" if p9 >= 0.85 else ("CONTRADICTS" if p9 < 0.60 else "NEITHER"),
        "inputs": {"h1": {"p1": p1s["overall"]["share"], "min_stratum": min_stratum(p1s, COUNT_BANDS),
                          "slope": res["P3"]["slope_D_on_u"], "median_signed": res["P1"]["median_signed_error_r_pos"],
                          "profile_best": res["n_profile"]["best_share"], "contrast_D": res["P2"]["contrast_D"]},
                   "h2": {"share_1000": s1000, "best": res["n_profile"]["best_share"], "best_outside": best_out,
                          "nbound_1000": res["n_profile"]["nbound_at_1000"]},
                   "h3": {"p4": p4m["overall"]["share"], "p5": p5m["overall"]["share"], "p5_min_stratum": min_stratum(p5m),
                          "p4_min_orderer": min(v["share"] for v in p4m["by_orderers"].values()),
                          "p4_chance": res["P4"]["mean_chance"], "p5_chance": res["P5"]["mean_chance"],
                          "p5_median_rel_err": res["P5"]["median_rel_err"]},
                   "h4": {"strata_used": len(strata_switch), "last_count_B": sw_count, "first_cycle_B": sw_cycle}},
    }
    return res


def git_head() -> str:
    try:
        return subprocess.run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    except Exception as exc:  # noqa: BLE001
        return f"unavailable: {exc}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    out_dir = args.out if args.out.is_absolute() else REPO_ROOT / args.out
    out_dir.mkdir(parents=True, exist_ok=False)

    prereg_check = {name: (digests(FORENSICS_DOCS / name)["sha256"] == sha) for name, sha in PREREGS.items()}
    if not all(prereg_check.values()):
        raise SystemExit(f"Pre-registration hash mismatch: {prereg_check}")
    started = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    self_check = algebra_self_check()
    frame, text, dig = load()
    f, integ = integrity(frame, text, dig)
    w = working(f)
    results = analyse(w, out_dir)
    manifest = {
        "started": started, "finished": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "command": [sys.executable] + sys.argv, "python": sys.version, "platform": platform.platform(),
        "numpy": np.__version__, "pandas": pd.__version__, "git_head_base": git_head(),
        "note_git_head": "Records the base commit only; this script and its outputs are uncommitted.",
        "script_sha256": digests(Path(__file__))["sha256"], "preregistrations_verified": prereg_check,
        "input": {"path": str(HFBTP_CSV.relative_to(REPO_ROOT)).replace("\\", "/"), **dig},
        "computation_class": "closed-form arithmetic on observed HFBTP columns; no simulator, Monte Carlo, bootstrap, model or MMBPD",
        "candidate_n_note": "n=1000 is a candidate reconstruction, not a documented HFBTP setting",
        "algebra_self_check_on_constructed_values": self_check,
    }
    (out_dir / "manifest.json").write_text(json.dumps(clean(manifest), indent=2), encoding="utf-8")
    (out_dir / "integrity.json").write_text(json.dumps(clean(integ), indent=2), encoding="utf-8")
    (out_dir / "results.json").write_text(json.dumps(clean(results), indent=2), encoding="utf-8")
    print(json.dumps(clean(results["verdicts"]), indent=2))


if __name__ == "__main__":
    main()
