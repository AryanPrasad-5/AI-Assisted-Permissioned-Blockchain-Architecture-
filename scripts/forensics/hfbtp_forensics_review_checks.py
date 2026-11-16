"""EXPLORATORY checks requested by the independent review of run_01 / run_02 (2026-11-09).

Not pre-registered. Closed-form arithmetic on observed HFBTP columns only (owner brief 2026-11-09):
no simulator, Monte Carlo, bootstrap, model or MMBPD. Writes only to a new --out directory.

Usage, from the repository root:
    .venv/Scripts/python.exe -B scripts/forensics/hfbtp_forensics_review_checks.py \
        --out docs/research/forensics/results_2026-11-09/run_03_review_checks
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hfbtp_allrow_forensics as base  # noqa: E402


def p4_single_c(w, c):
    d = base.p4_frame(w)
    lower = d["rhat_lo"] - base.ALLOW - base.JITTER_S / d["s"]
    upper = d["rhat_hi"] + base.ALLOW
    _, _, q = base.cycle_struct(base.N0, c, d["S"].to_numpy(float))
    ok = (q >= lower.to_numpy()) & (q <= upper.to_numpy())
    d = d.assign(q_c=q, ok=ok)
    return {"c": c, "rows": int(len(d)), "share": float(ok.mean()),
            "by_level": d.groupby("S").agg(q_pred=("q_c", "first"), median_rhat=("rhat", "median"), share=("ok", "mean")).round(3).reset_index().to_dict("records"),
            "by_orderers": d.groupby("O")["ok"].mean().round(3).to_dict()}


def h4_upward(w, lat, kk, pooled_low):
    p6 = w[base.p6_mask(w)].copy()
    p6["s"] = base.spacing(base.N0, p6["A"].to_numpy(float))
    p6["r"] = np.mod(base.N0, p6["B"].to_numpy(int))
    p6["L"] = base.lat_for(p6["S"].to_numpy(float), lat, pooled_low)
    d_cnt = np.where(p6["r"] == 0, p6["L"], p6["S"].map(kk).to_numpy(float) - (p6["r"] - 1) * p6["s"])
    p6["T_cnt"] = base.N0 / (base.N0 / p6["A"] + d_cnt)
    count_like = np.abs(p6["TP"] - p6["T_cnt"]) <= base.tol_t(p6["T_cnt"].to_numpy(), base.N0)
    cycle_like = base.p5_frame(w, lat, pooled_low, mask=base.p6_mask(w))["member"].to_numpy()
    p6["label"] = np.select([count_like & ~cycle_like, cycle_like & ~count_like], ["count", "cycle"], "other")
    out = {}
    strata = [("pooled", p6)] + [(f"orderers_{o}", g) for o, g in p6.groupby("O")] + [(f"band_{b}", g) for b, g in p6.groupby("band")]
    for name, g in strata:
        dec = g[g["label"] != "other"]
        tab = dec.groupby("B").agg(decidable=("label", "size"), count_share=("label", lambda x: (x == "count").mean()),
                                   cycle_share=("label", lambda x: (x == "cycle").mean())).reset_index()
        tab = tab[tab["decidable"] >= 3]
        last_count = None
        for _, row in tab.sort_values("B").iterrows():
            if row["count_share"] >= 0.8:
                last_count = int(row["B"])
            else:
                break
        after = tab[tab["B"] > (last_count or 0)]
        first_cycle = int(after[after["cycle_share"] >= 0.8]["B"].min()) if (after["cycle_share"] >= 0.8).any() else None
        above = tab[tab["B"] >= (first_cycle or 10 ** 6)]
        out[name] = {"decidable_rows": int(dec.shape[0]), "blocks_with_3plus": int(len(tab)), "last_count_B": last_count,
                     "first_cycle_B": first_cycle,
                     "share_of_blocks_at_or_above_first_cycle_with_cycle_ge_0_8": float((above["cycle_share"] >= 0.8).mean()) if len(above) else None,
                     "table_120_to_160": tab[tab["B"] <= 160].round(3).to_dict("records")}
    return out


def p1_fixed_rows(w):
    ref = base.count_frame(w, base.N0)
    keep = ref.index[ref["r"] > 0]
    out = {}
    for n in range(995, 1006):
        c = base.count_frame(w, n).loc[keep]
        out[n] = {"rows": int(len(c)), "share": float(c["agree"].mean()),
                  "median_signed_error": float((c["rhat"] - c["r"]).median()),
                  "share_r_zero_under_n": float((c["r"] == 0).mean())}
    return out


def p1_chance_by_stratum(w):
    c = base.count_frame(w, base.N0)
    pos = c[c["r"] > 0]
    return {"by_orderers": pos.groupby("O").agg(agree=("agree", "mean"), chance=("chance", "mean")).round(3).to_dict("index"),
            "by_band": pos.groupby("band").agg(agree=("agree", "mean"), chance=("chance", "mean")).round(3).to_dict("index"),
            "by_block": pos.groupby("B").agg(rows=("agree", "size"), agree=("agree", "mean"), chance=("chance", "mean")).round(3).to_dict("index")}


def e3_baselines(w):
    big = w[w["pace_ok"] & (w["B"] >= 410)].copy()
    high = big["LMAX"] >= 1.9
    level_major = big.assign(h=high).groupby("S")["h"].transform(lambda x: x.mean() >= 0.5)
    return {"rows": int(len(big)), "global_majority_share": float(max(high.mean(), 1 - high.mean())),
            "level_majority_ceiling": float((level_major == high).mean()),
            "note": "level_majority_ceiling is the best any per-level high/low rule can score; E3 scored 0.92-0.95"}


def h5_avg_only(w):
    big = w[w["pace_ok"] & (w["B"] >= 410) & (w["S"] >= 145)]
    tab = big.groupby("S").agg(T=("TP", "median"), avg=("LAVG", "median"), mn=("LMIN", "median"), mx=("LMAX", "median"))
    return tab.round(3).reset_index().to_dict("records")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    out_dir = args.out if args.out.is_absolute() else base.REPO_ROOT / args.out
    out_dir.mkdir(parents=True, exist_ok=False)
    started = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    frame, text, dig = base.load()
    f, _ = base.integrity(frame, text, dig)
    w = base.working(f)
    lat, kk, pooled_low, c_hat, _ = base.level_params(w)
    res = {"label": "EXPLORATORY - requested by independent review; not pre-registered", "c_hat": c_hat,
           "R1_p4_with_c_hat": p4_single_c(w, c_hat),
           "R1b_p4_with_c_2_06": p4_single_c(w, 2.06),
           "R2_h4_upward_scan": h4_upward(w, lat, kk, pooled_low),
           "R3_p1_fixed_rows_n_995_1005": p1_fixed_rows(w),
           "R4_p1_chance_by_stratum": p1_chance_by_stratum(w),
           "R5_e3_baselines": e3_baselines(w),
           "R6_h5_large_block_latency_S_ge_145": h5_avg_only(w),
           "started": started, "finished": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
           "script_sha256": base.digests(Path(__file__))["sha256"],
           "base_script_sha256": base.digests(Path(base.__file__))["sha256"]}
    (out_dir / "review_checks.json").write_text(json.dumps(base.clean(res), indent=2), encoding="utf-8")
    print("written", out_dir)


if __name__ == "__main__":
    main()
