"""EXPLORATORY follow-up to the pre-registered HFBTP forensic run (run_01).

Nothing here was pre-registered. Every quantity is labelled EXPLORATORY in the report. Fitted values
(per-level effective cycle) are post hoc and must not be read as confirmations.

Computation class: closed-form arithmetic on observed HFBTP columns (owner brief 2026-11-09).
No simulator, Monte Carlo, bootstrap, model or MMBPD. Reads HFBTP read-only; writes only to a new --out directory.

Usage, from the repository root:
    .venv/Scripts/python.exe scripts/forensics/hfbtp_forensics_exploratory.py \
        --out docs/research/forensics/results_2026-11-09/run_02_exploratory
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hfbtp_allrow_forensics as base  # noqa: E402  (same directory; closed-form helpers only)

C_FINE = np.round(np.arange(1.950, 2.200 + 1e-9, 0.001), 3)
CAPS = [125, 126, 127, 128, 129]


def within_level_slopes(cnt: pd.DataFrame) -> dict:
    pos = cnt[cnt["r"] > 0].copy()
    pos["u"] = (pos["r"] - 1) * pos["s"]
    dem = pos.assign(u_d=pos["u"] - pos.groupby("S")["u"].transform("mean"),
                     D_d=pos["D"] - pos.groupby("S")["D"].transform("mean"),
                     K_d=pos["K"] - pos.groupby("S")["K"].transform("mean"),
                     r_d=pos["r"] - pos.groupby("S")["r"].transform("mean"))
    b_fe, _ = base.slope(dem["u_d"], dem["D_d"])
    b_kr, _ = base.slope(dem["r_d"], dem["K_d"])
    per_level = {}
    for level, g in pos.groupby("S"):
        bk, ak = base.slope(g["r"], g["K"])
        bd, _ = base.slope(g["u"], g["D"])
        per_level[float(level)] = {"n": int(len(g)), "K_on_r_ms_per_tx": bk * 1000, "K_at_r0": ak, "D_on_u": bd}
    kr = pd.DataFrame(per_level).T
    return {"slope_D_on_u_level_fixed_effects": b_fe, "slope_K_on_r_level_fixed_effects_ms_per_tx": b_kr * 1000,
            "per_level_K_on_r_ms_per_tx_median": float(kr["K_on_r_ms_per_tx"].median()),
            "per_level_D_on_u_median": float(kr["D_on_u"].median()),
            "per_level_D_on_u_range": [float(kr["D_on_u"].min()), float(kr["D_on_u"].max())],
            "implied_D_on_u_if_K_rises_with_r": "D = c + e + a + b*r - (r-1)s, so dD/du = -1 + b*lambda",
            "per_level": per_level}


def effective_cycle_by_level(p5: pd.DataFrame) -> dict:
    out = {}
    big = p5[p5["B"] >= 410]
    for level, g in big.groupby("S"):
        s, lat, tp = g["s"].to_numpy(), g["L"].to_numpy(), g["TP"].to_numpy()
        shares = []
        for c in C_FINE:
            pred = base.t_cycle(base.N0, c, level, s, lat)
            shares.append(float(np.mean(np.abs(tp - pred) <= base.tol_t(pred, base.N0))))
        shares = np.array(shares)
        best = shares.max()
        good = C_FINE[shares >= 0.9 * best] if best > 0 else np.array([])
        out[float(level)] = {"rows": int(len(g)), "best_share": float(best),
                             "c_at_best": float(C_FINE[int(shares.argmax())]),
                             "c_range_within_90pct_of_best": [float(good.min()), float(good.max())] if good.size else None}
    return out


def max_latency_structure(p5: pd.DataFrame, c_by_level: dict, c_fixed: float) -> dict:
    """Predicted Max latency for B>=410 rows under timer cycles with a byte cap and a surviving timer."""
    big = p5[p5["B"] >= 410].copy()
    results = {}
    for label in ("c_hat_fixed", "c_per_level_fitted"):
        for cap in CAPS:
            preds = []
            for level, s, lat in zip(big["S"], big["s"], big["L"]):
                c = c_fixed if label == "c_hat_fixed" else c_by_level[float(level)]["c_at_best"]
                m, k, q = (float(x) for x in base.cycle_struct(base.N0, c, level))

                def cycle_wait(size):
                    j = int((size - 1) // cap)
                    waits = [c - j * cap * s]
                    if j >= 1:
                        waits.append(cap * s)
                    return max(waits)

                waits = [cycle_wait(q)]
                if k >= 2:
                    waits.append(cycle_wait(m))
                preds.append(max(waits) + lat)
            big[f"pred_{label}_{cap}"] = preds
            pred = big[f"pred_{label}_{cap}"]
            high_obs, high_pred = big["LMAX"] >= 1.9, pred >= 1.9
            results[f"{label}_cap{cap}"] = {
                "rows": int(len(big)), "median_abs_error_s": float((big["LMAX"] - pred).abs().median()),
                "share_within_0_15_s": float(((big["LMAX"] - pred).abs() <= 0.15).mean()),
                "high_low_agreement": float((high_obs == high_pred).mean()),
                "corr_level_medians": float(big.groupby("S")["LMAX"].median().corr(pred.groupby(big["S"]).median()))}
    by_level = big.groupby("S").agg(obs_max=("LMAX", "median"), pred_chat_127=("pred_c_hat_fixed_127", "median"),
                                    pred_fit_127=("pred_c_per_level_fitted_127", "median"))
    return {"summary": results, "by_level_cap127": by_level.round(3).reset_index().to_dict("records")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    out_dir = args.out if args.out.is_absolute() else base.REPO_ROOT / args.out
    out_dir.mkdir(parents=True, exist_ok=False)
    started = dt.datetime.now().astimezone().isoformat(timespec="seconds")

    frame, text, dig = base.load()
    f, _ = base.integrity(frame, text, dig)
    w = base.working(f)
    lat, kk, pooled_low, c_hat, cnt = base.level_params(w)
    p5 = base.p5_frame(w, lat, pooled_low)
    res = {"label": "EXPLORATORY - not pre-registered", "c_hat_from_small_block_rows": c_hat}

    res["E1_residual_time_slope"] = within_level_slopes(cnt)
    c_by_level = effective_cycle_by_level(p5)
    res["E2_effective_cycle_by_level_FITTED"] = c_by_level
    res["E3_max_latency_structure"] = max_latency_structure(p5, c_by_level, c_hat)

    # E4: level-165 and averaging, by throughput histogram (not by model membership)
    hist = {}
    for level in (35, 55, 70, 165):
        g = w[(w["S"] == level) & w["pace_ok"]]
        big = g[g["B"] >= 410]
        hist[level] = {"all_rows_T_quantiles": g["TP"].quantile([0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).round(2).to_dict(),
                       "B_ge_410_T_quantiles": big["TP"].quantile([0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).round(2).to_dict(),
                       "B_ge_410_hist_2tx": np.histogram(big["TP"], bins=np.arange(np.floor(big["TP"].min()), np.ceil(big["TP"].max()) + 2, 2))[0].tolist(),
                       "B_ge_410_hist_edges_start": float(np.floor(big["TP"].min()))}
    l165 = w[w["S"] == 165]
    hist["level_165_rows_T_130_to_150"] = l165[(l165["TP"] > 130) & (l165["TP"] < 150)][["S", "B", "O", "A", "TP", "LAVG", "LMAX"]].to_dict("records")
    res["E4_bimodality_by_histogram"] = hist

    # E5: small-block heavy-load slow rows (count regime), orderer dependence
    sb = w[(w["B"] <= 20) & (w["S"] >= 100) & w["pace_ok"]]
    slow = sb[sb["LAVG"] > 0.5]
    res["E5_small_block_heavy_slow_rows"] = {
        "definition": "B<=20, S>=100, Avg latency > 0.5 s (count regime; normal Avg here is about 0.1-0.2 s)",
        "rows": int(len(slow)), "of": int(len(sb)), "by_orderers": slow.groupby("O").size().to_dict(),
        "by_block": slow.groupby("B").size().to_dict(), "by_level": slow.groupby("S").size().to_dict(),
        "median_avg_latency_by_orderers_B10_S_ge_150": w[(w["B"] == 10) & (w["S"] >= 150)].groupby("O")["LAVG"].median().to_dict()}

    # E6: heavy-load decomposition by regime label from run_01 definitions
    heavy = w[w["S"] >= 165]
    reg = pd.Series("other", index=heavy.index)
    reg[base.count_mask(w).reindex(heavy.index, fill_value=False)] = "count"
    reg[base.p6_mask(w).reindex(heavy.index, fill_value=False)] = "cap_switch"
    reg[base.p5_mask(w).reindex(heavy.index, fill_value=False)] = "cycle"
    res["E6_heavy_rows_by_regime"] = heavy.assign(reg=reg).groupby("reg").agg(rows=("TP", "size"), mean_T=("TP", "mean")).round(2).to_dict("index")

    res["started"] = started
    res["finished"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    res["script_sha256"] = base.digests(Path(__file__))["sha256"]
    res["base_script_sha256"] = base.digests(Path(base.__file__))["sha256"]
    (out_dir / "exploratory_results.json").write_text(json.dumps(base.clean(res), indent=2), encoding="utf-8")
    print("written", out_dir)


if __name__ == "__main__":
    main()
