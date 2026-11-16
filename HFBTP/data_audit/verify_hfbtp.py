"""Research-audit verification of HFBTP facts and model-evaluation claims.

Diagnostic only (owner-approved 2026-10-17): every model below is fitted in
memory on a training partition and discarded. Nothing is exported, no frozen
artifact, manifest or raw file is modified, and none of these numbers replaces
the frozen Phase 1C results. Output: a JSON evidence file plus a text log.

Usage (from the repository root):
    .venv/Scripts/python.exe scripts/research_audit/verify_hfbtp.py --out D:/PLAN_BLOCK18/evidence
"""

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.data.hfbtp import HFBTPDatasetLoader  # noqa: E402
from fabric_perf.evaluation.splitters import (  # noqa: E402
    t1_interior_band_split,
    t1b_right_edge_band_split,
    t2_d0_workload_split,
    t3_orderer_split,
    workload_level_split,
)
from fabric_perf.models.baseline import (  # noqa: E402
    FEATURE_COLUMNS,
    RAW_FEATURES,
    add_engineered_features,
)

SET, BLOCK, ORD = "Set Transaction Arrival Rate", "Block Size", "Orderers"
TARGETS = ["Throughput", "Avg Latency"]
EXPECTED_MD5 = "E9275C49A2D7DC1F2A4015B6D8B28482"
LOG = []


def log(msg: str) -> None:
    print(msg, flush=True)
    LOG.append(msg)


def scores(y, p):
    return {"mae": float(mean_absolute_error(y, p)), "r2": float(r2_score(y, p))}


def make_model(name, seed=42):
    if name == "dummy":
        return DummyRegressor(strategy="median"), RAW_FEATURES
    if name == "mlr3":
        return LinearRegression(), RAW_FEATURES
    if name == "mlr8":
        return Pipeline([("s", StandardScaler()), ("m", LinearRegression())]), FEATURE_COLUMNS
    if name == "rf":
        return RandomForestRegressor(n_estimators=200, random_state=seed, n_jobs=-1), FEATURE_COLUMNS
    if name == "hgb":
        return HistGradientBoostingRegressor(random_state=seed), RAW_FEATURES
    raise ValueError(name)


def fit_predict(name, tr, te, target, seed=42):
    model, feats = make_model(name, seed)
    model.fit(tr[feats], tr[target])
    return model.predict(te[feats])


def nearest_block_copy(tr, te, target):
    """Zero-parameter baseline: copy the training cell with the same (Set,
    Orderers) and the nearest Block Size. Measures how much of a block-band
    score is explained by grid smoothness alone."""
    out = np.empty(len(te))
    groups = {k: g.sort_values(BLOCK) for k, g in tr.groupby([SET, ORD])}
    for i, (s, b, o) in enumerate(te[[SET, BLOCK, ORD]].itertuples(index=False)):
        g = groups.get((s, o))
        if g is None:
            out[i] = np.nan
            continue
        j = (g[BLOCK] - b).abs().to_numpy().argmin()
        out[i] = g[target].to_numpy()[j]
    return out


def nearest_level_copy(tr, te, target):
    """Zero-parameter baseline for workload splits: copy the training cell with
    the same (Block, Orderers) and the nearest offered level."""
    out = np.empty(len(te))
    groups = {k: g for k, g in tr.groupby([BLOCK, ORD])}
    for i, (s, b, o) in enumerate(te[[SET, BLOCK, ORD]].itertuples(index=False)):
        g = groups.get((b, o))
        if g is None:
            out[i] = np.nan
            continue
        j = (g[SET] - s).abs().to_numpy().argmin()
        out[i] = g[target].to_numpy()[j]
    return out


def nearest_orderer_copy(tr, te, target):
    """Zero-parameter baseline for orderer transfer: copy the training cell with
    the same (Set, Block) and the nearest orderer count."""
    out = np.empty(len(te))
    groups = {k: g for k, g in tr.groupby([SET, BLOCK])}
    for i, (s, b, o) in enumerate(te[[SET, BLOCK, ORD]].itertuples(index=False)):
        g = groups.get((s, b))
        if g is None:
            out[i] = np.nan
            continue
        j = (g[ORD] - o).abs().to_numpy().argmin()
        out[i] = g[target].to_numpy()[j]
    return out


def cluster_bootstrap_mae(y, p, groups, n_boot=2000, seed=0):
    """95% percentile CI for MAE resampling whole groups (not cells). Describes
    sensitivity to which held-out groups were drawn - NOT run-to-run noise."""
    rng = np.random.default_rng(seed)
    err = pd.Series(np.abs(np.asarray(y) - np.asarray(p))).groupby(np.asarray(groups))
    sums, counts = err.sum().to_numpy(), err.count().to_numpy()
    k = len(sums)
    stats = []
    for _ in range(n_boot):
        idx = rng.integers(0, k, k)
        stats.append(sums[idx].sum() / counts[idx].sum())
    return [float(np.percentile(stats, 2.5)), float(np.percentile(stats, 97.5))]


def main(out_dir: Path) -> None:
    t0 = time.time()
    out_dir.mkdir(parents=True, exist_ok=True)
    ev = {}

    loader = HFBTPDatasetLoader(REPO / "data" / "raw" / "hfbtp")
    df = loader.load_raw()
    md5 = hashlib.md5(loader._find_raw_file().read_bytes()).hexdigest().upper()
    grid = loader.describe_grid(df)
    full = pd.MultiIndex.from_product(
        [sorted(df[SET].unique()), sorted(df[BLOCK].unique()), sorted(df[ORD].unique())]
    )
    present = pd.MultiIndex.from_frame(df[[SET, BLOCK, ORD]])
    missing = full.difference(present)
    miss_df = missing.to_frame(index=False)
    miss_df.columns = [SET, BLOCK, ORD]
    ev["identity"] = {
        "md5": md5,
        "md5_matches_pin": md5 == EXPECTED_MD5,
        "shape": list(df.shape),
        "grid": grid,
        "duplicate_rows": int(df.duplicated().sum()),
        "missing_slots_block_orderers": miss_df.groupby([BLOCK, ORD]).size().to_dict().__repr__(),
        "missing_per_level_unique_counts": sorted(set(miss_df.groupby(SET).size().tolist())),
        "nulls": int(df.isnull().sum().sum()),
    }
    log(f"[identity] md5 ok={md5 == EXPECTED_MD5} shape={df.shape} missing={len(missing)}")

    lvl = df.groupby(SET).agg(
        tput_mean=("Throughput", "mean"),
        tput_sd=("Throughput", "std"),
        lat_mean=("Avg Latency", "mean"),
        lat_sd=("Avg Latency", "std"),
    )
    lvl["util"] = lvl["tput_mean"] / lvl.index.to_numpy()
    ev["per_level"] = lvl.round(4).reset_index().to_dict(orient="list")
    # Heteroscedasticity: spread of throughput across the configuration menu grows with load
    ev["heteroscedasticity"] = {
        "tput_sd_low_10_55": float(lvl.loc[10:55, "tput_sd"].mean()),
        "tput_sd_mid_60_145": float(lvl.loc[60:145, "tput_sd"].mean()),
        "tput_sd_heavy_165_200": float(lvl.loc[165:200, "tput_sd"].mean()),
        "lat_sd_low_10_55": float(lvl.loc[10:55, "lat_sd"].mean()),
        "lat_sd_heavy_165_200": float(lvl.loc[165:200, "lat_sd"].mean()),
    }
    # Grid smoothness: |difference| between adjacent block sizes at fixed (Set, Orderers)
    s = df.sort_values([SET, ORD, BLOCK])
    adj = s.groupby([SET, ORD])[TARGETS].diff().abs()
    adj[SET] = s[SET]
    band = pd.cut(adj[SET], [0, 55, 145, 160, 200], labels=["low", "mid", "edge", "heavy"])
    ev["adjacent_block_abs_diff"] = (
        adj.groupby(band, observed=True)[TARGETS].median().round(4).to_dict()
    )
    # Correlations / collinearity of the 8 engineered features
    fe = add_engineered_features(df)
    X8 = StandardScaler().fit_transform(fe[FEATURE_COLUMNS])
    ev["collinearity"] = {
        "condition_number_8feat_std": float(np.linalg.cond(X8)),
        "max_abs_pairwise_corr_8feat": float(
            (pd.DataFrame(X8).corr().abs() - np.eye(8)).to_numpy().max()
        ),
        "corr_set_actual": float(df[SET].corr(df["Actual Transaction Arrival Rate"])),
        "corr_actual_tput": float(df["Actual Transaction Arrival Rate"].corr(df["Throughput"])),
    }
    log(f"[structure] heterosc={ev['heteroscedasticity']}")

    splits = {
        "T1": t1_interior_band_split(fe),
        "T1b": t1b_right_edge_band_split(fe),
        "T2D0": t2_d0_workload_split(fe),
        "T3": t3_orderer_split(fe),
    }
    res = {}
    for sname, (tr, va, te, info) in splits.items():
        res[sname] = {"rows": [len(tr), len(va), len(te)]}
        for target in TARGETS:
            r = {}
            for m in ["dummy", "mlr3", "mlr8", "hgb"]:
                r[m] = {
                    "val": scores(va[target], fit_predict(m, tr, va, target)),
                    "test": scores(te[target], fit_predict(m, tr, te, target)),
                }
            rf_test = [scores(te[target], fit_predict("rf", tr, te, target, sd)) for sd in range(5)]
            r["rf_5seeds_test_mae"] = [round(x["mae"], 4) for x in rf_test]
            r["rf_5seeds_test_r2"] = [round(x["r2"], 4) for x in rf_test]
            p_rf = fit_predict("rf", tr, te, target, 42)
            r["rf"] = {
                "val": scores(va[target], fit_predict("rf", tr, va, target, 42)),
                "test": scores(te[target], p_rf),
            }
            grp = te[SET] if sname.startswith("T2") else te[BLOCK]
            r["rf_test_mae_cluster_boot95"] = cluster_bootstrap_mae(te[target], p_rf, grp)
            copier = {"T1": nearest_block_copy, "T1b": nearest_block_copy,
                      "T2D0": nearest_level_copy, "T3": nearest_orderer_copy}[sname]
            nb = copier(tr, te, target)
            ok = ~np.isnan(nb)
            r["nearest_copy"] = (
                {**scores(te[target][ok], nb[ok]), "coverage": float(ok.mean())}
                if ok.any() else {"coverage": 0.0}
            )
            if target == "Avg Latency":
                p = fit_predict("mlr8", tr, te, target)
                r["mlr8_negative_latency_preds_test"] = int((p < 0).sum())
            res[sname][target] = r
        log(f"[split] {sname} done ({time.time()-t0:.0f}s)")
    ev["splits"] = res

    tr, te = train_test_split(fe, test_size=0.2, random_state=42)
    ev["random_80_20"] = {
        t: {m: scores(te[t], fit_predict(m, tr, te, t)) for m in ["mlr3", "rf", "hgb"]}
        for t in TARGETS
    }
    log(f"[random] {ev['random_80_20']}")

    levels = sorted(fe[SET].unique())
    lolo = []
    for L in levels:
        tr_, te_ = fe[fe[SET] != L], fe[fe[SET] == L]
        row = {"level": float(L)}
        for t in TARGETS:
            row[f"hgb_{t}"] = float(mean_absolute_error(te_[t], fit_predict("hgb", tr_, te_, t)))
            row[f"mlr3_{t}"] = float(mean_absolute_error(te_[t], fit_predict("mlr3", tr_, te_, t)))
            nb = nearest_level_copy(tr_, te_, t)
            row[f"nearest_{t}"] = float(np.nanmean(np.abs(te_[t].to_numpy() - nb)))
        lolo.append(row)
    ev["leave_one_level_out"] = lolo
    log(f"[lolo] done ({time.time()-t0:.0f}s)")

    sweep = []
    for ceiling in [55, 100, 145]:
        tr_ = fe[fe[SET] <= ceiling]
        for gap_lo, gap_hi in [(5, 25), (30, 50), (55, 75), (80, 200)]:
            te_ = fe[(fe[SET] > ceiling + gap_lo - 5) & (fe[SET] <= ceiling + gap_hi)]
            if te_.empty:
                continue
            row = {"ceiling": ceiling, "test_levels": f"{ceiling + gap_lo}-{min(ceiling + gap_hi, 200)}"}
            for t in TARGETS:
                for m in ["hgb", "rf", "mlr3"]:
                    row[f"{m}_{t}_mae"] = float(mean_absolute_error(te_[t], fit_predict(m, tr_, te_, t)))
            # capacity-aware baseline: min(offered, max throughput seen for that config in training)
            cap = tr_.groupby([BLOCK, ORD])["Throughput"].max()
            capv = te_.set_index([BLOCK, ORD]).index.map(cap).to_numpy(dtype=float)
            row["cap_min_offered_Throughput_mae"] = float(
                np.nanmean(np.abs(te_["Throughput"].to_numpy() - np.minimum(te_[SET].to_numpy(), capv)))
            )
            row["identity_offered_Throughput_mae"] = float(mean_absolute_error(te_["Throughput"], te_[SET]))
            sweep.append(row)
    ev["extrapolation_ceiling_sweep"] = sweep
    log(f"[sweep] done ({time.time()-t0:.0f}s)")

    tr, va, te, _ = workload_level_split(
        fe, test_levels=tuple(range(165, 201, 5)), val_levels=(150, 155),
        extra_excluded_levels=(30, 140, 160), name="HEAVY_diag",
    )
    heavy = {}
    for t in TARGETS:
        heavy[t] = {m: scores(te[t], fit_predict(m, tr, te, t)) for m in ["mlr3", "rf", "hgb"]}
    heavy["Throughput"]["identity_offered"] = scores(te["Throughput"], te[SET])
    heavy["Throughput"]["const_level145_mean"] = scores(
        te["Throughput"], np.full(len(te), fe.loc[fe[SET] == 145, "Throughput"].mean())
    )
    ev["heavy_diag"] = heavy
    log(f"[heavy] {heavy}")

    ev["runtime_s"] = round(time.time() - t0, 1)
    ev["env"] = {"python": sys.version.split()[0], "pandas": pd.__version__, "numpy": np.__version__}
    (out_dir / "verify_hfbtp_evidence.json").write_text(json.dumps(ev, indent=2, default=str))
    (out_dir / "verify_hfbtp_log.txt").write_text("\n".join(LOG))
    log(f"[done] wrote {out_dir} in {ev['runtime_s']}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="D:/PLAN_BLOCK18/evidence")
    main(Path(ap.parse_args().out))
