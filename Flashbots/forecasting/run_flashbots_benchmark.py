"""FB-FC-v1: offline multi-horizon forecasting benchmark (select -> freeze -> final), plus a P2 reproduction check.

    # 1. reproduce the stored, frozen P2 benchmark (recompute its 12 Oct forecasts and compare with the stored ones)
    .venv/Scripts/python.exe -B Flashbots/forecasting/run_flashbots_benchmark.py reproduce-p2
    # 2. selection: reads ONLY 2026-09-21 (D fits, V selects), writes and hashes selection_frozen.json
    .venv/Scripts/python.exe -B Flashbots/forecasting/run_flashbots_benchmark.py select
    # 3. final: reads 2026-10-12 once, refuses unless the frozen file's SHA-256 and the code hashes match
    .venv/Scripts/python.exe -B Flashbots/forecasting/run_flashbots_benchmark.py final --frozen-sha256 <sha>

Outputs: Flashbots/results/FB-FC-v1/{select,final}/, Flashbots/models/FB-FC-v1/, Flashbots/experiments/runs/.
Nothing outside Flashbots/ is written. Existing outputs are never overwritten (each stage refuses if its folder exists).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import fbcommon as fc  # noqa: E402
import models as fm  # noqa: E402

sys.path.insert(0, str(fc.FB / "evaluation"))
import metrics as em  # noqa: E402

from fabric_perf.forecasting import horizon_benchmark as hb  # noqa: E402

CONFIG = fc.FB / "experiments" / "configs" / "FB-FC-v1.json"
D_DATE, F_DATE = "2026-09-21", "2026-10-12"


def cfg() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def out_root(config: dict) -> Path:
    return fc.FB / "results" / config["experiment_id"]


def code_hashes() -> dict[str, str]:
    files = [Path(__file__), HERE / "models.py", fc.FB / "evaluation" / "metrics.py", fc.FB / "fbcommon.py", Path(hb.__file__)]
    return {str(p.resolve().relative_to(fc.REPO)).replace("\\", "/"): fc.sha256_file(p) for p in files}


def series_file(date: str) -> Path:
    return fc.TS_DIR / f"target_series_{date}_1s.csv.gz"


def run_record(config: dict, mode: str, started: float, extra: dict) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(started))
    path = fc.FB / "experiments" / "runs" / f"{config['experiment_id']}_{mode}_{stamp}.json"
    fc.write_json(path, {"experiment_id": config["experiment_id"], "mode": mode, "command": " ".join(sys.argv),
                         "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
                         "finished_utc": fc.utc_now(), "duration_s": round(time.time() - started, 1),
                         "config_file": str(CONFIG.relative_to(fc.REPO)), "config_sha256": fc.sha256_file(CONFIG),
                         "code_sha256": code_hashes(), "environment": fc.environment(), **extra})
    return path


def score_rows(pool: str, target: str, horizon: int, y: np.ndarray, yhat: dict[str, np.ndarray], scale: float) -> pd.DataFrame:
    rows = [{"pool": pool, "target": target, "horizon_s": horizon, "model": m, **em.all_metrics(y / scale, p / scale)}
            for m, p in yhat.items()]
    frame = pd.DataFrame(rows)
    ref = float(frame.loc[frame["model"] == fm.REFERENCE, "mae"].iloc[0])
    frame["skill_mae_vs_persistence"] = 1.0 - frame["mae"] / ref
    frame["units"] = "per second" if scale == 1 else f"per second / {scale:g}"
    return frame


def regime_rows(pool, target, horizon, y, yhat, labels, scale) -> list[dict]:
    rows = []
    for k, name in enumerate(("calm", "typical", "busy")):
        mask = labels == k
        if mask.any():
            for m, p in yhat.items():
                rows.append({"pool": pool, "target": target, "horizon_s": horizon, "model": m, "regime": name,
                             **em.all_metrics(y[mask] / scale, p[mask] / scale)})
    return rows


def forecast_frame(org, horizon, y, trailing, labels, yhat, date) -> pd.DataFrame:
    frame = pd.DataFrame({"origin_s": org, "origin_utc": (pd.Timestamp(date, tz="UTC") + pd.to_timedelta(org, unit="s")).astype(str),
                          "target_end_s": org + horizon, "y_observed": y, "trailing_300s_arrival_rate": trailing, "regime": labels})
    for m, p in yhat.items():
        frame[f"yhat_{m}"] = p
    return frame


# ------------------------------------------------------------------------------------------------ select
def select_run(force: bool = False) -> dict:
    started = time.time()
    config = cfg()
    out = out_root(config) / "select"
    model_dir = fc.FB / "models" / config["experiment_id"]
    if out.exists() and not force:
        raise FileExistsError(f"{out} exists; the selection is frozen once")
    day = fc.load_target_series(D_DATE)  # 21 Sep ONLY
    pools = config["pools"]
    d0, d1 = pools["D"]["first_origin_s"], pools["D"]["end_s"]
    v0, v1 = pools["V"]["first_origin_s"], pools["V"]["end_s"]
    arrivals = day["arrival_count"].to_numpy(float)
    cut_org = fm.origins(d0, d1, 60)
    cutoffs = [float(x) for x in np.quantile(fm.trailing_rate(arrivals[:d1], cut_org), [1 / 3, 2 / 3])]
    out.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)
    frozen = {"experiment_id": config["experiment_id"], "created_utc": fc.utc_now(), "regime_cutoffs_trailing_300s_tx_per_s": cutoffs,
              "selection": {}}
    scores, regimes, ridge_tables, alpha_tables = [], [], [], []
    for target, spec in config["targets"].items():
        series = day[spec["column"]].to_numpy(float)
        scale = float(spec.get("report_scale", 1))
        series_d = series[:d1]  # pool D only: everything fitted below sees nothing after 12:00 UTC
        for horizon in config["horizons_s"]:
            org_d, org_v = fm.origins(d0, d1, horizon), fm.origins(v0, v1, horizon)
            alpha, a_table = fm.tune_ewma(series_d, org_d, horizon, config["ewma_alpha_grid"])
            alpha_tables.append(a_table.assign(target=target, horizon_s=horizon))
            ridge, r_table = fm.fit_ridge(series_d, org_d, horizon, config["ridge"]["windows_s"], config["ridge"]["lambda_grid"],
                                          config["ridge"]["cv_folds"])
            ridge_tables.append(r_table.assign(target=target, horizon_s=horizon))
            ridge.update({"target": target, "unit": spec["unit"], "fitted_on": f"{D_DATE} pool D, origins [{d0}, {d1}) s"})
            ridge_path = model_dir / f"ridge_ar_{target}_H{horizon}.json"
            fc.write_json(ridge_path, ridge)
            names = fm.models_for_horizon(horizon)
            results = {}
            for pool, org, data in (("D", org_d, series_d), ("V", org_v, series)):
                yhat = {m: fm.forecast(m, data, org, horizon, alpha, ridge) for m in names}
                y = fm.targets(data, org, horizon)
                labels = np.digitize(fm.trailing_rate(arrivals, org), cutoffs)
                scores.append(score_rows(pool, target, horizon, y, yhat, scale))
                regimes.extend(regime_rows(pool, target, horizon, y, yhat, labels, scale))
                results[pool] = (org, y, yhat, labels)
            val = scores[-1].set_index("model")["mae"].to_dict()
            chosen = fm.select(val, config["near_tie"])
            org, y, yhat, labels = results["V"]
            forecast_frame(org, horizon, y, fm.trailing_rate(arrivals, org), labels, yhat, D_DATE).to_csv(
                out / f"forecasts_V_{target}_H{horizon}.csv", index=False)
            frozen["selection"][f"{target}|{horizon}"] = {
                "target": target, "horizon_s": horizon, "models": names, "selected_model": chosen, "ewma_alpha": alpha,
                "ridge_lambda": ridge["lambda"], "ridge_params_file": str(ridge_path.relative_to(fc.REPO)).replace("\\", "/"),
                "ridge_params_sha256": fc.sha256_file(ridge_path), "validation_mae": val,
                "origins": {"D": int(len(org_d)), "V": int(len(org_v))}}
            print(f"select {target:16s} H={horizon:>3}s alpha={alpha:<6} lambda={ridge['lambda']:<7} -> {chosen:22s} V-MAE={val[chosen] :.4f}")
    pd.concat(scores).to_csv(out / "scores_D_V.csv", index=False)
    pd.DataFrame(regimes).to_csv(out / "regimes_D_V.csv", index=False)
    pd.concat(alpha_tables).to_csv(out / "ewma_alpha_grid_D.csv", index=False)
    rt = pd.concat(ridge_tables)
    rt["cv_mae_folds"] = rt["cv_mae_folds"].map(json.dumps)
    rt.to_csv(out / "ridge_lambda_cv_D.csv", index=False)
    fc.write_json(model_dir / "ewma_alphas.json", {k: {"target": v["target"], "horizon_s": v["horizon_s"], "alpha": v["ewma_alpha"]}
                                                   for k, v in frozen["selection"].items()})
    frozen.update({"config_sha256": fc.sha256_file(CONFIG), "code_sha256": code_hashes(),
                   "series_sha256": {D_DATE: fc.sha256_file(series_file(D_DATE))}, "environment": fc.environment()})
    path = out / "selection_frozen.json"
    fc.write_json(path, frozen)
    sha = fc.sha256_file(path)
    rec = run_record(config, "select", started, {"inputs": {str(series_file(D_DATE).relative_to(fc.REPO)): frozen["series_sha256"][D_DATE]},
                                                  "outputs_dir": str(out.relative_to(fc.REPO)), "selection_frozen_sha256": sha})
    print(f"FROZEN {path.relative_to(fc.REPO)} sha256={sha}\nrun record: {rec.relative_to(fc.REPO)}")
    return {"frozen_sha256": sha}


# ------------------------------------------------------------------------------------------------ final
def final_run(expected_sha: str) -> dict:
    started = time.time()
    config = cfg()
    root = out_root(config)
    frozen_path = root / "select" / "selection_frozen.json"
    if fc.sha256_file(frozen_path) != expected_sha.upper():
        raise ValueError("selection_frozen.json SHA-256 differs from the value given")
    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
    if frozen["code_sha256"] != code_hashes() or frozen["config_sha256"] != fc.sha256_file(CONFIG):
        raise ValueError("code or config changed after the selection was frozen")
    out = root / "final"
    if out.exists():
        raise FileExistsError(f"{out} exists; F is scored exactly once")
    test_day = fc.load_target_series(F_DATE)
    reference_day = fc.load_target_series(D_DATE)  # only for the exploratory time-of-day profile (an earlier date)
    out.mkdir(parents=True)
    f0, f1 = config["pools"]["F"]["first_origin_s"], config["pools"]["F"]["end_s"]
    cutoffs = frozen["regime_cutoffs_trailing_300s_tx_per_s"]
    arrivals = test_day["arrival_count"].to_numpy(float)
    scores, regimes, dm_rows = [], [], []
    report = {"experiment_id": config["experiment_id"], "created_utc": fc.utc_now(), "frozen_sha256": expected_sha.upper(),
              "series_sha256": {F_DATE: fc.sha256_file(series_file(F_DATE)), D_DATE: fc.sha256_file(series_file(D_DATE))},
              "results": {}}
    for key, sel in frozen["selection"].items():
        target, horizon = sel["target"], sel["horizon_s"]
        spec = config["targets"][target]
        scale = float(spec.get("report_scale", 1))
        series = test_day[spec["column"]].to_numpy(float)
        ridge = json.loads((fc.REPO / sel["ridge_params_file"]).read_text(encoding="utf-8"))
        if fc.sha256_file(fc.REPO / sel["ridge_params_file"]) != sel["ridge_params_sha256"]:
            raise ValueError("ridge parameter file changed after freezing")
        org = fm.origins(f0, f1, horizon)
        y = fm.targets(series, org, horizon)
        yhat = {m: fm.forecast(m, series, org, horizon, sel["ewma_alpha"], ridge) for m in sel["models"]}
        yhat["tod_profile_0921"] = fm.tod_profile(reference_day[spec["column"]].to_numpy(float), org, horizon)
        labels = np.digitize(fm.trailing_rate(arrivals, org), cutoffs)
        table = score_rows("F", target, horizon, y, yhat, scale)
        table["selected"] = table["model"] == sel["selected_model"]
        table["exploratory_not_selectable"] = table["model"].isin(fm.EXPLORATORY)
        scores.append(table)
        regimes.extend(regime_rows("F", target, horizon, y, yhat, labels, scale))
        forecast_frame(org, horizon, y, fm.trailing_rate(arrivals, org), labels, yhat, F_DATE).to_csv(
            out / f"forecasts_F_{target}_H{horizon}.csv", index=False)
        chosen = sel["selected_model"]
        loss_sel = np.abs(yhat[chosen] - y)
        for comparator in yhat:
            if comparator != chosen:
                dm_rows.append({"target": target, "horizon_s": horizon, "selected": chosen, "comparator": comparator,
                                **hb.dm_test(loss_sel, np.abs(yhat[comparator] - y))})
        row = table.set_index("model").loc[chosen]
        report["results"][key] = {"target": target, "horizon_s": horizon, "selected_model": chosen, "origins_F": int(len(org)),
                                  "mae_F": float(row["mae"]) , "mae_V": sel["validation_mae"][chosen],
                                  "skill_mae_vs_persistence_F": float(row["skill_mae_vs_persistence"]),
                                  "best_F_model_hindsight": str(table.loc[~table["exploratory_not_selectable"]].sort_values("mae")["model"].iloc[0]),
                                  "unit": spec.get("report_unit", spec["unit"])}
        print(f"final  {target:16s} H={horizon:>3}s {chosen:22s} F-MAE={row['mae']:.4f} skill={row['skill_mae_vs_persistence']:+.3f}")
    dm = pd.DataFrame(dm_rows)
    primary = (dm["comparator"] == fm.REFERENCE) & ~dm["identical"]
    dm["p_holm_vs_persistence_family"] = np.nan
    if primary.any():
        dm.loc[primary, "p_holm_vs_persistence_family"] = hb.holm(dm.loc[primary, "p"].tolist())
    dm.to_csv(out / "dm_F.csv", index=False)
    pd.concat(scores).to_csv(out / "scores_F.csv", index=False)
    pd.DataFrame(regimes).to_csv(out / "regimes_F.csv", index=False)
    report["code_sha256"] = code_hashes()
    fc.write_json(out / "final_report.json", report)
    rec = run_record(config, "final", started, {"inputs": {k: v for k, v in report["series_sha256"].items()},
                                                 "selection_frozen_sha256": expected_sha.upper(),
                                                 "outputs_dir": str(out.relative_to(fc.REPO)),
                                                 "final_report_sha256": fc.sha256_file(out / "final_report.json")})
    print(f"run record: {rec.relative_to(fc.REPO)}")
    return report


# ------------------------------------------------------------------------------------------------ P2 reproduction
def reproduce_p2() -> dict:
    """Recompute the frozen P2 forecasts from the stored 1-s series and compare with the stored outputs; then add
    metrics P2 did not report (sMAPE, MAPE) computed on P2's own stored 12 Oct forecasts."""
    from fabric_perf.replay import trace

    started = time.time()
    out = fc.FB / "results" / "p2_reproduction"
    if out.exists():
        raise FileExistsError(f"{out} exists")
    frozen = json.loads((fc.P2_DIR / "select" / "selection_frozen.json").read_text(encoding="utf-8"))
    counts = trace.read_series(fc.STORED_SERIES_DIR, F_DATE, 1).astype(float)
    checks, extended = [], []
    for horizon_s, info in frozen["horizons"].items():
        horizon = int(horizon_s)
        stored_fc = pd.read_csv(fc.P2_DIR / "final" / f"forecasts_F_H{horizon}.csv")
        stored_scores = pd.read_csv(fc.P2_DIR / "final" / f"scores_F_H{horizon}.csv").set_index("model")
        res = hb.evaluate_pool("F", counts, horizon, info["alpha"])
        for model, yhat in res.yhat.items():
            s = hb.score(res.y, yhat)
            checks.append({"horizon_s": horizon, "model": model,
                           "max_abs_diff_forecast_vs_stored": float(np.abs(stored_fc[f"yhat_{model}"].to_numpy() - yhat).max()),
                           "mae_recomputed": s["mae"], "mae_stored": float(stored_scores.loc[model, "mae"]),
                           "abs_diff_mae": abs(s["mae"] - float(stored_scores.loc[model, "mae"])),
                           "abs_diff_rmse": abs(s["rmse"] - float(stored_scores.loc[model, "rmse"]))})
            extended.append({"pool": "F", "horizon_s": horizon, "model": model, "selected_by_P2": model == info["selected_model"],
                             **em.all_metrics(stored_fc["y_observed"].to_numpy(), stored_fc[f"yhat_{model}"].to_numpy())})
        checks.append({"horizon_s": horizon, "model": "<targets>", "max_abs_diff_forecast_vs_stored":
                       float(np.abs(stored_fc["y_observed"].to_numpy() - res.y).max())})
    out.mkdir(parents=True)
    pd.DataFrame(checks).to_csv(out / "p2_reproduction_checks.csv", index=False)
    pd.DataFrame(extended).to_csv(out / "p2_extended_metrics_F.csv", index=False)
    worst = max(c["max_abs_diff_forecast_vs_stored"] for c in checks)
    summary = {"generated_utc": fc.utc_now(), "p2_dir": str(fc.P2_DIR.relative_to(fc.REPO)),
               "p2_frozen_selection_sha256": fc.sha256_file(fc.P2_DIR / "select" / "selection_frozen.json"),
               "p2_code_hashes_match_current_src": {k: v == frozen["code_sha256"][k] for k, v in
                                                    {Path(hb.__file__).name: fc.sha256_file(hb.__file__)}.items()},
               "max_abs_diff_any_forecast_or_target": worst, "reproduced_exactly": worst < 1e-9,
               "note": "Recomputation of already-published P2 numbers with the frozen configuration; no new selection on 12 Oct."}
    fc.write_json(out / "p2_reproduction.json", summary)
    run_record(cfg(), "reproduce-p2", started, {"outputs_dir": str(out.relative_to(fc.REPO)), **summary})
    print(json.dumps(summary, indent=2))
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["reproduce-p2", "select", "final"])
    parser.add_argument("--frozen-sha256")
    args = parser.parse_args()
    if args.mode == "reproduce-p2":
        reproduce_p2()
    elif args.mode == "select":
        select_run()
    else:
        if not args.frozen_sha256:
            print("REFUSED: --frozen-sha256 is required")
            return 2
        final_run(args.frozen_sha256)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
