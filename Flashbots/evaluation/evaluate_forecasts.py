"""Evaluate the stored FB-FC-v1 forecasts: comparison tables, horizon table, residual diagnostics, figures.

Reads only the stored forecast/score files written by forecasting/run_flashbots_benchmark.py (no model is
refitted and no new forecast is made). Writes Flashbots/evaluation/FB-FC-v1/*.csv|json and
Flashbots/visualizations/fc_*.png.

    .venv/Scripts/python.exe -B Flashbots/evaluation/evaluate_forecasts.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import fbcommon as fc  # noqa: E402
import metrics as em  # noqa: E402

fc.plot_style()
import matplotlib.pyplot as plt  # noqa: E402

EXP = "FB-FC-v1"
RES = fc.FB / "results" / EXP
OUT = fc.FB / "evaluation" / EXP
TARGET_LABEL = {"arrival_rate": ("Arrival rate", "tx/s", 1.0), "gas_demand_rate": ("Declared gas demand", "Mgas/s", 1e6)}
MODEL_COLOR = {"persistence_interval": fc.SERIES[0], "ewma": fc.SERIES[1], "ridge_ar": fc.SERIES[2], "rolling_mean_900": fc.SERIES[3],
               "expanding_mean": fc.SERIES[6], "tod_profile_0921": fc.SERIES[7], "rolling_mean_60": fc.SERIES[5]}
LABEL = {"persistence_interval": "naive (persistence)", "ewma": "EWMA", "ridge_ar": "ridge AR", "rolling_mean_900": "rolling mean 900 s",
         "expanding_mean": "expanding mean", "tod_profile_0921": "time-of-day profile (21 Sep)", "rolling_mean_60": "rolling mean 60 s"}


def load(pool: str, target: str, horizon: int) -> pd.DataFrame:
    folder = "final" if pool == "F" else "select"
    return pd.read_csv(RES / folder / f"forecasts_{pool}_{target}_H{horizon}.csv")


def acf(x: np.ndarray, lags: int) -> list[float]:
    x = x - x.mean()
    d = float(x @ x)
    return [float(x[:-k] @ x[k:]) / d for k in range(1, lags + 1)]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frozen = json.loads((RES / "select" / "selection_frozen.json").read_text(encoding="utf-8"))
    scores_f = pd.read_csv(RES / "final" / "scores_F.csv")
    scores_dv = pd.read_csv(RES / "select" / "scores_D_V.csv")
    dm = pd.read_csv(RES / "final" / "dm_F.csv")

    # ---------------- model comparison (all models, F, with V MAE alongside)
    v = scores_dv[scores_dv.pool == "V"][["target", "horizon_s", "model", "mae"]].rename(columns={"mae": "mae_V"})
    comp = scores_f.merge(v, on=["target", "horizon_s", "model"], how="left")
    comp["rank_F_mae"] = comp.groupby(["target", "horizon_s"])["mae"].rank(method="min")
    comp.to_csv(OUT / "model_comparison_F.csv", index=False)

    # ---------------- horizon table: selected vs persistence vs best-in-hindsight
    rows, gaps, diag, hourly = [], [], [], []
    for key, sel in frozen["selection"].items():
        target, horizon, chosen = sel["target"], sel["horizon_s"], sel["selected_model"]
        block = comp[(comp.target == target) & (comp.horizon_s == horizon)].set_index("model")
        selectable = block[~block["exploratory_not_selectable"]]
        best = selectable["mae"].idxmin()
        d = dm[(dm.target == target) & (dm.horizon_s == horizon) & (dm.comparator == "persistence_interval")]
        for role, model in (("selected", chosen), ("naive_reference", "persistence_interval"), ("best_F_in_hindsight", best)):
            r = block.loc[model]
            rows.append({"target": target, "unit": TARGET_LABEL[target][1], "horizon_s": horizon, "role": role, "model": model,
                         "mae_F": r["mae"], "rmse_F": r["rmse"], "bias_F": r["bias"], "wape_pct_F": r["wape_pct"], "smape_pct_F": r["smape_pct"],
                         "mape_pct_F_y_gt_0": r["mape_pct_y_gt_0"], "skill_mae_vs_persistence_F": r["skill_mae_vs_persistence"], "mae_V": r["mae_V"],
                         "dm_p_holm_selected_vs_persistence": (float(d["p_holm_vs_persistence_family"].iloc[0]) if role == "selected" and len(d) else None)})
        gaps.append({"target": target, "horizon_s": horizon, "selected": chosen, "mae_V": block.loc[chosen, "mae_V"], "mae_F": block.loc[chosen, "mae"],
                     "relative_change_F_vs_V": block.loc[chosen, "mae"] / block.loc[chosen, "mae_V"] - 1,
                     "selected_rank_on_F_among_selectable": int(selectable["mae"].rank(method="min").loc[chosen]),
                     "n_selectable": len(selectable), "regret_vs_best_F_mae": block.loc[chosen, "mae"] - selectable["mae"].min()})
        # residual diagnostics of the selected model
        f = load("F", target, horizon)
        scale = TARGET_LABEL[target][2]
        e = (f[f"yhat_{chosen}"] - f["y_observed"]).to_numpy() / scale
        sd = e.std(ddof=1)
        a = acf(e, 5)
        diag.append({"target": target, "horizon_s": horizon, "model": chosen, "n": len(e), "residual_mean": e.mean(), "residual_sd": sd,
                     **{f"residual_acf_lag{k}": a[k - 1] for k in range(1, 6)},
                     "share_abs_error_gt_2sd": float((np.abs(e) > 2 * sd).mean()),
                     "residual_q01": np.quantile(e, 0.01), "residual_q50": np.quantile(e, 0.5), "residual_q99": np.quantile(e, 0.99),
                     "largest_under_forecast": float(e.min()), "largest_under_forecast_at_utc": f["origin_utc"].iloc[int(e.argmin())],
                     "largest_over_forecast": float(e.max()), "largest_over_forecast_at_utc": f["origin_utc"].iloc[int(e.argmax())]})
        if horizon in (10, 60):
            f["utc_hour"] = f["origin_s"] // 3600
            for hour, g in f.groupby("utc_hour"):
                for model in (chosen, "persistence_interval"):
                    hourly.append({"target": target, "horizon_s": horizon, "utc_hour": hour, "model": model,
                                   "mae": em.mae(g["y_observed"] / scale, g[f"yhat_{model}"] / scale), "observed_mean": g["y_observed"].mean() / scale})
    horizon_table = pd.DataFrame(rows)
    horizon_table.to_csv(OUT / "horizon_table.csv", index=False)
    pd.DataFrame(gaps).to_csv(OUT / "validation_vs_test_stability.csv", index=False)
    pd.DataFrame(diag).to_csv(OUT / "residual_diagnostics.csv", index=False)
    hourly = pd.DataFrame(hourly)
    hourly.to_csv(OUT / "error_by_utc_hour_F.csv", index=False)
    regimes = pd.read_csv(RES / "final" / "regimes_F.csv")
    regimes.to_csv(OUT / "error_by_regime_F.csv", index=False)
    fc.write_json(OUT / "evaluation_summary.json", {
        "generated_utc": fc.utc_now(), "experiment_id": EXP, "selection_frozen_sha256": fc.sha256_file(RES / "select" / "selection_frozen.json"),
        "final_report_sha256": fc.sha256_file(RES / "final" / "final_report.json"),
        "selected": {k: {"model": s["selected_model"], **{c: horizon_table[(horizon_table.target == s["target"]) & (horizon_table.horizon_s == s["horizon_s"]) &
                                                                           (horizon_table.role == "selected")].iloc[0][c] for c in
                                                       ("mae_F", "rmse_F", "smape_pct_F", "wape_pct_F", "bias_F", "skill_mae_vs_persistence_F", "mae_V", "dm_p_holm_selected_vs_persistence")}}
                     for k, s in frozen["selection"].items()},
    })
    figures(frozen, comp, hourly, regimes)
    print(horizon_table[horizon_table.role != "best_F_in_hindsight"][["target", "horizon_s", "role", "model", "mae_F", "smape_pct_F",
                                                                       "skill_mae_vs_persistence_F", "dm_p_holm_selected_vs_persistence"]].to_string(index=False))


def figures(frozen, comp, hourly, regimes) -> None:
    sel = {(s["target"], s["horizon_s"]): s["selected_model"] for s in frozen["selection"].values()}

    # fc_01 actual vs forecast, arrival rate H = 60 s, full test day + zoom on the worst 2 hours
    f = load("F", "arrival_rate", 60)
    chosen = sel[("arrival_rate", 60)]
    hours = f["origin_s"] / 3600
    err = np.abs(f[f"yhat_{chosen}"] - f["y_observed"]).rolling(120, center=True, min_periods=1).mean()
    centre = float(hours.iloc[int(err.idxmax())])
    lo, hi = max(0.25, centre - 1), min(24, centre + 1)
    fig, axes = plt.subplots(2, 1, figsize=(11, 6.4))
    for ax, (a, b) in zip(axes, ((0, 24), (lo, hi))):
        m = (hours >= a) & (hours <= b)
        ax.plot(hours[m], f["y_observed"][m], color="#9a9893", lw=1.0, label="observed 60-s mean rate")
        if (a, b) != (0, 24):  # full day: naive is the observed series shifted by one origin, so it would hide it
            ax.plot(hours[m], f["yhat_persistence_interval"][m], color=MODEL_COLOR["persistence_interval"], lw=1.0, label="naive (persistence)")
        ax.plot(hours[m], f[f"yhat_{chosen}"][m], color=MODEL_COLOR[chosen], lw=1.6, label=f"selected: {LABEL[chosen]}")
        ax.set_xlim(a, b)
        ax.set_ylabel("Arrival rate (tx/s)")
    axes[0].set_title("Test day 2026-10-12: next-60-s arrival rate, observed vs forecast (each point = one forecast origin)")
    axes[1].set_title(f"Zoom: the 2 h with the largest selected-model errors ({lo:.1f}-{hi:.1f} UTC)")
    axes[1].set_xlabel("Forecast origin (UTC hour)")
    axes[0].legend(loc="upper right", ncol=2)
    axes[1].legend(loc="upper right", ncol=3)
    fig.tight_layout()
    fc.save_figure(fig, "fc_01_actual_vs_forecast_arrival_H60.png")

    # fc_02 short horizon zoom (H = 10 s, 30 min)
    f10 = load("F", "arrival_rate", 10)
    chosen10 = sel[("arrival_rate", 10)]
    m = (f10["origin_s"] >= 12 * 3600) & (f10["origin_s"] < 12.5 * 3600)
    minutes = (f10["origin_s"][m] - 12 * 3600) / 60
    fig, ax = plt.subplots(figsize=(11, 3.8))
    ax.plot(minutes, f10["y_observed"][m], color="#9a9893", lw=1.0, label="observed 10-s mean rate")
    ax.plot(minutes, f10["yhat_persistence_interval"][m], color=MODEL_COLOR["persistence_interval"], lw=1.0, label="naive (persistence)")
    ax.plot(minutes, f10[f"yhat_{chosen10}"][m], color=MODEL_COLOR[chosen10], lw=1.6, label=f"selected: {LABEL[chosen10]}")
    ax.set_xlabel("Minutes after 12:00 UTC, 2026-10-12")
    ax.set_ylabel("Arrival rate (tx/s)")
    ax.set_title("Short horizon (H = 10 s): forecasts track the level, not the 10-s noise")
    ax.legend(loc="upper right", ncol=3)
    fc.save_figure(fig, "fc_02_actual_vs_forecast_arrival_H10_zoom.png")

    # fc_03 skill by horizon
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.2), sharey=True)
    for ax, target in zip(axes, TARGET_LABEL):
        block = comp[comp.target == target]
        for model in ("ewma", "ridge_ar", "rolling_mean_900", "expanding_mean", "tod_profile_0921"):
            b = block[block.model == model].sort_values("horizon_s")
            ax.plot(b.horizon_s, b.skill_mae_vs_persistence * 100, marker="o", ms=5, color=MODEL_COLOR[model], label=LABEL[model])
        chosen_pts = [(h, block[(block.model == sel[(target, h)]) & (block.horizon_s == h)].skill_mae_vs_persistence.iloc[0] * 100)
                      for h in sorted(block.horizon_s.unique())]
        ax.scatter(*zip(*chosen_pts), s=130, facecolors="none", edgecolors=fc.TEXT, linewidths=1.3, label="selected on V", zorder=5)
        ax.axhline(0, color=fc.TEXT2, lw=1)
        ax.set_xscale("log")
        ax.set_xticks([1, 10, 60, 300], ["1 s", "10 s", "60 s", "300 s"])
        ax.set_xlabel("Forecast horizon H")
        ax.set_title(f"{TARGET_LABEL[target][0]} ({TARGET_LABEL[target][1]})")
    axes[0].set_ylabel("MAE skill vs naive on 12 Oct (%)\n(> 0 = better than persistence)")
    axes[1].legend(loc="lower left", fontsize=8.5)
    fig.suptitle("Test-day skill by horizon: smoothing helps the arrival rate, little helps gas demand", fontweight="bold")
    fig.tight_layout()
    fc.save_figure(fig, "fc_03_skill_by_horizon.png")

    # fc_04 model comparison at H = 60 s (arrival) and H = 10 s
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4))
    for ax, horizon in zip(axes, (60, 10)):
        b = comp[(comp.target == "arrival_rate") & (comp.horizon_s == horizon)].sort_values("mae", ascending=False)
        ys = np.arange(len(b))
        colors = [fc.SERIES[1] if s else ("#c9c7c1" if x else fc.SERIES[0]) for s, x in zip(b.selected, b.exploratory_not_selectable)]
        ax.barh(ys, b.mae, color=colors, height=0.62)
        ax.scatter(b.mae_V, ys, marker="D", s=26, color=fc.TEXT, zorder=4, label="validation MAE (21 Sep 12-24Z)")
        for y_, val in zip(ys, b.mae):
            ax.text(val - 0.03, y_, f"{val:.2f}", va="center", ha="right", fontsize=8, color="white", fontweight="bold")
        ax.set_yticks(ys, [LABEL.get(m, m.replace("_", " ")) for m in b.model])
        ax.set_xlabel("MAE on test day 2026-10-12 (tx/s)")
        ax.set_title(f"Arrival rate, H = {horizon} s")
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    handles = [Patch(color=fc.SERIES[1], label="selected on validation"), Patch(color=fc.SERIES[0], label="other selectable model"),
               Patch(color="#c9c7c1", label="exploratory (not selectable)"),
               Line2D([], [], marker="D", ls="", color=fc.TEXT, label="validation MAE (21 Sep 12-24Z)")]
    fig.legend(handles=handles, loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.06))
    fig.suptitle("Test-day MAE per model (bars) with validation MAE (diamonds)", fontweight="bold")
    fig.tight_layout()
    fc.save_figure(fig, "fc_04_model_comparison_arrival.png")

    # fc_05 residuals of the selected H = 60 s model
    e = f[f"yhat_{chosen}"] - f["y_observed"]
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 3.8), gridspec_kw={"width_ratios": [2.2, 1]})
    axes[0].scatter(hours, e, s=5, color=MODEL_COLOR[chosen], alpha=0.6, linewidths=0)
    axes[0].axhline(0, color=fc.TEXT2, lw=1)
    axes[0].set_xlim(0, 24)
    axes[0].set_xlabel("Forecast origin (UTC hour, 2026-10-12)")
    axes[0].set_ylabel("Forecast - observed (tx/s)")
    axes[0].set_title(f"Residuals over the test day ({LABEL[chosen]}, H = 60 s)")
    axes[1].hist(e, bins=60, color=MODEL_COLOR[chosen])
    axes[1].axvline(0, color=fc.TEXT2, lw=1)
    axes[1].set_xlabel("Forecast - observed (tx/s)")
    axes[1].set_ylabel("Forecast origins")
    axes[1].set_title(f"Distribution (bias {e.mean():+.3f} tx/s)")
    fig.tight_layout()
    fc.save_figure(fig, "fc_05_residuals_arrival_H60.png")

    # fc_06 error by UTC hour
    fig, ax = plt.subplots(figsize=(10, 3.8))
    h = hourly[(hourly.target == "arrival_rate") & (hourly.horizon_s == 60)]
    for model in (chosen, "persistence_interval"):
        g = h[h.model == model]
        ax.plot(g.utc_hour + 0.5, g.mae, marker="o", ms=4, color=MODEL_COLOR[model], label=LABEL[model])
    ax.set_xlim(0, 24)
    ax.set_xticks(range(0, 25, 3))
    ax.set_xlabel("UTC hour of forecast origin (2026-10-12)")
    ax.set_ylabel("MAE (tx/s)")
    ax.set_title("Where forecasts fail: hourly MAE, arrival rate, H = 60 s")
    ax.legend()
    fc.save_figure(fig, "fc_06_error_by_hour_arrival_H60.png")

    # fc_07 gas demand actual vs forecast (failure case)
    g = load("F", "gas_demand_rate", 60)
    gsel = sel[("gas_demand_rate", 60)]
    gh = g["origin_s"] / 3600
    fig, ax = plt.subplots(figsize=(11, 3.8))
    ax.plot(gh, g["y_observed"] / 1e6, color="#9a9893", lw=0.9, label="observed 60-s mean")
    ax.plot(gh, g[f"yhat_{gsel}"] / 1e6, color=MODEL_COLOR.get(gsel, fc.SERIES[3]), lw=1.6, label=f"selected: {LABEL.get(gsel, gsel)}")
    ax.set_yscale("log")
    ax.set_xlim(0, 24)
    ax.set_xlabel("Forecast origin (UTC hour, 2026-10-12)")
    ax.set_ylabel("Declared gas demand (Mgas/s, log)")
    ax.set_title("Gas demand, H = 60 s: dominated by isolated large-gas-limit transactions that no baseline anticipates")
    ax.legend(loc="upper right", ncol=2)
    fc.save_figure(fig, "fc_07_actual_vs_forecast_gas_H60.png")

    # fc_08 error by regime
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    r = regimes[(regimes.target == "arrival_rate") & (regimes.horizon_s == 60)]
    order = ["calm", "typical", "busy"]
    for i, model in enumerate((chosen, "persistence_interval")):
        g = r[r.model == model].set_index("regime").loc[order]
        xs = np.arange(3) + (i - 0.5) * 0.4
        ax.bar(xs, g["mae"], width=0.38, color=MODEL_COLOR[model], label=LABEL[model])
        for x, val, n in zip(xs, g["mae"], g["n"]):
            ax.text(x, val + 0.02, f"{val:.2f}", ha="center", fontsize=8, color=fc.TEXT2)
    ax.set_xticks(range(3), [f"{o}\n(n={int(r[(r.model == chosen) & (r.regime == o)]['n'].iloc[0])})" for o in order])
    ax.set_ylabel("MAE (tx/s)")
    ax.set_title("Error by workload regime (trailing-300-s rate terciles fixed on 21 Sep), H = 60 s")
    ax.legend()
    fc.save_figure(fig, "fc_08_error_by_regime_arrival_H60.png")


if __name__ == "__main__":
    main()
