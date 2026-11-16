"""Orchestration of the P2 benchmark with the freeze enforced in code.

select_run  loads only the 21 Sep series, tunes alpha on D, scores V, freezes the selection (selection_frozen.json).
final_run   loads only the 12 Oct series; refuses unless the frozen file's SHA-256 and the code hashes match what was
            frozen; refuses to run twice (the output directory must not exist). F is therefore opened exactly once.
"""

from __future__ import annotations

import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import scipy

from fabric_perf.replay import trace
from fabric_perf.replay.common import IntegrityError

from . import horizon_benchmark as hb

D_DATE, F_DATE = "2026-09-21", "2026-10-12"
REFERENCE = "persistence_interval"  # skill is reported against interval-level persistence
METHOD = (
    "Target = mean rate over [t, t+H), forecast at origin t from counts[0:t]; origins every H s (non-overlapping). "
    "D (21 Sep 00-12Z) tunes EWMA alpha and regime terciles; V (21 Sep 12-24Z) selects (lowest MAE, models within 1% tied, "
    "simpler wins); F (12 Oct) is scored once in a separate process with the frozen selection. Days are never joined. "
    "Primary H = 60 s, secondary H = 10 s."
)


def _load(series_dir: str | Path, date: str) -> np.ndarray:
    return trace.read_series(series_dir, date, 1).astype(float)


def _code_hashes() -> dict[str, str]:
    return {Path(p).name: trace.sha256_file(p) for p in (hb.__file__, __file__)}


def _tables(res: hb.PoolResult, cutoffs: tuple[float, float]) -> dict[str, pd.DataFrame]:
    labels = hb.regime_labels(res.trailing, cutoffs)
    scores, thresholds, regimes = [], [], []
    for model, yhat in res.yhat.items():
        scores.append({"pool": res.pool, "horizon_s": res.horizon, "model": model, **hb.score(res.y, yhat)})
        for threshold in hb.THRESHOLDS:
            thresholds.append({"pool": res.pool, "horizon_s": res.horizon, "model": model, **hb.threshold_counts(res.y, yhat, threshold)})
        for label, name in enumerate(("calm", "typical", "busy")):
            mask = labels == label
            if mask.any():
                regimes.append({"pool": res.pool, "horizon_s": res.horizon, "model": model, "regime": name,
                                **hb.score(res.y[mask], yhat[mask])})
    scores = pd.DataFrame(scores)
    reference_mae = float(scores.loc[scores["model"] == REFERENCE, "mae"].iloc[0])
    scores["skill_vs_persistence_interval"] = 1.0 - scores["mae"] / reference_mae
    return {"scores": scores, "thresholds": pd.DataFrame(thresholds), "regimes": pd.DataFrame(regimes)}


def _forecast_frame(res: hb.PoolResult, cutoffs: tuple[float, float]) -> pd.DataFrame:
    frame = pd.DataFrame({"origin_s": res.org, "target_end_s": res.org + res.horizon, "y_observed": res.y,
                          "trailing_300s": res.trailing, "regime": hb.regime_labels(res.trailing, cutoffs)})
    for model, yhat in res.yhat.items():
        frame[f"yhat_{model}"] = yhat
    return frame


def _dm_family(res: hb.PoolResult, selected: str) -> pd.DataFrame:
    """Selected model against every other model on absolute error; Holm over the non-identical comparisons."""
    rows = []
    loss_selected = np.abs(res.yhat[selected] - res.y)
    for model, yhat in res.yhat.items():
        if model == selected:
            continue
        rows.append({"pool": res.pool, "horizon_s": res.horizon, "selected": selected, "comparator": model,
                     **hb.dm_test(loss_selected, np.abs(yhat - res.y))})
    frame = pd.DataFrame(rows)
    testable = ~frame["identical"]
    frame["p_holm"] = np.nan
    if testable.any():
        frame.loc[testable, "p_holm"] = hb.holm(frame.loc[testable, "p"].tolist())
    return frame


def select_run(series_dir: str | Path, out_dir: str | Path) -> dict:
    """Tune on D, score D and V, freeze. Loads the 21 Sep series only."""
    out = Path(out_dir) / "select"
    if out.exists():
        raise FileExistsError(f"{out} exists; the selection is frozen once")
    counts = _load(series_dir, D_DATE)
    out.mkdir(parents=True)
    frozen: dict = {"horizons": {}}
    for horizon in hb.HORIZONS:
        alpha, grid = hb.tune_alpha(counts[: hb.POOL_END["D"]], horizon)
        grid.to_csv(out / f"alpha_grid_D_H{horizon}.csv", index=False)
        res_d = hb.evaluate_pool("D", counts[: hb.POOL_END["D"]], horizon, alpha)  # D only
        cutoffs = hb.regime_cutoffs(res_d.trailing)
        res_v = hb.evaluate_pool("V", counts, horizon, alpha)  # forecasts read counts[0:t]; V targets are scored only for selection
        tables_d, tables_v = _tables(res_d, cutoffs), _tables(res_v, cutoffs)
        val_mae = dict(zip(tables_v["scores"]["model"], tables_v["scores"]["mae"]))
        selected = hb.select_model(val_mae)
        dm_v = _dm_family(res_v, selected)
        for name, frame in tables_d.items():
            frame.to_csv(out / f"{name}_D_H{horizon}.csv", index=False)
        for name, frame in tables_v.items():
            frame.to_csv(out / f"{name}_V_H{horizon}.csv", index=False)
        dm_v.to_csv(out / f"dm_V_H{horizon}_descriptive.csv", index=False)
        _forecast_frame(res_v, cutoffs).to_csv(out / f"forecasts_V_H{horizon}.csv", index=False)
        hb.lookback_table(selected, res_v.org, horizon).to_csv(out / f"lookback_selected_V_H{horizon}.csv", index=False)
        frozen["horizons"][str(horizon)] = {
            "alpha": alpha, "regime_cutoffs_trailing_300s": list(cutoffs), "selected_model": selected,
            "validation_mae": val_mae, "origins": {"D": len(res_d.org), "V": len(res_v.org)},
        }
    frozen.update({
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "method": METHOD, "models": list(hb.MODELS), "alpha_grid": list(hb.ALPHA_GRID), "thresholds": list(hb.THRESHOLDS),
        "min_history_s": hb.MIN_HISTORY_S, "near_tie": hb.NEAR_TIE, "simplicity_order": list(hb.SIMPLICITY),
        "series_sha256": {D_DATE: trace.sha256_file(Path(series_dir) / f"series_{D_DATE}_1s.csv.gz")},
        "code_sha256": _code_hashes(),
        "environment": {"python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__, "scipy": scipy.__version__},
    })
    path = out / "selection_frozen.json"
    path.write_text(json.dumps(frozen, indent=2), encoding="utf-8")
    frozen["frozen_file_sha256"] = trace.sha256_file(path)
    return frozen


def final_run(series_dir: str | Path, frozen_path: str | Path, expected_frozen_sha256: str, out_dir: str | Path) -> dict:
    """Score F once with the frozen selection. Loads the 12 Oct series only."""
    if trace.sha256_file(frozen_path) != expected_frozen_sha256.upper():
        raise IntegrityError("frozen selection file SHA-256 differs from the pre-registered value")
    frozen = json.loads(Path(frozen_path).read_text(encoding="utf-8"))
    if _code_hashes() != frozen["code_sha256"]:
        raise IntegrityError("benchmark code changed after the selection was frozen")
    out = Path(out_dir) / "final"
    if out.exists():
        raise FileExistsError(f"{out} exists; F is opened exactly once")
    counts = _load(series_dir, F_DATE)
    out.mkdir(parents=True)
    report: dict = {"horizons": {}, "frozen_file_sha256": expected_frozen_sha256.upper(),
                    "series_sha256": {F_DATE: trace.sha256_file(Path(series_dir) / f"series_{F_DATE}_1s.csv.gz")},
                    "code_sha256": _code_hashes(), "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    for horizon in hb.HORIZONS:
        info = frozen["horizons"][str(horizon)]
        cutoffs = tuple(info["regime_cutoffs_trailing_300s"])
        res_f = hb.evaluate_pool("F", counts, horizon, info["alpha"])
        tables = _tables(res_f, cutoffs)
        dm = _dm_family(res_f, info["selected_model"])
        for name, frame in tables.items():
            frame.to_csv(out / f"{name}_F_H{horizon}.csv", index=False)
        dm.to_csv(out / f"dm_F_H{horizon}.csv", index=False)
        _forecast_frame(res_f, cutoffs).to_csv(out / f"forecasts_F_H{horizon}.csv", index=False)
        hb.lookback_table(info["selected_model"], res_f.org, horizon).to_csv(out / f"lookback_selected_F_H{horizon}.csv", index=False)
        selected_row = tables["scores"].set_index("model").loc[info["selected_model"]]
        report["horizons"][str(horizon)] = {
            "selected_model": info["selected_model"], "alpha": info["alpha"], "origins_F": len(res_f.org),
            "mae_F_selected": float(selected_row["mae"]), "mae_V_selected": float(info["validation_mae"][info["selected_model"]]),
            "skill_vs_persistence_interval_F": float(selected_row["skill_vs_persistence_interval"]),
        }
    (out / "final_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    report["final_report_sha256"] = trace.sha256_file(out / "final_report.json")
    return report
