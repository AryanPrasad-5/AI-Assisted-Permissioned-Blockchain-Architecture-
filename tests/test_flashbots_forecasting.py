import numpy as np
import pandas as pd
import zipfile

from fabric_perf.forecasting.flashbots import build_daily_series, evaluate_baselines


def test_baseline_evaluation_uses_identical_origins_and_reports_metrics():
    values = np.array([0, 1, 2, 0, 3, 4, 0, 1] * 100, dtype=float)
    frame = pd.DataFrame({"arrival_count": values})
    result = evaluate_baselines(frame, 1, 5, "within_day", burst_threshold=3, eval_start=300)
    assert set(result["baseline"]) == {
        "persistence", "rolling_mean_60s", "ewma_alpha_0.2", "ewma_alpha_0.5",
        "seasonal_naive_60s", "seasonal_naive_300s", "poisson_rate_300s",
    }
    assert result.loc[result["regime"] == "normal", "forecast_origins"].nunique() == 1
    assert result.loc[result["regime"] == "normal", "mae"].notna().all()
    assert result.loc[result["regime"] == "normal", "rmse"].notna().all()


def test_baseline_forecasts_do_not_use_future_values():
    values = np.ones(400, dtype=float)
    values[301:] = 1000
    frame = pd.DataFrame({"arrival_count": values})
    result = evaluate_baselines(frame, 1, 5, "within_day", burst_threshold=10, eval_start=300)
    persistence = result[(result.baseline == "persistence") & (result.regime == "normal")].iloc[0]
    assert persistence["mae"] > 0


def test_daily_series_reindexes_zero_arrival_bins(tmp_path):
    csv_path = tmp_path / "2026-01-01.csv"
    csv_path.write_text(
        "timestamp_ms,hash\n"
        "1767225600123,a\n"
        "1767225600789,b\n"
        "1767225605123,c\n",
        encoding="utf-8",
    )
    archive_path = tmp_path / "2026-01-01.csv.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(csv_path, arcname=csv_path.name)
    frame, audit = build_daily_series(archive_path, 1)
    assert len(frame) == 86400
    assert frame["arrival_count"].sum() == 3
    assert frame.loc[0, "arrival_count"] == 2
    assert frame.loc[1, "arrival_count"] == 0
    assert audit["zero_arrival_bins"] == 86398
