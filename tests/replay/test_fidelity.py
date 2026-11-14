"""Integrity checks must pass on faithful schedules and fail on each kind of tampering (synthetic data only)."""

from __future__ import annotations

import numpy as np
import pytest

from fabric_perf.replay import fidelity as f
from fabric_perf.replay import schedules as s
from fabric_perf.replay.windows import Window


def _window(offsets, length_s=60):
    return Window("d", 100, length_s, 60, np.asarray(offsets, dtype=np.int64), np.empty(0, dtype=np.int64))


SOURCE = [0, 0, 3, 3, 3, 900, 1000, 1000, 5000, 59_999]


def test_identity_schedule_passes_every_check():
    win = _window(SOURCE)
    report = f.observed_schedule_integrity(win, s.observed_schedule(win))
    assert report.ok, report.render()


@pytest.mark.parametrize("name,mutate,failing", [
    ("dropped_tie", lambda a: np.delete(a, 1), "count_conserved"),
    ("extra_arrival", lambda a: np.sort(np.append(a, 77)), "count_conserved"),
    ("shifted_one_ms", lambda a: a + np.eye(len(a), dtype=np.int64)[4], "max_abs_offset_difference_ms"),
    ("smoothed_ties", lambda a: np.where(np.diff(a, prepend=-1) == 0, a + 1, a), "zero_gaps_preserved"),
    ("reordered", lambda a: a[::-1].copy(), "negative_gaps"),
    ("outside_window", lambda a: np.append(a[:-1], 60_000), "offsets_within_window"),
])
def test_tampering_is_detected(name, mutate, failing):
    win = _window(SOURCE)
    report = f.observed_schedule_integrity(win, mutate(s.observed_schedule(win)))
    assert not report.ok
    assert failing in [c.name for c in report.failures], (name, report.render())


def test_generated_schedule_integrity_passes_and_detects_problems():
    rates = np.array([0.0, 20.0, 5.0, 0.0])
    schedule = s.poisson_schedule(rates, 15, seed=8)
    assert f.generated_schedule_integrity(schedule, rates, 15).ok
    injected = np.sort(np.append(schedule, 3_000))  # inside the zero-rate first interval
    assert "arrivals_in_zero_rate_intervals" in [c.name for c in f.generated_schedule_integrity(injected, rates, 15).failures]
    assert "volume_within_5_sigma_of_intensity" in [c.name for c in f.generated_schedule_integrity(schedule[:10], rates, 15).failures]
    assert "negative_gaps" in [c.name for c in f.generated_schedule_integrity(schedule[::-1].copy(), rates, 15).failures]


def test_rate_table_keeps_four_rates_separate_and_never_invents_delivery():
    win = _window(np.repeat(np.arange(60) * 1000 + 100, 3))  # 3 tx per second, observed
    schedule = np.repeat(np.arange(60) * 1000 + 200, 2)  # offered: 2 per second
    table = f.rate_table(win, schedule, forecast_rates=np.array([2.0, 4.0, 3.0]), forecast_interval_s=20)
    assert list(table.columns) == ["second", "observed_y", "forecast_yhat", "offered_s", "delivered_d"]
    assert (table["observed_y"] == 3).all() and (table["offered_s"] == 2).all()
    assert table["forecast_yhat"].tolist() == [2.0] * 20 + [4.0] * 20 + [3.0] * 20
    assert table["delivered_d"].isna().all()
    summary = f.rate_summary(table, interval_s=20)
    assert summary["delivered_status"] == "NOT_MEASURED_OFFLINE"
    assert summary["offered_over_observed_volume"] == pytest.approx(2 / 3)
    assert summary["forecast_mae_vs_interval_mean"] == pytest.approx((1 + 1 + 0) / 3)
    assert summary["forecast_bias_vs_interval_mean"] == pytest.approx((-1 + 1 + 0) / 3)


def test_rate_table_without_forecast_has_nan_forecast_and_requires_exact_tiling():
    win = _window([0, 1000])
    table = f.rate_table(win, np.array([0, 1000]))
    assert table["forecast_yhat"].isna().all()
    assert "forecast_mae_vs_interval_mean" not in f.rate_summary(table)
    with pytest.raises(ValueError, match="tile the window"):
        f.rate_table(win, np.array([0]), forecast_rates=np.array([1.0, 2.0]), forecast_interval_s=7)


def test_schedule_descriptors_report_gap_exposure():
    stats = f.schedule_descriptors(np.array([0, 0, 3, 3, 3, 900, 1000]), 60)
    assert stats["n_tx"] == 7
    assert stats["gap_le5ms_share"] == pytest.approx(4 / 6)  # gaps 0,3,0,0,897,100 -> four are <= 5 ms
    assert stats["zero_gap_share"] == pytest.approx(3 / 6)
