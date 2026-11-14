"""The law, the policies and the exposure tiers (synthetic rates; the law is pinned to the frozen P1b Tier 1 predictions)."""

from __future__ import annotations

import numpy as np
import pytest

from fabric_perf.forecasting import law_voi as lv

# (B, rate tx/s, predicted p95 ms) from the frozen P1b Tier 1 evaluation_summary.md (cp01..cp10)
TIER1 = [(40, 15, 1940), (10, 15, 625), (40, 20, 1926), (10, 20, 475), (40, 25, 1585),
         (10, 25, 385), (10, 30, 325), (40, 30, 1325), (40, 40, 1000), (10, 40, 250)]


@pytest.mark.parametrize("batch,rate,predicted_ms", TIER1)
def test_law_reproduces_the_ten_frozen_tier1_predictions(batch, rate, predicted_ms):
    assert float(lv.law_p95_s(np.array([rate]), batch)[0]) * 1000 == pytest.approx(predicted_ms, abs=1e-6)


def test_no_traffic_has_no_latency():
    assert np.isnan(lv.law_p95_s(np.array([0.0, -1.0]), 10)).all()


def test_under_the_registered_law_b10_weakly_dominates_b40_at_every_rate():
    """A property of the law, checked on a fine grid: tie where both are timer-cut, strictly better above 4.5 tx/s."""
    rates = np.linspace(0.01, 800, 800_000)
    p10, p40 = lv.law_p95_s(rates, 10), lv.law_p95_s(rates, 40)
    assert (p40 - p10).min() >= -1e-12
    tie = rates <= 4.5
    assert np.allclose(p10[tie], p40[tie], atol=1e-12)
    assert (p40 - p10)[rates > 4.5 + 1e-6].min() > 0
    validated = (rates >= 10) & (rates <= 40)
    assert (p40 - p10)[validated].min() >= 0.75 - 1e-9  # smallest validated-range gap is at 40 tx/s: 0.75 s, ~19x the law tolerance


def test_oracle_headroom_over_best_static_is_exactly_zero_on_this_menu_and_over_b40_is_large():
    y = np.array([3.0, 8.0, 15.0, 30.0, 4.6, 55.0, 0.0])
    s = lv.summarize(y, 60, yhat=y)
    assert s["best_static"] == "static_B10"
    assert s["oracle_headroom_over_best_static_s"] == pytest.approx(0.0, abs=1e-12)
    assert s["oracle_headroom_over_B40_s"] > 0.3
    assert s["forecast_regret_vs_oracle_s"] == pytest.approx(0.0, abs=1e-12) and s["selection_errors"] == 0
    assert s["share_volume_oracle_chooses_B10"] == pytest.approx(1.0)
    assert (lv.choose(np.array([0.5, 4.6, 12.0, 90.0, -3.0])) == 0).all()  # any forecast, including nonsense, picks B=10


def test_regret_and_selection_error_machinery_on_a_toy_law_where_the_optimum_switches(monkeypatch):
    def toy(rate, batch, tau=2.0):
        rate = np.asarray(rate, float)
        p = np.where(rate > 20, 2.0, 1.0) if batch == 10 else np.full(rate.shape, 1.5)
        return np.where(rate > 0, p, np.nan)

    monkeypatch.setattr(lv, "law_p95_s", toy)
    y, yhat = np.array([10.0, 30.0]), np.array([30.0, 10.0])  # both forecasts on the wrong side of the switch at 20
    s = lv.summarize(y, 60, yhat=yhat)
    assert s["static_B10"] == pytest.approx((1.0 * 600 + 2.0 * 1800) / 2400) and s["best_static"] == "static_B40"
    assert s["oracle_by_interval"] == pytest.approx((1.0 * 600 + 1.5 * 1800) / 2400)
    assert s["selection_errors"] == 2 and s["selection_error_rate"] == pytest.approx(1.0)
    assert s["forecast_regret_vs_oracle_s"] == pytest.approx(0.5)
    assert s["oracle_headroom_over_best_static_s"] == pytest.approx(1.5 - (1.0 * 600 + 1.5 * 1800) / 2400)
    assert s["share_intervals_headroom_ge_sesoi"] == pytest.approx(0.5) and s["share_volume_headroom_ge_sesoi"] == pytest.approx(0.25)
    right = lv.summarize(y, 60, yhat=y)
    assert right["selection_errors"] == 0 and right["forecast_regret_vs_oracle_s"] == pytest.approx(0.0)


def test_volume_weighting_uses_the_transactions_in_each_interval():
    y = np.array([20.0, 40.0])
    s = lv.summarize(y, 10)
    w = np.array([200.0, 400.0])
    expected = (lv.law_p95_s(y[:1], 10)[0] * w[0] + lv.law_p95_s(y[1:], 10)[0] * w[1]) / w.sum()
    assert s["static_B10"] == pytest.approx(expected)
    assert s["volume_tx"] == 600.0


def test_exposure_tiers_partition_the_volume_and_label_extrapolation():
    y = np.array([2.0, 9.99, 10.0, 25.0, 40.0, 40.01, 90.0, 0.0])
    t = lv.tiered(y, 60, yhat=y)
    parts = t[t["tier"] != "all"]
    assert parts["share_volume"].sum() == pytest.approx(1.0) and parts["share_intervals"].sum() == pytest.approx(1.0)
    assert set(parts["tier"]) == set(lv.TIER_NAMES.values())
    assert lv.tier(np.array([9.99, 10.0, 40.0, 40.01])).tolist() == [0, 1, 1, 2]
    assert t.loc[t["tier"] == "validated_10_to_40", "intervals"].iloc[0] == 3


def test_indeterminate_share_is_the_volume_where_the_two_settings_are_within_twice_the_law_tolerance():
    y = np.array([4.6, 30.0])  # gap at 4.6 tx/s is ~43 ms (< 80 ms); at 30 tx/s it is 1.0 s
    s = lv.summarize(y, 60)
    assert s["share_volume_indeterminate_within_2x_law_tolerance"] == pytest.approx(276 / (276 + 1800))
    assert s["share_intervals_headroom_ge_sesoi"] == 0.0  # B=10 is both best static and oracle, so no headroom
