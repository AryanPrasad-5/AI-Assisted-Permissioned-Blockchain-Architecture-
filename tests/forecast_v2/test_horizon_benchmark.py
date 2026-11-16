"""Leakage and correctness tests for the horizon-mean benchmark. Synthetic series only; no archive is read."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from scipy import stats

from fabric_perf.forecasting import horizon_benchmark as hb

ALPHA = 0.05


def bursty(n: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    mean = 10 + 6 * np.sin(np.arange(n) / 2500.0) + np.where(rng.random(n) < 0.002, 40, 0)
    return rng.negative_binomial(3, 3 / (3 + mean)).astype(float)


def reference(model: str, history, horizon: int, alpha: float | None = None) -> float:
    """Independent per-origin implementation: it can only see the slice it is handed."""
    h = [float(x) for x in history]
    if model == "last_1s":
        return h[-1]
    if model == "persistence_interval":
        return sum(h[-horizon:]) / horizon
    if model.startswith("rolling_mean_"):
        w = int(model.rsplit("_", 1)[1])
        return sum(h[-w:]) / w
    if model.startswith("seasonal_naive_"):
        p = int(model.rsplit("_", 1)[1])
        seg = h[len(h) - p: len(h) - p + horizon]
        return sum(seg) / horizon
    if model == "expanding_mean":
        return sum(h) / len(h)
    if model == "ewma":
        e = h[0]
        for x in h[1:]:
            e = alpha * x + (1 - alpha) * e
        return e
    raise AssertionError(model)


def run(model, counts, horizon):
    return hb.forecast_series(model, counts, horizon, ALPHA if model == "ewma" else None)


@pytest.mark.parametrize("horizon", [60, 10])
@pytest.mark.parametrize("model", hb.MODELS)
def test_matches_the_independent_per_origin_reference(model, horizon):
    counts = bursty(1600, seed=1)
    full = run(model, counts, horizon)
    for t in (400, 555, 700, 1000, 1337, 1600):
        assert full[t] == pytest.approx(reference(model, counts[:t], horizon, ALPHA), rel=1e-12, abs=1e-12), (model, t)


@pytest.mark.parametrize("horizon", [60, 10])
@pytest.mark.parametrize("model", hb.MODELS)
def test_prefix_invariance(model, horizon):
    """A forecast made at origin t must not change when the series is cut at t."""
    counts = bursty(1600, seed=2)
    full = run(model, counts, horizon)
    for cut in (650, 900, 1250):
        prefix = run(model, counts[:cut], horizon)
        np.testing.assert_allclose(full[: cut + 1], prefix[: cut + 1], equal_nan=True, rtol=0, atol=1e-12)


@pytest.mark.parametrize("horizon", [60, 10])
@pytest.mark.parametrize("model", hb.MODELS)
def test_future_perturbation_invariance(model, horizon):
    """Rewriting everything from t onward (including the target window) must not change the forecast at t."""
    rng = np.random.default_rng(3)
    counts = bursty(1600, seed=4)
    base = run(model, counts, horizon)
    for t in (450, 800, 1111, 1500):
        changed = counts.copy()
        changed[t:] = rng.integers(0, 500, size=len(changed) - t)
        assert run(model, changed, horizon)[t] == base[t], (model, t)


def test_the_invariance_checks_have_power_a_leaky_forecaster_is_caught():
    """Negative control: a forecaster that peeks at the target window must fail both invariance checks."""
    def leaky(counts, horizon=60):
        out = np.full(len(counts) + 1, np.nan)
        cs = np.concatenate([[0.0], np.cumsum(counts)])
        t = np.arange(len(counts) - horizon + 1)
        out[t] = (cs[t + horizon] - cs[t]) / horizon  # the realised target itself
        return out

    counts = bursty(1600, seed=12)
    full, prefix = leaky(counts), leaky(counts[:900])
    assert not np.allclose(full[801:901], prefix[801:901], equal_nan=True)  # prefix invariance breaks near the cut
    changed = counts.copy()
    changed[800:] = 0.0
    assert leaky(changed)[800] != full[800]  # future perturbation changes the forecast at t


def test_target_is_the_mean_of_the_next_h_seconds_and_excludes_the_last_observed_second():
    counts = np.arange(100, dtype=float)
    y = hb.targets(counts, np.array([20, 50]), 10)
    assert y.tolist() == [np.mean(counts[20:30]), np.mean(counts[50:60])]
    assert y[0] != np.mean(counts[19:29])


def test_origin_grids_are_non_overlapping_and_inside_their_pools():
    sizes = {}
    for pool in ("D", "V", "F"):
        for horizon in hb.HORIZONS:
            org = hb.origins(pool, horizon)
            assert (np.diff(org) == horizon).all()  # targets never overlap
            assert org[0] == hb.POOL_START[pool] and org[-1] + horizon <= hb.POOL_END[pool]
            assert org[-1] + 2 * horizon > hb.POOL_END[pool]  # no origin was dropped from the end
            sizes[(pool, horizon)] = len(org)
    assert hb.POOL_START["V"] == 43_200 and hb.POOL_END["D"] == 43_200  # D and V are disjoint
    assert hb.POOL_START["D"] >= hb.MIN_HISTORY_S
    assert sizes[("D", 60)] == 710 and sizes[("V", 60)] == 720 and sizes[("F", 60)] == 1430


@pytest.mark.parametrize("model", hb.MODELS)
def test_lookback_never_reaches_the_origin(model):
    org = hb.origins("V", 60)
    table = hb.lookback_table(model, org, 60)
    assert (table["lookback_end_s"] <= table["origin_s"]).all() and (table["lookback_start_s"] >= 0).all()
    assert (table["target_start_s"] == table["origin_s"]).all()


def test_lookback_table_raises_if_a_model_would_read_at_or_after_the_origin(monkeypatch):
    monkeypatch.setattr(hb, "lookback", lambda model, t, horizon: (t - 5, t + 1))
    with pytest.raises(AssertionError, match="lookback"):
        hb.lookback_table("rolling_mean_60", np.array([700, 760]), 60)


def test_seasonal_naive_cannot_use_a_period_shorter_than_the_horizon():
    with pytest.raises(ValueError, match="at least the horizon"):
        hb.forecast_series("seasonal_naive_60", bursty(300, 1), 120)


def test_regime_label_uses_only_the_past():
    counts = bursty(2000, seed=5)
    org = np.array([700, 900, 1500])
    base = hb.trailing_rate(counts, org)
    changed = counts.copy()
    changed[1500:] += 1000  # rewrite the future of the last origin
    assert hb.trailing_rate(changed, org).tolist() == base.tolist()
    assert hb.trailing_rate(counts, np.array([1000]))[0] == pytest.approx(counts[700:1000].mean())
    cutoffs = hb.regime_cutoffs(np.arange(300, dtype=float))
    assert hb.regime_labels(np.array([10.0, 150.0, 290.0]), cutoffs).tolist() == [0, 1, 2]


def test_alpha_is_tuned_on_d_only_and_ignores_later_data():
    d_counts = bursty(43_200, seed=6)
    tail_a, tail_b = bursty(20_000, seed=7), bursty(20_000, seed=8) + 50
    alpha_a, grid_a = hb.tune_alpha(np.concatenate([d_counts, tail_a]), 60)
    alpha_b, grid_b = hb.tune_alpha(np.concatenate([d_counts, tail_b]), 60)
    assert alpha_a == alpha_b and grid_a.equals(grid_b)
    assert alpha_a == grid_a.loc[grid_a["mae_D"].idxmin(), "alpha"]
    with pytest.raises(ValueError, match="cover the D pool"):
        hb.tune_alpha(d_counts[:40_000], 60)


def test_evaluate_pool_returns_every_model_on_the_same_origins():
    counts = bursty(43_200, seed=9)
    res = hb.evaluate_pool("D", counts, 60, ALPHA)
    assert set(res.yhat) == set(hb.MODELS)
    assert all(len(v) == len(res.org) == len(res.y) for v in res.yhat.values())
    assert not any(np.isnan(v).any() for v in res.yhat.values())
    with pytest.raises(ValueError, match="do not cover"):
        hb.evaluate_pool("V", counts, 60, ALPHA)


def test_selection_rule_breaks_near_ties_toward_the_simpler_model():
    mae = {"last_1s": 6.0, "persistence_interval": 4.60, "rolling_mean_60": 4.55, "ewma": 4.50, "seasonal_naive_60": 5.0}
    assert hb.select_model(mae) == "ewma"  # the 1% band is 4.50 * 1.01 = 4.545; 4.55 and 4.60 just miss it
    mae["persistence_interval"] = 4.53
    assert hb.select_model(mae) == "persistence_interval"  # 4.53 <= 4.50 * 1.01 = 4.545 and simpler than ewma
    mae["persistence_interval"] = 4.60
    mae["rolling_mean_60"] = 4.54
    assert hb.select_model(mae) == "rolling_mean_60"  # simpler than ewma, within 1%
    assert hb.select_model({"ewma": 4.0, "rolling_mean_60": 5.0}) == "ewma"  # clear winner needs no tie-break


def test_score_and_threshold_counts():
    y, yhat = np.array([2.0, 10.0, 25.0, 30.0]), np.array([3.0, 8.0, 20.0, 10.0])
    s = hb.score(y, yhat)
    assert s["mae"] == pytest.approx((1 + 2 + 5 + 20) / 4) and s["bias"] == pytest.approx((1 - 2 - 5 - 20) / 4)
    assert s["rmse"] == pytest.approx(np.sqrt((1 + 4 + 25 + 400) / 4)) and s["wape"] == pytest.approx(28 / 67)
    c = hb.threshold_counts(y, yhat, 19.5)
    assert (c["n_above"], c["tp"], c["fp"], c["fn"]) == (2, 1, 0, 1) and c["agreement"] == pytest.approx(0.75)


def test_dm_identical_losses_and_clear_difference():
    rng = np.random.default_rng(10)
    loss = np.abs(rng.normal(size=500))
    same = hb.dm_test(loss, loss.copy())
    assert same["identical"] and same["p"] == 1.0
    better = hb.dm_test(loss, loss + 1.0 + 0.1 * rng.normal(size=500))
    assert better["mean_diff"] < 0 and better["p"] < 1e-6  # negative mean_diff favours the first argument
    worse = hb.dm_test(loss + 1.0 + 0.1 * rng.normal(size=500), loss)
    assert worse["mean_diff"] > 0 and worse["stat"] > 0


def test_dm_hac_is_more_conservative_than_an_iid_test_under_autocorrelation():
    rng = np.random.default_rng(11)
    d = np.zeros(2000)
    for k in range(1, len(d)):
        d[k] = 0.8 * d[k - 1] + rng.normal()
    d += 0.15
    result = hb.dm_test(d, np.zeros_like(d))
    iid_p = stats.ttest_1samp(d, 0.0).pvalue
    assert result["p"] > iid_p


def test_holm_matches_the_hand_calculation():
    assert hb.holm([0.01, 0.04, 0.03]) == pytest.approx([0.03, 0.06, 0.06])
    assert hb.holm([0.5]) == [0.5] and max(hb.holm([0.4, 0.9, 0.7])) <= 1.0


def test_frame_types():
    assert isinstance(hb.lookback_table("ewma", hb.origins("D", 60), 60), pd.DataFrame)
