"""Unit tests: causal EWMA, rate-trace scaling, protocol rules (crossover, k, fallback, reactive thresholds)."""
import random

import numpy as np
import pytest

from Fabric_Live_Testbed.closed_loop_v2 import protocol as P
from Fabric_Live_Testbed.closed_loop_v2 import scaling as S
from Fabric_Live_Testbed.closed_loop_v2.forecast import CausalEwma, ForecastError, ewma_last

from fabric_perf.forecasting.horizon_benchmark import ewma_states  # the frozen recurrence (numpy)


# ------------------------------------------------------------------------------------------------ forecast
def test_ewma_equals_the_frozen_recurrence_exactly():
    rng = random.Random(1)
    counts = [float(rng.randint(0, 40)) for _ in range(3000)]
    for n in (600, 601, 1234, 3000):
        assert abs(ewma_last(counts[:n]) - float(ewma_states(np.array(counts[:n]), 0.005)[-1])) < 1e-12


def test_forecast_uses_only_the_history_it_is_given():
    f = CausalEwma()
    base = [float(i % 17) for i in range(700)]
    a = f.predict(base, 60)
    # anything that exists after the origin cannot be seen: the function has no other input
    assert a == f.predict(list(base), 60)
    assert f.predict(base + [1e6], 60) != a            # a later value does change a LATER forecast, as it must


def test_forecast_refusals():
    f = CausalEwma()
    with pytest.raises(ForecastError):
        f.predict([5.0] * 599, 60)                      # frozen minimum history 600 s
    with pytest.raises(ForecastError):
        f.predict([5.0] * 700, 10)                      # frozen horizon 60 s only
    with pytest.raises(ForecastError):
        f.predict([5.0] * 699 + [-1.0], 60)
    with pytest.raises(ForecastError):
        CausalEwma(alpha=0.01)
    assert f.predict([7.0] * 700, 60) == pytest.approx(7.0)


# ------------------------------------------------------------------------------------------------ scaling
def test_evenly_spaced_round_has_exact_count_and_is_deterministic():
    for n in (1, 7, 60, 600, 1234):
        o = S.evenly_spaced(n)
        assert len(o) == n and o == S.evenly_spaced(n) and 0 <= o[0] and o[-1] < 60_000
        assert all(b >= a for a, b in zip(o, o[1:]))


def test_rounds_from_rates_preserve_the_ratios_between_minutes():
    rates = [8.0, 12.0, 24.0, 16.0]
    rounds = S.rounds_from_rates(10.0, [3 * r for r in rates])
    assert [len(r.offsets_ms) for r in rounds] == [600, 1440, 2160, 4320, 2880]
    assert [r.index for r in rounds] == [0, 1, 2, 3, 4] and not rounds[0].measured and rounds[1].measured
    ratios = [len(rounds[i + 1].offsets_ms) / len(rounds[1].offsets_ms) for i in range(4)]
    assert ratios == pytest.approx([r / rates[0] for r in rates])


def test_counts_per_second_and_warmup_history():
    r = S.rounds_from_rates(10.0, [20.0])[1]
    c = S.counts_per_second(r.offsets_ms)
    assert len(c) == 60 and sum(c) == 1200 and set(c) == {20}
    w = S.warmup_counts([5.0] * 10)
    assert len(w) == 600 and sum(w) == 10 * 300
    h = S.history_by_round(S.rounds_from_rates(10.0, [20.0, 30.0]))
    assert sorted(h) == [0, 1, 2] and sum(h[2]) == 1800


# ------------------------------------------------------------------------------------------------ protocol
def rung(b, rate, p95, *, valid=True, reps=None, failed=False):
    return {"B": b, "rate_tps": float(rate), "p95_ms": p95, "p95_reps": reps or [p95, p95], "mean_ms": p95 * 0.6,
            "client_valid": valid, "failed_exceeds": failed}


def table(p10, p40, flags=None):
    flags = flags or {}
    rates = P.X0_RUNG_RATES_TPS
    rows = []
    for r, a, b in zip(rates, p10, p40):
        rows += [rung(10, r, a, **flags.get(("10", r), {})), rung(40, r, b, **flags.get(("40", r), {}))]
    return rows


LOW10 = [900, 900, 950, 1000, 1100, 1300, 2500, 9000, 20000]          # B=10 degrades at high load
FLAT40 = [2000, 1950, 1900, 1800, 1700, 1600, 1500, 1500, 1500]


def test_sesoi_is_relative_with_a_floor():
    assert P.sesoi_ms(926, 1932) == pytest.approx(92.6)
    assert P.sesoi_ms(100, 400) == 25.0


def test_crossover_detected_with_both_sides_and_persistence():
    res = P.analyse_crossover(table(LOW10, FLAT40))
    assert res["outcome"] == "CROSSOVER" and res["r_c"] == 130 and res["r_max"] == 200
    assert res["detail"]["per_rung"][10] == "B10_BETTER"


def test_no_crossover_when_b10_wins_or_ties_everywhere():
    res = P.analyse_crossover(table([900] * 9, [1900] * 9))
    assert res["outcome"] == "NO_CROSSOVER_WITHIN_CLIENT_CAPACITY" and res["r_c"] is None


def test_single_rung_win_is_not_a_crossover():
    p10 = [900] * 8 + [20000]
    res = P.analyse_crossover(table(p10, [1900] * 9))
    assert res["outcome"] == "NOT_ESTABLISHED"          # one B40 rung: needs CONFIRM_RUNGS=2 consecutive


def test_replicate_sign_disagreement_blocks_a_win():
    rows = table(LOW10, FLAT40)
    for r in rows:
        if r["B"] == 10 and r["rate_tps"] in (160.0, 200.0):
            r["p95_reps"] = [20000, 100]                 # one replicate says B10 is better
    res = P.analyse_crossover(rows)
    assert res["detail"]["per_rung"][160] == "INDISTINGUISHABLE"


def test_client_limited_rungs_cap_r_max_and_hide_the_crossover():
    flags = {("10", 160.0): {"valid": False}, ("10", 200.0): {"valid": False}}
    res = P.analyse_crossover(table(LOW10, FLAT40, flags))
    assert res["r_max"] == 130 and res["outcome"] != "CROSSOVER"
    assert res["r_c"] is None or res["r_c"] <= 130


def test_failures_count_as_a_loss_for_the_failing_configuration():
    flags = {("10", 160.0): {"failed": True}, ("10", 200.0): {"failed": True}}
    p10 = [900, 900, 950, 1000, 1100, 1300, 1400, 1400, 1400]     # latency alone would not show a loss
    res = P.analyse_crossover(table(p10, FLAT40, flags))
    assert res["outcome"] == "CROSSOVER" and res["r_c"] == 160


def test_non_contiguous_invalid_low_rung_stops_r_max():
    flags = {("40", 60.0): {"valid": False}}
    res = P.analyse_crossover(table(LOW10, FLAT40, flags))
    assert res["r_max"] == 40


def test_classify_round_separates_client_from_fabric_saturation():
    ok = dict(scheduled=1000, rows=1000, failed=0, overflow=0)
    assert P.classify_round(lag_p99_ms=12, **ok) == "OK"
    assert P.classify_round(lag_p99_ms=250, **ok) == "CLIENT_LIMITED"           # driver late = client, not Fabric
    assert P.classify_round(lag_p99_ms=12, **{**ok, "overflow": 3}) == "CLIENT_LIMITED"
    assert P.classify_round(lag_p99_ms=None, **ok) == "CLIENT_LIMITED"
    assert P.classify_round(lag_p99_ms=12, **{**ok, "rows": 990}) == "INCOMPLETE"
    assert P.classify_round(lag_p99_ms=12, **{**ok, "failed": 20}) == "FAILED_TX"  # saturation shows as latency/failures, driver on time


def test_choose_k_centres_the_window_on_the_crossover_and_respects_capacity():
    m = [7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 14.0, 17.0, 20.0, 25.0]
    r = P.choose_k(m, r_c=100.0, r_max=200.0)
    assert r["k0"] == pytest.approx(100 / (7 * 25) ** 0.5)
    assert r["k"] == round(min(r["k0"], 0.9 * 200 / 25.0, 25.0), 3) and r["feasible"]
    assert r["segments_below"] >= 2 and r["segments_above"] >= 2
    cap = P.choose_k(m, r_c=100.0, r_max=60.0)          # capacity binds: scaled top segment is 54
    assert cap["k"] == pytest.approx(0.9 * 60 / 25.0, abs=1e-3)
    assert not cap["feasible"] and cap["infeasible_reason"]


def test_crossover_beyond_client_capacity_is_infeasible_for_every_ranked_window():
    ranked = [{"start_s": 600 * i, "segment_rates_tps": [8, 9, 10, 11, 12, 13, 14, 15, 16, 17.0]} for i in range(1, 8)]
    res = P.select_k_over_ranked_windows(ranked, r_c=190.0, r_max=200.0)
    assert res["status"] == "NO_FEASIBLE_WINDOW" and len(res["tried"]) == P.FALLBACK_TOP_N


def test_fallback_uses_the_first_feasible_ranked_window():
    flat = {"start_s": 1, "segment_rates_tps": [10.0] * 10}            # no dynamic range: cannot straddle
    good = {"start_s": 2, "segment_rates_tps": [7.0, 8, 9, 10, 11, 12, 14, 17, 20, 25]}
    res = P.select_k_over_ranked_windows([flat, good], r_c=100.0, r_max=200.0)
    assert res["status"] == "WINDOW_AND_K_SELECTED" and res["window"]["start_s"] == 2 and not res["tried"][0]["feasible"]


def test_reactive_thresholds_come_from_x0_only():
    r = P.reactive_params(1234.0)
    assert r["latency_high_ms"] == 1234.0 and r["latency_low_ms"] == pytest.approx(987.2) and r["consecutive_samples"] == 2
    assert r["congested_B"] == 40 and "decided from X0 only" in r["rule"]


def test_robust_ratio_ignores_a_single_minute_spike():
    assert P.robust_ratio([10, 10, 10, 10, 10, 10, 10, 10, 10, 500.0]) == 1.0
    assert P.robust_ratio([5, 10, 10, 10, 10, 10, 10, 10, 10, 30.0]) == 1.0
    assert P.robust_ratio([5, 6, 10, 10, 10, 10, 10, 10, 12, 30.0]) == pytest.approx(2.0)


def test_b40_better_everywhere_is_not_a_crossover_because_there_is_no_low_load_side():
    res = P.analyse_crossover(table([5000] * 9, [1000] * 9))
    assert res["outcome"] == "NOT_ESTABLISHED" and res["r_c"] == 10
