"""Local predictor and the live predictive policy: decisions, abstentions, causality, no extrapolation."""
import copy

import pytest

from fabric_perf.controllers.base import FabricConfig

from Fabric_Live_Testbed.closed_loop.telemetry import RoundObservation
from Fabric_Live_Testbed.closed_loop_v2.policies_v2 import PredictiveLivePolicy
from Fabric_Live_Testbed.closed_loop_v2.predictor import LocalSurfacePredictor, OutsideSurface
from Fabric_Live_Testbed.closed_loop_v2 import scaling as S

# synthetic X0 surface: B=10 is best at low load and degrades; B=40 is slow at low load and holds at high load
SURF = []
for rate, m10, m40, f10, f40 in [(10, 500, 1000, 1, 1), (20, 520, 950, 1, 1), (40, 700, 850, 1, 1),
                                 (80, 1800, 800, 0.8, 1), (160, 9000, 900, 0.4, 1)]:
    SURF += [{"B": 10, "rate_tps": rate, "mean_ms": m10, "delivered_fraction": f10},
             {"B": 40, "rate_tps": rate, "mean_ms": m40, "delivered_fraction": f40}]


def cfg(b=10):
    return FabricConfig(max_message_count=b)


def obs(round_index, n_rows, wall1_ms):
    return RoundObservation(round_index=round_index, label=f"r{round_index}", rows=n_rows, success=n_rows, failed=0, other=0,
                            pending_at_deactivate=0, latency_mean_ms=500.0, latency_p50_ms=400.0, latency_p95_ms=900.0,
                            latency_p99_ms=1000.0, latency_max_ms=1200.0, first_create_ms=1, last_final_ms=2, wall0_ms=wall1_ms - 61000,
                            wall1_ms=wall1_ms, wall_minus_monotonic_ms=0.1, csv_sha256="0" * 64, manifest_sha256="0" * 64)


def make_policy(segment_rates, warm_rate=10.0, k=1.0):
    rounds = S.rounds_from_rates(warm_rate * k, [r * k for r in segment_rates])
    pol = PredictiveLivePolicy(
        cfg(10), (10, 40), warmup_counts=S.warmup_counts([warm_rate * k] * 10), round_counts=S.history_by_round(rounds),
        scheduled_n={r.index: len(r.offsets_ms) for r in rounds}, predictor=LocalSurfacePredictor(SURF))
    return pol, rounds


T0 = 1_800_000_000_000


# ------------------------------------------------------------------------------------------------ predictor
def test_surface_interpolates_in_log_rate_and_refuses_to_extrapolate():
    p = LocalSurfacePredictor(SURF)
    a = p.predict(20.0, cfg(10))
    assert a.latency_ms == pytest.approx(520) and a.throughput_tps == pytest.approx(20)
    mid = p.predict(28.284271, cfg(10))                     # geometric midpoint of 20 and 40
    assert mid.latency_ms == pytest.approx((520 + 700) / 2, rel=1e-3)
    for rate in (5.0, 161.0, 0.0, float("nan")):
        with pytest.raises(OutsideSurface):
            p.predict(rate, cfg(10))
    with pytest.raises(OutsideSurface):
        p.predict(20.0, cfg(10).__class__(max_message_count=20))      # B never measured


def test_delivered_fraction_deadband_and_backlog():
    p = LocalSurfacePredictor(SURF)
    assert p.predict(10.0, cfg(10)).throughput_tps == pytest.approx(10.0)       # fraction 1.0
    assert p.predict(80.0, cfg(10)).throughput_tps == pytest.approx(64.0)       # 0.8 delivered: a backlog the utility penalizes
    assert p.predict(80.0, cfg(40)).throughput_tps == pytest.approx(80.0)


# ------------------------------------------------------------------------------------------------ policy decisions
def run_decisions(pol, rounds, current=None, n=None):
    cur, out = current or cfg(10), []
    for d in range(n or len(rounds) - 1):
        r = pol.decide(d, obs(d, len(rounds[d].offsets_ms), T0 + 61_000 * (d + 1)), cur, 0.5)
        out.append(r)
        if r["changed"]:
            cur = cfg(r["target_B"])
            pol.applied(cur)
    return out


def test_low_load_holds_b10_and_high_load_selects_b40_then_cooldown_applies():
    pol, rounds = make_policy([10.0] * 10)
    out = run_decisions(pol, rounds)
    assert {r["target_B"] for r in out} == {10} and not any(r["abstained"] for r in out)
    assert all(r["controller_metadata"]["forecast_tps"] == pytest.approx(10.0, rel=0.01) for r in out)
    pol, rounds = make_policy([80.0] * 10, warm_rate=80.0)
    out = run_decisions(pol, rounds)
    assert out[0]["target_B"] == 40 and out[0]["changed"] and out[0]["reason"] == "predicted_utility_improvement"
    assert all(r["target_B"] == 40 for r in out)
    meta = out[0]["controller_metadata"]
    assert meta["history_seconds_used"] == 660 and meta["forecast_model"].startswith("ewma_alpha_0.005")
    assert {c["config"]["max_message_count"] for c in meta["candidate_utilities"]} == {10, 40}


def test_decision_never_depends_on_rounds_after_the_decision():
    pol, rounds = make_policy([10, 10, 10, 80, 80, 80, 10, 10, 10, 10.0], warm_rate=10.0)
    base = run_decisions(pol, rounds, n=3)
    pol2, rounds2 = make_policy([10, 10, 10, 80, 80, 80, 10, 10, 10, 10.0], warm_rate=10.0)
    # replace every round after round 2 with a wildly different future (counts and scheduled n stay consistent)
    for idx in range(3, len(rounds2)):
        pol2.round_counts[idx] = [500] * 60
        pol2.scheduled_n[idx] = 30000
    changed = run_decisions(pol2, rounds2, n=3)
    assert [r["target_B"] for r in base] == [r["target_B"] for r in changed]
    assert [r["controller_metadata"]["forecast_tps"] for r in base] == [r["controller_metadata"]["forecast_tps"] for r in changed]
    assert [r["controller_metadata"]["history_seconds_used"] for r in base] == [660, 720, 780]


def test_history_before_decision_is_warmup_plus_finished_rounds_only():
    pol, rounds = make_policy([10.0] * 10)
    for d in (0, 4, 9):
        h = pol.history_before_decision(d)
        assert len(h) == 600 + 60 * (d + 1)
        assert h[-60:] == [float(v) for v in pol.round_counts[d]]


def test_policy_abstains_when_observation_is_missing_or_differs_from_the_schedule():
    pol, rounds = make_policy([10.0] * 10)
    r = pol.decide(0, None, cfg(10), None)
    assert r["abstained"] and r["target_B"] == 10 and r["reason"].endswith("fail_safe")
    pol, rounds = make_policy([10.0] * 10)
    r = pol.decide(0, obs(0, len(rounds[0].offsets_ms) - 1, T0 + 61_000), cfg(10), 0.5)
    assert r["abstained"] and r["reason"] == "observed_arrivals_differ_from_schedule_fail_safe" and not r["changed"]


def test_policy_abstains_outside_the_measured_surface_instead_of_extrapolating():
    pol, rounds = make_policy([200.0] * 10, warm_rate=200.0)       # forecast ~200 tx/s > 160, the top of the surface
    r = pol.decide(0, obs(0, len(rounds[0].offsets_ms), T0 + 61_000), cfg(10), 0.5)
    assert r["abstained"] and r["reason"] == "prediction_failure_fail_safe" and not r["changed"]
    assert "outside the measured range" in r["controller_metadata"]["error"]


def test_policy_needs_the_frozen_minimum_history():
    from Fabric_Live_Testbed.closed_loop.policies import PolicyError
    rounds = S.rounds_from_rates(10.0, [10.0] * 10)
    with pytest.raises(PolicyError):
        PredictiveLivePolicy(cfg(10), (10, 40), warmup_counts=[10] * 599, round_counts=S.history_by_round(rounds),
                             scheduled_n={r.index: len(r.offsets_ms) for r in rounds}, predictor=LocalSurfacePredictor(SURF))


def test_counts_that_do_not_sum_to_the_schedule_are_refused():
    from Fabric_Live_Testbed.closed_loop.policies import PolicyError
    rounds = S.rounds_from_rates(10.0, [10.0] * 10)
    n = {r.index: len(r.offsets_ms) for r in rounds}
    n[3] += 1
    with pytest.raises(PolicyError):
        PredictiveLivePolicy(cfg(10), (10, 40), warmup_counts=S.warmup_counts([10.0] * 10), round_counts=S.history_by_round(rounds),
                             scheduled_n=n, predictor=LocalSurfacePredictor(SURF))
