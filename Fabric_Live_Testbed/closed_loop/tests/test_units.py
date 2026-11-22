import datetime as dt

import pytest

from Fabric_Live_Testbed.closed_loop.segments import SegmentError, build_rounds, schedule_text
from Fabric_Live_Testbed.closed_loop.policies import PolicyError, PredictivePolicy, ReactivePolicy, StaticPolicy, config_with_b
from Fabric_Live_Testbed.closed_loop.telemetry import RoundObservation, nearest_rank
from fabric_perf.controllers.base import FabricConfig

B10 = FabricConfig(10, 2000, 10485760, 103809024, 3)


def obs(i, mean, wall1=None):
    w1 = wall1 or (1_700_000_000_000 + 60_000 * (i + 1))
    return RoundObservation(i, f"r{i}", 100, 100, 0, 0, 0, mean, mean, mean, mean, mean, 0, 0, w1 - 60_000, w1, 0.0, "a", "b")


def test_build_rounds_splits_window_exactly():
    measured = [0, 5, 59_999, 60_000, 60_001, 179_999]
    pre = [-60_000, -1]
    rounds = build_rounds(measured, pre, 180, 60)
    assert [r.name for r in rounds] == ["r00", "r01", "r02", "r03"]
    assert rounds[0].offsets_ms == (0, 59_999) and not rounds[0].measured
    assert rounds[1].offsets_ms == (0, 5, 59_999)
    assert rounds[2].offsets_ms == (0, 1) and rounds[2].window_start_ms == 60_000
    assert rounds[3].offsets_ms == (59_999,)
    assert rounds[1].label("cl01") == "s01-cl01" and rounds[0].label("cl01") == "pr-cl01"


@pytest.mark.parametrize("measured,pre,msg", [
    ([0, 70_000], [-1], "empty round"),          # segment 0 has one, segment 1 has one, ... with 180 s window segment 2 empty
    ([5, 3], [-1], "non-decreasing"),
    ([0, 60_000, 120_000], [], "pre-roll"),
])
def test_build_rounds_refuses_bad_input(measured, pre, msg):
    with pytest.raises(SegmentError, match=msg):
        build_rounds(measured, pre, 180, 60)


def test_schedule_text_is_lf_without_trailing_newline():
    assert schedule_text((0, 5, 10)) == b"0\n5\n10"


def test_nearest_rank():
    assert nearest_rank([1, 2, 3, 4], 0.5) == 2 and nearest_rank([1, 2, 3, 4], 0.95) == 4 and nearest_rank([], 0.5) is None


def test_static_always_holds():
    p = StaticPolicy(B10, (10, 40))
    for i in range(5):
        d = p.decide(i, obs(i, 5000), B10, 0.1)
        assert d["target_B"] == 10 and not d["changed"]


def reactive(high, low):
    return ReactivePolicy(B10, (10, 40), congested_b=40, latency_high_ms=high, latency_low_ms=low, consecutive_samples=2,
                          cooldown_samples=0, max_telemetry_age_s=5.0)


def test_reactive_r1_holds_at_batching_latency_and_switches_only_after_two_high_samples():
    p = reactive(2000, 1600)
    assert not p.decide(0, obs(0, 520), B10, 0.2)["changed"]
    assert not p.decide(1, obs(1, 2100), B10, 0.2)["changed"]
    d = p.decide(2, obs(2, 2100), B10, 0.2)
    assert d["changed"] and d["target_B"] == 40 and d["reason"] == "congestion_threshold_crossed"


def test_reactive_r0_misfires_at_low_load_batching_wait_and_holds_after_applied():
    p = reactive(500, 200)
    assert not p.decide(0, obs(0, 520), B10, 0.2)["changed"]
    d = p.decide(1, obs(1, 530), B10, 0.2)
    assert d["changed"] and d["target_B"] == 40
    b40 = config_with_b(B10, 40)
    p.applied(b40)
    d = p.decide(2, obs(2, 1030), b40, 0.2)
    assert not d["changed"] and d["reason"] == "congested_state_hold"


def test_reactive_fail_safe_on_stale_missing_or_unconfirmed_telemetry():
    p = reactive(500, 200)
    assert p.decide(0, obs(0, 900), B10, 10.0)["reason"] == "stale_telemetry_fail_safe"
    assert p.decide(1, None, B10, None)["abstained"]
    d = p.decide(2, obs(2, 900), config_with_b(B10, 40), 0.1)       # not confirmed by the actuator
    assert d["reason"] == "configuration_mismatch_fail_safe" and d["target_B"] == 10


def pred_rows(bs=(10, 10, 40)):
    t0 = dt.datetime(2026, 10, 12, 23, tzinfo=dt.timezone.utc)
    return [{"decision_index": i, "decision_ts_utc": (t0 + dt.timedelta(minutes=i)).isoformat(), "predictive_B_stored": b,
             "current_B_before": 10, "predictive_abstained": False, "predictive_cause_stored": "hold_best_is_current",
             "predictive_forecast_tps_stored": "10.2"} for i, b in enumerate(bs)], \
        [(t0 + dt.timedelta(minutes=i)).isoformat() for i in range(len(bs))]


def test_predictive_applies_stored_decisions_and_abstains_when_precondition_fails():
    rows, times = pred_rows()
    p = PredictivePolicy(B10, (10, 40), stored_rows=rows, decision_times_utc=times)
    assert p.decide(0, None, B10, None)["target_B"] == 10
    d = p.decide(2, None, B10, None)
    assert d["changed"] and d["target_B"] == 40
    d = p.decide(1, None, config_with_b(B10, 40), None)
    assert d["abstained"] and d["reason"] == "stored_decision_precondition_failed_hold" and not d["changed"]


def test_predictive_refuses_misaligned_table():
    rows, times = pred_rows()
    with pytest.raises(PolicyError, match="is for"):
        PredictivePolicy(B10, (10, 40), stored_rows=rows, decision_times_utc=times[1:] + ["2026-10-12T23:05:00+00:00"])
    rows[1]["predictive_B_stored"] = 45
    with pytest.raises(PolicyError, match="outside the menu"):
        PredictivePolicy(B10, (10, 40), stored_rows=rows, decision_times_utc=times)
