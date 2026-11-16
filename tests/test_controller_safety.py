"""Deterministic unit cases; these fixtures are not historical workloads."""

from dataclasses import replace

import pytest

from fabric_perf.controllers import (
    ApplyResult, CallableActuator, ControllerExperimentHarness, ControllerState, FabricConfig,
    PerformancePrediction, PredictiveController, ReactiveController, RecordingActuator, ShadowActuator,
    RollingMeanArrivalForecaster,
)
from fabric_perf.controllers.base import coerce_config
from fabric_perf.controllers.live_integration import IntegratedControllerRunner, TelemetryFrame
from fabric_perf.controllers.telemetry import FabricTelemetrySnapshot, MetricObservation, TelemetryDataError

BASE = FabricConfig(max_message_count=45)
TARGET = replace(BASE, max_message_count=40)


def state(config=BASE, latency=100, **kwargs):
    kwargs.setdefault("telemetry_age_s", 0)
    return ControllerState(current_config=config, observed_latency_ms=latency, **kwargs)


def reactive(**kwargs):
    return ReactiveController(BASE, congested_config=TARGET, latency_high_ms=200, latency_low_ms=100, **kwargs)


class Model:
    def predict(self, arrival, config):
        return PerformancePrediction(100 if config == BASE else 110, 100)


def predictive(**kwargs):
    return PredictiveController(Model(), BASE, arrival_forecaster=RollingMeanArrivalForecaster(),
                                candidate_configs=[BASE, TARGET], **kwargs)


@pytest.mark.parametrize("latency,changed", [(199.99, False), (200, True), (201, True)])
def test_reactive_threshold_is_inclusive(latency, changed):
    assert reactive().decide(state(latency=latency)).changed is changed


def test_queue_can_trigger_but_recovery_requires_latency_and_queue():
    controller = reactive(queue_high=10, queue_low=3, cooldown_samples=0)
    assert controller.decide(state(latency=None, queue_depth=10)).config == TARGET
    controller.configuration_applied(TARGET)
    assert not controller.decide(state(TARGET, latency=None, queue_depth=0)).changed
    assert not controller.decide(state(TARGET, latency=100, queue_depth=4)).changed
    assert controller.decide(state(TARGET, latency=100, queue_depth=3)).config == BASE


@pytest.mark.parametrize("bad", [state(latency=None), state(latency=1000, telemetry_age_s=6)])
def test_missing_or_stale_breaks_threshold_streak(bad):
    controller = reactive(consecutive_samples=2)
    assert not controller.decide(state(latency=200)).changed
    decision = controller.decide(bad)
    assert decision.outcome == "abstain"
    assert not controller.decide(state(latency=200)).changed
    assert controller.decide(state(latency=200)).changed


@pytest.mark.parametrize("kind", ["reactive", "predictive"])
def test_proposal_and_shadow_do_not_start_cooldown_or_change_applied_state(kind):
    controller = reactive(cooldown_samples=3) if kind == "reactive" else predictive(cooldown_samples=3)
    actuator = ShadowActuator(BASE)
    harness = ControllerExperimentHarness(controller, actuator, run_id="unit")
    for _ in range(3):
        record = harness.process(state(latency=250, arrival_history_tps=(100,)))
        assert record["decision"]["outcome"] == "propose"
        assert record["application"]["accepted"] is False
        assert record["configuration_state"]["after"] == BASE.as_dict()
        assert controller._verified_config == BASE
    if kind == "reactive":
        assert controller._active is False


def test_reactive_cooldown_starts_on_confirmed_application():
    controller = reactive(cooldown_samples=3)
    harness = ControllerExperimentHarness(controller, RecordingActuator(BASE), run_id="unit")
    first = harness.process(state(latency=200))
    assert first["configuration_state"]["proposal_applied"]
    assert controller._active
    assert not harness.process(state(latency=100))["decision"]["changed"]
    assert not harness.process(state(latency=100))["decision"]["changed"]
    assert harness.process(state(latency=100))["decision"]["config"] == BASE.as_dict()


def test_known_rejection_preserves_state_and_can_retry():
    class Reject(ShadowActuator):
        def apply(self, config):
            return ApplyResult(False, BASE, 0, "rejected before submission", requested_config=config)
    controller = reactive()
    harness = ControllerExperimentHarness(controller, Reject(BASE), run_id="unit")
    assert harness.process(state(latency=200))["decision"]["changed"]
    assert controller._active is False
    assert harness.process(state(latency=200))["decision"]["changed"]


@pytest.mark.parametrize("result", [None, {"success": False},
    ApplyResult(True, TARGET, 0, verified_config=BASE),
    {"success": True, "after_config": TARGET.as_dict(), "remote_state_unknown": True}])
def test_ambiguous_actuation_latches_stop_and_never_confirms(result):
    calls = []
    actuator = CallableActuator(BASE, lambda c: calls.append(c) or result)
    controller = reactive()
    harness = ControllerExperimentHarness(controller, actuator, run_id="unit")
    record = harness.process(state(latency=200))
    assert not record["application"]["accepted"]
    assert record["configuration_state"]["after"] is None
    assert controller._verified_config == BASE
    assert harness.process(state(latency=200))["decision"]["outcome"] == "abstain"
    assert calls == [TARGET]


def test_actuator_exception_is_distinct_from_policy_rejection_and_stops():
    class Throws(ShadowActuator):
        def apply(self, config):
            raise TimeoutError("no confirmation")
    harness = ControllerExperimentHarness(reactive(), Throws(BASE), run_id="unit")
    record = harness.process(state(latency=200))
    assert record["application"]["error"].startswith("actuator_error")
    assert harness.remote_state_unknown


@pytest.mark.parametrize("margin,changed", [(9.999, True), (10, False), (10.001, False)])
def test_predictive_margin_is_strict(margin, changed):
    decision = predictive(cooldown_samples=0, minimum_utility_improvement=margin).decide(
        state(arrival_history_tps=(100,)))
    assert decision.changed is changed
    assert decision.metadata["utility_improvement"] == 10
    if not changed:
        assert decision.outcome == "hold"
        assert decision.metadata["hold_cause"] == "insufficient_margin"


def test_predictive_tie_is_hold_not_model_preference_for_change():
    class Tie(Model):
        def predict(self, arrival, config):
            return PerformancePrediction(100, 100)
    controller = predictive(cooldown_samples=0)
    controller.performance_predictor = Tie()
    decision = controller.decide(state(arrival_history_tps=(100,)))
    assert decision.outcome == "hold"
    assert decision.metadata["utility_improvement"] == 0


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1])
def test_invalid_forecast_abstains_without_scoring(value):
    class BadForecast:
        def predict(self, history, horizon):
            return value
    controller = predictive()
    controller.arrival_forecaster = BadForecast()
    assert controller.decide(state(arrival_history_tps=(100,))).outcome == "abstain"


@pytest.mark.parametrize("times", [("2026-09-21T00:00:00Z",), ("2026-09-21T00:00:11Z",),
                                    ("2026-09-21T00:00:10",)])
def test_timestamp_checks_apply_to_scalar_forecasters(times):
    decision = predictive().decide(state(timestamp_utc="2026-09-21T00:00:10Z",
                                         arrival_history_tps=(100,), arrival_history_timestamps_utc=times))
    assert decision.reason == "prediction_failure_fail_safe"


def test_model_exception_abstains_and_keeps_config():
    class Bad(Model):
        def predict(self, arrival, config):
            raise RuntimeError("missing model")
    controller = predictive()
    controller.performance_predictor = Bad()
    decision = controller.decide(state(arrival_history_tps=(100,)))
    assert decision.outcome == "abstain" and decision.config == BASE


def test_predictive_confirmation_starts_cooldown():
    controller = predictive(cooldown_samples=3)
    assert controller.decide(state(arrival_history_tps=(100,))).changed
    controller.configuration_applied(TARGET)
    # Reverse the utility ranking, then require three fresh decisions.
    class Reverse(Model):
        def predict(self, arrival, config):
            return PerformancePrediction(110 if config == BASE else 100, 100)
    controller.performance_predictor = Reverse()
    assert not controller.decide(state(TARGET, arrival_history_tps=(100,))).changed
    assert not controller.decide(state(TARGET, arrival_history_tps=(100,))).changed
    assert controller.decide(state(TARGET, arrival_history_tps=(100,))).changed


@pytest.mark.parametrize("kwargs", [{"latency_high_ms": float("nan")}, {"latency_high_ms": -1},
    {"latency_high_ms": 100, "cooldown_samples": 1.2}, {"latency_high_ms": 100, "max_telemetry_age_s": float("inf")}])
def test_reactive_invalid_parameters_rejected(kwargs):
    with pytest.raises(ValueError):
        ReactiveController(BASE, **kwargs)


@pytest.mark.parametrize("kwargs", [{"latency_penalty_per_ms": float("nan")}, {"horizon_seconds": 1.5},
                                   {"cooldown_samples": True}, {"minimum_utility_improvement": -1}])
def test_predictive_invalid_parameters_rejected(kwargs):
    with pytest.raises(ValueError):
        predictive(**kwargs)


@pytest.mark.parametrize("kwargs", [{"batch_timeout_ms": True}, {"batch_timeout_ms": 1.2},
                                   {"absolute_max_bytes": 1.2}, {"max_message_count": 10.1},
                                   {"orderers": True}])
def test_configuration_does_not_truncate_invalid_values(kwargs):
    with pytest.raises(ValueError):
        coerce_config(kwargs)
    with pytest.raises(ValueError):
        FabricConfig(**kwargs)


def test_snapshot_keeps_old_metric_age_even_with_fresh_capture_and_rejects_wrong_units():
    metric = MetricObservation("latency_mean_ms", 200, "ms", "2026-09-21T00:00:00Z", "unit", 1)
    snapshot = FabricTelemetrySnapshot("channel", "2026-09-21T00:00:10Z", {metric.name: metric}, BASE, {})
    state = snapshot.to_controller_state(now_utc=snapshot.timestamp_utc, require_fresh=False)
    assert state.telemetry_age_s == 10
    assert reactive().decide(state).outcome == "abstain"
    wrong = replace(snapshot, metrics={metric.name: replace(metric, unit="s")})
    with pytest.raises(TelemetryDataError, match="must use ms"):
        wrong.to_controller_state(now_utc=snapshot.timestamp_utc, require_fresh=False)


def test_provider_collects_next_observation_after_previous_decision():
    events = []
    class Provider:
        def collect(self, *, run_id, decision_index):
            events.append("observe")
            return TelemetryFrame(run_id, decision_index, f"2026-09-21T00:00:0{decision_index}Z", 1,
                                  state(timestamp_utc=f"2026-09-21T00:00:0{decision_index}Z"))
    class Controller(ReactiveController):
        def decide(self, state):
            events.append("decide")
            return super().decide(state)
    runner = IntegratedControllerRunner({"reactive": Controller(BASE, latency_high_ms=200)}, BASE, run_id="unit")
    runner.run_provider(Provider(), samples=2)
    assert events == ["observe", "decide", "observe", "decide"]


def test_reordered_or_stale_source_frames_fail_closed():
    frame = TelemetryFrame("unit", 0, "2026-09-21T00:00:10Z", 1,
                           state(timestamp_utc="2026-09-21T00:00:10Z"),
                           source_timestamps_utc={"latency": "2026-09-21T00:00:00Z"})
    result = IntegratedControllerRunner({"reactive": reactive()}, BASE, run_id="unit").run([frame])
    assert result["record_count"] == 0 and result["stop_reasons"]
    frame = replace(frame, source_timestamps_utc={})
    result = IntegratedControllerRunner({"reactive": reactive()}, BASE, run_id="unit").run([frame, frame])
    assert result["record_count"] == 1 and result["stop_reasons"]


def test_incomplete_actuator_evidence_cannot_confirm_defaults():
    actuator = CallableActuator(TARGET, lambda _: {"success": True, "after_config": {}})
    result = actuator.apply(FabricConfig())
    assert not result.accepted and result.remote_state_unknown
    assert actuator.current_config == TARGET


def test_incomplete_independent_config_evidence_is_rejected():
    from fabric_perf.controllers import IndependentConfigurationVerifier
    verifier = IndependentConfigurationVerifier(lambda: {"block_number": 9},
                                                 transition_validator=lambda *_: {"ok": True})
    assert not verifier({"before_block_number": 8}, FabricConfig())["success"]


def test_snapshot_requires_observed_configuration():
    snapshot = FabricTelemetrySnapshot("channel", "2026-09-21T00:00:00Z", {}, None, {})
    with pytest.raises(TelemetryDataError, match="configuration"):
        snapshot.to_controller_state(now_utc=snapshot.timestamp_utc)


@pytest.mark.parametrize("evidence", [{}, {"max_message_count": 40}])
def test_provider_rejects_incomplete_configuration_reader(evidence):
    from fabric_perf.controllers import ProductionTelemetryProvider, PrometheusTelemetryAdapter
    prometheus = PrometheusTelemetryAdapter({"unit": "http://unit/metrics"},
        fetch=lambda *_: 'consensus_etcdraft_is_leader{channel="unit"} 1\n')
    provider = ProductionTelemetryProvider(prometheus, "unit", config_reader=lambda: evidence, required_fields=())
    frame = provider.collect(run_id="unit", decision_index=0, observation_timestamp_utc="2026-09-21T00:00:00Z")
    assert "current_config" in frame.critical_missing
    assert any("configuration_read_error" in error for error in frame.critical_missing)


def test_continuous_telemetry_excludes_invalid_ledger_and_failed_latency(tmp_path):
    from fabric_perf.controllers import ContinuousCaliperLedgerAdapter
    recorder = tmp_path / "recorder.csv"
    submits = tmp_path / "submits.csv"
    ledger = tmp_path / "ledger.csv"
    recorder.write_text("tx_id,final_ms,latency_ms,status\na,1000,100,success\nb,1000,900,failed\n")
    submits.write_text("t_submit_evt_ms,count\n0,1\n1000,1\n")
    ledger.write_text("tx_id,validation_code\na,0\nb,1\n")
    adapter = ContinuousCaliperLedgerAdapter(recorder_csv=recorder, submits_csv=submits, ledger_csv=ledger,
                                             channel_id="unit", run_id="unit")
    first = adapter.collect(observation_timestamp_utc="1970-01-01T00:00:02Z")
    assert first.metric("latency_mean_ms").value == 100
    assert first.metric("latency_p95_ms").value == 100
    with ledger.open("a") as stream:
        stream.write("c,0\nd,1\n")
    second = adapter.collect(observation_timestamp_utc="1970-01-01T00:00:03Z")
    assert second.metric("delivered_valid_tps").value == 1
